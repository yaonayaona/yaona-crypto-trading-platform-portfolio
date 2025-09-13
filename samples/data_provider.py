# data_provider.py
# -*- coding: utf-8 -*-
"""
ローリングキャッシュ（最新足のみの増分取得） + Parquet スナップショット

公開API:
  - warm_cache(timeframes=("5m","15m","1h","4h"), window=DEFAULT_WINDOW)
  - refresh_cache(timeframes)
  - save_snapshot(timeframes)
  - prime_cache_from_snapshot(timeframes)
  - get_latest_all(...), get_latest(...)
"""

from __future__ import annotations

import os
import re
import logging
import threading
from pathlib import Path
from typing import Dict, Optional, Sequence, List

import pandas as pd
from sqlalchemy import create_engine, text  # SQLAlchemy 2.0
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from urllib.parse import quote_plus

# ──────────────────────────────
# env helper（すべてのenv読み取りはこれ経由）
# ──────────────────────────────
def senv(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

# ------------------------------------------------------------------------------
# ログ
# ------------------------------------------------------------------------------
logger = logging.getLogger("data_provider")
if not logger.handlers:
    logging.basicConfig(
        level=senv("DP_LOGLEVEL", "INFO") or "INFO",
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

# ------------------------------------------------------------------------------
# DB (Timescale/Postgres) — プロジェクト側の db.py に依存
# ------------------------------------------------------------------------------
try:
    from db import get_engine  # type: ignore
except Exception:
    get_engine = None  # type: ignore

# フォールバック付きエンジン作成
_ENGINE_CACHE = None

def _make_uri(host: str) -> str:
    user = senv("PGUSER", "postgres") or "postgres"
    pwd  = senv("PGPASSWORD", "") or ""
    port = senv("PGPORT", "5432") or "5432"
    db   = senv("PGDATABASE", "postgres") or "postgres"
    return f"postgresql+psycopg2://{user}:{quote_plus(pwd)}@{host}:{port}/{db}"

def _get_engine_fallback():
    global _ENGINE_CACHE
    if _ENGINE_CACHE is not None:
        return _ENGINE_CACHE

    # 1) まず get_engine()
    if get_engine is not None:
        try:
            eng = get_engine()
            with eng.connect() as _:
                pass
            _ENGINE_CACHE = eng
            return eng
        except Exception as e:
            logger.warning("[DP] get_engine() failed; try fallbacks: %r", e)

    # 2) 代表的なホスト名で順に接続を試す
    host0 = senv("PGHOST", "localhost") or "localhost"
    if host0 in ("localhost", "127.0.0.1"):
        candidates = ["127.0.0.1", "localhost", "timescaledb", "db", "postgres", "host.docker.internal"]
    else:
        candidates = [host0, "127.0.0.1", "timescaledb", "db", "postgres", "host.docker.internal"]

    last_err = None
    for h in candidates:
        try:
            uri = _make_uri(h)
            eng = create_engine(uri, pool_pre_ping=True)
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))  # SQLAlchemy 2.0 互換
            logger.info("[DP] DB engine connected via host=%s", h)
            _ENGINE_CACHE = eng
            return eng
        except Exception as e:
            last_err = e
            logger.warning("[DP] DB connect failed host=%s (%r)", h, e)

    logger.error("[DP] DB connection unavailable (all fallbacks failed): %r", last_err)
    return None

# ------------------------------------------------------------------------------
# 設定
# ------------------------------------------------------------------------------
DEFAULT_WINDOW = int(senv("CACHE_WINDOW", "4032") or "4032")
SNAPSHOT_DIR   = Path(senv("CACHE_SNAPSHOT_DIR", "/dev/shm/market_cache") or "/dev/shm/market_cache").resolve()
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
USE_SNAPSHOT   = (senv("USE_SNAPSHOT", "1") or "1") == "1"

TIMEFRAME_TABLE_MAP: Dict[str, str] = {
    "5m":  "allsymbol_price_5m",
    "15m": "allsymbol_price_15m",
    "1h":  "allsymbol_price_1h",
    "4h":  "allsymbol_price_4h",
}

COLUMN_ALIASES = {"vol": "volume"}

# ------------------------------------------------------------------------------
# スナップショットパス
# ------------------------------------------------------------------------------
def _snap_path_current(tf: str) -> Path:
    return SNAPSHOT_DIR / f"{tf}_allsymbol_current.parquet"

def _snap_path_windowed(tf: str, window: int = DEFAULT_WINDOW) -> Path:
    return SNAPSHOT_DIR / f"{tf}_allsymbol_{window}.parquet"

def _pick_best_snapshot(tf: str) -> Optional[Path]:
    cur = _snap_path_current(tf)
    if cur.exists():
        return cur
    cands = sorted(
        SNAPSHOT_DIR.glob(f"{tf}_allsymbol_*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    for p in cands:
        if p.name.endswith("_current.parquet"):
            continue
        return p
    return None

# ------------------------------------------------------------------------------
# ユーティリティ
# ------------------------------------------------------------------------------
_SYM_RE = re.compile(r"^[A-Z0-9_]+$")

def _empty_multiindex_df() -> pd.DataFrame:
    return (
        pd.DataFrame(columns=["symbol","time","open","high","low","close","volume","oi"])
        .set_index(pd.MultiIndex.from_arrays([[],[]], names=["symbol","time"]))
    )

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    for a, b in COLUMN_ALIASES.items():
        if a in df.columns and b not in df.columns:
            df.rename(columns={a: b}, inplace=True)
    return df

# ------------------------------------------------------------------------------
# DB取得: 初回（末尾window本）/ 以後（差分）
# ------------------------------------------------------------------------------
def _fetch_tail_per_symbol(timeframe: str, per_symbol_rows: int) -> pd.DataFrame:
    """各シンボル末尾 per_symbol_rows をまとめて取得（初回ウォーム用）。"""
    table = TIMEFRAME_TABLE_MAP[timeframe]
    sql = f"""
        SELECT symbol, time, open, high, low, close, COALESCE(volume,0) AS volume, oi
        FROM (
            SELECT
                symbol, time, open, high, low, close, volume, oi,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) AS rn
            FROM {table}
        ) t
        WHERE rn <= %(limit)s
        ORDER BY symbol, time
    """
    eng = _get_engine_fallback()
    if eng is None:
        return _empty_multiindex_df()

    df = pd.read_sql(sql, eng, params={"limit": per_symbol_rows}, parse_dates=["time"])
    if df.empty:
        return _empty_multiindex_df()
    _normalize_cols(df)
    df.sort_values(["symbol","time"], inplace=True)
    df.set_index(["symbol","time"], inplace=True)
    return df

def _fetch_since_per_symbol(timeframe: str, since_map: Dict[str, pd.Timestamp], chunk: int = 300) -> pd.DataFrame:
    """
    銘柄ごとの last_ts を与えて、新しい足だけをまとめて取得（JOIN VALUES）。
    返値: MultiIndex(symbol,time)
    """
    if not since_map:
        return _empty_multiindex_df()

    table = TIMEFRAME_TABLE_MAP[timeframe]
    eng = _get_engine_fallback()
    if eng is None:
        return _empty_multiindex_df()

    frames: List[pd.DataFrame] = []
    items = list(since_map.items())
    for i in range(0, len(items), chunk):
        part = items[i:i+chunk]
        safe_part = [(s, ts) for s, ts in part if _SYM_RE.match(s)]
        if not safe_part:
            continue

        values_rows = ",\n".join(
            f"('{s}', TIMESTAMP '{pd.Timestamp(ts).strftime('%Y-%m-%d %H:%M:%S')}')"
            for s, ts in safe_part
        )
        sql = f"""
            WITH m(symbol, since) AS (
                VALUES
                {values_rows}
            )
            SELECT
                t.symbol, t.time, t.open, t.high, t.low, t.close,
                COALESCE(t.volume,0) AS volume, t.oi
            FROM {table} t
            JOIN m ON m.symbol = t.symbol AND t.time > m.since
            ORDER BY t.symbol, t.time
        """
        df = pd.read_sql(sql, eng, parse_dates=["time"])
        if not df.empty:
            _normalize_cols(df)
            df.sort_values(["symbol","time"], inplace=True)
            df.set_index(["symbol","time"], inplace=True)
            frames.append(df)

    if not frames:
        return _empty_multiindex_df()

    out = pd.concat(frames).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out

# ------------------------------------------------------------------------------
# ローリングキャッシュ
# ------------------------------------------------------------------------------
class _RollingCache:
    """
    TFごとに MultiIndex(symbol,time) DataFrame を保持。
    - 初回 warm: 末尾 window 本
    - 以後 refresh: 銘柄ごとの last_ts 以降のみ（JOIN VALUES で差分）
    """
    def __init__(self, window: int = DEFAULT_WINDOW) -> None:
        self.window = int(window)
        self._lock  = threading.Lock()
        self._store: Dict[str, pd.DataFrame] = {}

    def _clip_window(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return df.groupby(level=0, group_keys=False).tail(self.window)

    def warm(self, timeframes: Sequence[str]) -> None:
        with self._lock:
            for tf in timeframes:
                df = _fetch_tail_per_symbol(tf, self.window)
                self._store[tf] = self._clip_window(df)
                logger.info("[DP] warm %s rows=%d", tf, len(df))

    def refresh(self, timeframes: Sequence[str]) -> None:
        with self._lock:
            for tf in timeframes:
                current = self._store.get(tf)
                if current is None or current.empty:
                    full_df = _fetch_tail_per_symbol(tf, max(self.window, 2))
                    self._store[tf] = self._clip_window(full_df)
                    logger.info("[DP] refresh %s: cache empty → warm rows=%d", tf, len(full_df))
                    continue

                # 銘柄ごとの last_ts
                last_by_sym = current.reset_index().groupby("symbol")["time"].max().to_dict()

                # 差分取得
                incr = _fetch_since_per_symbol(tf, last_by_sym)
                logger.info(
                    "[DP] refresh %s: since_map=%d symbols, incr_rows=%d, cache_rows_before=%d",
                    tf, len(last_by_sym), len(incr), len(current)
                )

                if incr.empty:
                    self._store[tf] = self._clip_window(current)
                    continue

                merged = pd.concat([current, incr]).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]
                merged = self._clip_window(merged)
                self._store[tf] = merged

                logger.info("[DP] refresh %s: cache_rows_after=%d", tf, len(merged))

    def get(self, timeframe: str) -> pd.DataFrame:
        df = self._store.get(timeframe)
        return df if df is not None else _empty_multiindex_df()

# シングルトン
_CACHE = _RollingCache(window=DEFAULT_WINDOW)

# ------------------------------------------------------------------------------
# スナップショット I/O（Parquet）
# ------------------------------------------------------------------------------
def save_snapshot(timeframes: Sequence[str] | str = ("5m","15m","1h","4h")) -> None:
    """現在キャッシュを Parquet に保存。"""
    if isinstance(timeframes, str):
        timeframes = [timeframes]

    for tf in timeframes:
        df = _CACHE.get(tf)
        if df.empty:
            logger.info("[DP] save_snapshot %s: empty; skip", tf)
            continue

        windowed = _snap_path_windowed(tf, _CACHE.window)
        tmp_win  = windowed.with_suffix(".parquet.tmp")
        df.to_parquet(tmp_win, compression="zstd", index=True)
        tmp_win.replace(windowed)

        current  = _snap_path_current(tf)
        tmp_link = SNAPSHOT_DIR / f".{tf}_allsymbol_current.parquet.tmp"

        try:
            if tmp_link.exists() or tmp_link.is_symlink():
                tmp_link.unlink()
            os.symlink(windowed.name, tmp_link)  # type: ignore[name-defined]
            os.replace(tmp_link, current)
        except Exception:
            tmp_cp = current.with_suffix(".parquet.tmp")
            df.to_parquet(tmp_cp, compression="zstd", index=True)
            tmp_cp.replace(current)

        logger.info("[DP] save_snapshot %s: wrote %s and updated current", tf, windowed.name)

def prime_cache_from_snapshot(timeframes: Sequence[str] | str = ("5m","15m","1h","4h")) -> None:
    """既存の Parquet（current優先）からキャッシュ復元。無ければスキップ。"""
    if isinstance(timeframes, str):
        timeframes = [timeframes]

    for tf in timeframes:
        p = _pick_best_snapshot(tf)
        if p is None or not p.exists():
            logger.info("[DP] prime %s: snapshot not found", tf)
            continue
        try:
            df = pd.read_parquet(p)
            if not isinstance(df.index, pd.MultiIndex) or list(df.index.names) != ["symbol","time"]:
                if {"symbol","time"}.issubset(df.columns):
                    df = df.sort_values(["symbol","time"]).set_index(["symbol","time"])
                else:
                    logger.warning("[DP] prime %s: invalid snapshot shape; skip", tf)
                    continue
            df.sort_index(inplace=True)
            with _CACHE._lock:
                _CACHE._store[tf] = _CACHE._clip_window(df)
            logger.info("[DP] prime %s: loaded rows=%d from %s", tf, len(df), p.name)
        except Exception as e:
            logger.warning("[DP] prime %s: failed to load (%r)", tf, e)

# ------------------------------------------------------------------------------
# 公開 API
# ------------------------------------------------------------------------------
def warm_cache(timeframes: Sequence[str] | str = ("5m","15m","1h","4h"), window: int = DEFAULT_WINDOW) -> None:
    if isinstance(timeframes, str):
        timeframes = [timeframes]
    global _CACHE
    if window != _CACHE.window:
        _CACHE = _RollingCache(window=window)
    _CACHE.warm(timeframes)

def refresh_cache(timeframes: Sequence[str] | str = ("5m","15m","1h","4h")) -> None:
    if isinstance(timeframes, str):
        timeframes = [timeframes]
    _CACHE.refresh(timeframes)

def get_latest_all(
    *,
    timeframes: Sequence[str] | str = "5m",
    n: int = DEFAULT_WINDOW,
    symbols: Optional[Sequence[str]] = None,
    reset_index: bool = True,
) -> Dict[str, pd.DataFrame]:
    """{tf: DataFrame} を返す。"""
    if isinstance(timeframes, str):
        timeframes = [timeframes]

    out: Dict[str, pd.DataFrame] = {}
    for tf in timeframes:
        df = _CACHE.get(tf)
        if df.empty:
            out[tf] = pd.DataFrame(columns=["symbol","time","open","high","low","close","volume","oi","tf"])
            continue
        cur = df
        if symbols:
            cur = cur.loc[cur.index.get_level_values("symbol").isin(list(symbols))]
        if n < DEFAULT_WINDOW:
            cur = cur.groupby(level=0, group_keys=False).tail(n)
        if reset_index:
            cur = cur.reset_index()
        cur = cur.assign(tf=tf)
        out[tf] = cur
    return out

def get_latest(
    timeframe: str,
    n: int = DEFAULT_WINDOW,
    *,
    symbols: Optional[Sequence[str]] = None,
    reset_index: bool = True,
) -> pd.DataFrame:
    """単一TFの最新 n 本（get_latest_all の糖衣）。"""
    return get_latest_all(timeframes=[timeframe], n=n, symbols=symbols, reset_index=reset_index)[timeframe]

# ------------------------------------------------------------------------------
# 手動確認
# ------------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    prime_cache_from_snapshot(["5m","15m","1h","4h"])
    refresh_cache(["5m","15m","1h","4h"])
    save_snapshot(["5m","15m","1h","4h"])
    d = get_latest_all(timeframes=["5m"], n=100, reset_index=True)
    for tf, df in d.items():
        print(tf, df.shape, df[["symbol","time","close"]].tail(3).to_string(index=False))
