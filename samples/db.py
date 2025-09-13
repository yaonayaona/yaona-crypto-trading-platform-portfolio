"""
db.py — TimescaleDB & Parquet hybrid loader (with in-process cache)
────────────────────────────────────────────────────────────────────
提供機能:
・get_engine()         : SQLAlchemy Engine を取得
・fetch_price_df(...)  : OHLCV+OI の DataFrame を取得（Parquet優先→DBフォールバック）
・list_symbols(table)  : 指定テーブルの distinct symbol 一覧

互換ポリシー:
・既存コードからの呼び出しシグネチャを維持:
    fetch_price_df(start, end, *, symbols, table)
・返却カラムは常に ["symbol","time","open","high","low","close","vol","oi"]
・index なし（必要なら呼び出し側で set_index してください）
"""

from __future__ import annotations

# .env 読み込み
from dotenv import load_dotenv
load_dotenv()

import os
from pathlib import Path
from typing import Optional, Sequence
from collections import OrderedDict

import pandas as pd
import sqlalchemy as sa

# ──────────────────────────────────────────────────────────────
# DB 接続設定（.env）
# ──────────────────────────────────────────────────────────────
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "crypto_db")
PG_USER = os.getenv("PGUSER", "admin")
PG_PWD  = os.getenv("PGPASSWORD", "admin")

_ENGINE: Optional[sa.Engine] = None

def get_engine() -> sa.Engine:
    """singleton Engine（SQLAlchemy 2.x）。"""
    global _ENGINE
    if _ENGINE is None:
        url = f"postgresql+psycopg2://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        _ENGINE = sa.create_engine(url, pool_pre_ping=True, future=True)
    return _ENGINE


# ──────────────────────────────────────────────────────────────
# Parquet 優先読み設定
# ──────────────────────────────────────────────────────────────
BT_USE_PARQUET = (os.getenv("BT_USE_PARQUET", "1") == "1")
SNAPSHOT_DIR   = Path(os.getenv("CACHE_SNAPSHOT_DIR", "/dev/shm/market_cache")).resolve()

# 1プロセス内キャッシュ（ファイル mtime をキーに LRU）
BT_PARQUET_CACHE       = (os.getenv("BT_PARQUET_CACHE", "1") == "1")
BT_PARQUET_CACHE_MAX   = int(os.getenv("BT_PARQUET_CACHE_MAX", "4"))
# 読み込み列（無駄を減らして I/O を軽くする）
NEEDED_COLS = ["symbol", "time", "open", "high", "low", "close", "vol", "oi"]

# { path_str: (mtime_float, pandas.DataFrame) } を LRU で保持
_parquet_lru: "OrderedDict[str, tuple[float, pd.DataFrame]]" = OrderedDict()


def _infer_tf_from_table(table: str) -> Optional[str]:
    """
    テーブル名から時間足を推定。慣習:
      allsumbol_price_5m / 15m / 1h / 4h など。
    """
    t = table.lower()
    if "5m" in t:
        return "5m"
    if "15m" in t:
        return "15m"
    # "1h" と "15m" の衝突を避ける
    if "1h" in t and "15m" not in t:
        return "1h"
    if "4h" in t:
        return "4h"
    return None


def _pick_snapshot_path(tf: str) -> Optional[Path]:
    """
    最新スナップショットを選ぶ:
      1) {tf}_allsymbol_current.parquet があれば最優先
      2) {tf}_allsymbol_*.parquet のうち最も新しいもの
      3) 互換: {tf}_allysymbol_*.parquet（旧表記）も許容
    """
    cur = SNAPSHOT_DIR / f"{tf}_allsymbol_current.parquet"
    if cur.exists():
        return cur

    cands = sorted(SNAPSHOT_DIR.glob(f"{tf}_allsymbol_*.parquet"))
    if not cands:
        cands = sorted(SNAPSHOT_DIR.glob(f"{tf}_allysymbol_*.parquet"))
    return cands[-1] if cands else None


def _read_parquet_cached(p: Path) -> Optional[pd.DataFrame]:
    """
    Parquet を読み込み。BT_PARQUET_CACHE=1 のときは
    同一ファイル（mtime一致）なら DataFrame を再利用。
    """
    try:
        mtime = p.stat().st_mtime
    except FileNotFoundError:
        return None

    key = str(p)

    # LRU ヒット
    if BT_PARQUET_CACHE and key in _parquet_lru:
        cached_mtime, cached_df = _parquet_lru[key]
        if cached_mtime == mtime:
            # LRU 更新
            _parquet_lru.move_to_end(key)
            return cached_df

    # 読み込み（列を絞る / スレッド有効）
    # ※ pyarrow があれば自動的に利用される。無くても pandas が対応エンジンで読む。
    df = pd.read_parquet(p, columns=None)  # 一旦全部読み、後で列リネーム・絞り込み
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()

    # 列名正規化: "volume" → "vol"
    if "volume" in df.columns and "vol" not in df.columns:
        df = df.rename(columns={"volume": "vol"})

    # 必須列不足なら失敗扱い
    if not set(NEEDED_COLS).issubset(df.columns):
        return None

    # LRU へ格納
    if BT_PARQUET_CACHE:
        _parquet_lru[key] = (mtime, df)
        # あふれたら最古を削除
        while len(_parquet_lru) > BT_PARQUET_CACHE_MAX:
            _parquet_lru.popitem(last=False)

    return df


def _fetch_from_parquet(
    *,
    timeframe: str,
    start: str,
    end: str,
    symbols: Optional[Sequence[str]],
) -> Optional[pd.DataFrame]:
    """Parquet から必要範囲を抽出。失敗/空なら None。"""
    p = _pick_snapshot_path(timeframe)
    if p is None or not p.exists():
        return None

    base = _read_parquet_cached(p)
    if base is None or base.empty:
        return None

    # 型整形: time 列を datetime へ
    if not pd.api.types.is_datetime64_any_dtype(base["time"]):
        base = base.copy()
        base["time"] = pd.to_datetime(base["time"], utc=False)

    s = pd.to_datetime(start)
    e = pd.to_datetime(end)

    mask = (base["time"] >= s) & (base["time"] <= e)
    if symbols:
        mask &= base["symbol"].isin(list(symbols))

    out = base.loc[mask, NEEDED_COLS]
    if out.empty:
        return None

    return out.sort_values(["symbol", "time"]).reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
# 既存 I/F: Parquet 優先 → DB フォールバック
# ──────────────────────────────────────────────────────────────
def fetch_price_df(
    start: str,
    end: str,
    *,
    symbols: Sequence[str] | None,
    table: str,
) -> pd.DataFrame:
    """
    OHLCV+OI を DataFrame で返す。
    ・Parquet が利用可能/範囲内なら Parquet から返却
    ・そうでなければ DB へフォールバック
    返却カラム: symbol,time,open,high,low,close,vol,oi
    """
    tf = _infer_tf_from_table(table)

    # 1) Parquet 優先
    if BT_USE_PARQUET and tf is not None:
        out = _fetch_from_parquet(timeframe=tf, start=start, end=end, symbols=symbols)
        if out is not None:
            import logging
            logging.getLogger(__name__).info(
                "Using Parquet snapshot for table=%s tf=%s rows=%d", table, tf, len(out)
            )
            return out

    # 2) SQL フォールバック
    cond = "" if symbols is None else "AND symbol = ANY(:syms)"
    sql = sa.text(
        f"""
        SELECT symbol, time,
               open, high, low, close,
               volume AS vol, oi
          FROM {table}
         WHERE time BETWEEN :s AND :e
         {cond}
         ORDER BY symbol, time;
        """
    )
    params = {"s": start, "e": end}
    if symbols is not None:
        params["syms"] = list(symbols)

    return pd.read_sql(sql, get_engine(), params=params, parse_dates=["time"])


def list_symbols(*, table: str) -> list[str]:
    """指定テーブルに存在する distinct symbol を返す。"""
    sql = sa.text(f"SELECT DISTINCT symbol FROM {table} ORDER BY symbol;")
    with get_engine().connect() as conn:
        res = conn.execute(sql)
        return [row[0] for row in res]
