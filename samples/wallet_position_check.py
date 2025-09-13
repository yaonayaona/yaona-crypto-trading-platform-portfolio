#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wallet_position_check.py — 残高/ポジション/日次損益/イベント集計/DBスナップショット保存（cron & 起動カード兼用）

[作成] 2025-08-16 JST
[更新] 2025-08-16 JST (v5)
  - .env の CR/LF・余計な引用符の除去を “依存モジュールを import する前” に実施
  - DB接続のフォールバックに加え、エンジンをキャッシュして再接続を抑制
  - Discord Embed の timestamp を timezone-aware に変更（utcnow→now(timezone.utc)）
"""

from __future__ import annotations
import os
import json
import math
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote_plus

# ---------- まず .env を読み込み＆環境変数をサニタイズ（依存モジュール import 前） ----------
from dotenv import load_dotenv
load_dotenv()

def _sanitize_value(v: str) -> str:
    v = v.replace("\r", "").replace("\n", "").strip()
    if (len(v) >= 2) and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        v = v[1:-1].strip()
    return v

def _scrub_env(keys: list[str]) -> None:
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            os.environ[k] = _sanitize_value(v)

def _scrub_all_env() -> None:
    for k, v in list(os.environ.items()):
        if isinstance(v, str):
            os.environ[k] = _sanitize_value(v)

_scrub_env([
    "PGHOST","PGPORT","PGDATABASE","PGUSER","PGPASSWORD",
    "DISCORD_URL_walletnews",
    "BYBIT_API_KEY","BYBIT_API_SECRET","BYBIT_HTTP_TIMEOUT","SETTLE_COIN",
    "MAX_POSITIONS","MAX_POS","CANDIDATE_FILE","WALLET_POS_LOCK",
    "SNAP_TABLE","TRADE_TABLE","LOG_LEVEL"
])
_scrub_all_env()  # 念のため全体にも適用

# ---------- 依存モジュール（環境変数に依存するものはここから import） ----------
import requests
from filelock import FileLock, Timeout
from pybit.unified_trading import HTTP
import sqlalchemy as sa

# プロジェクト内
from position_notifier import notify_news_card  # フォールバック用
import risk_control
from manual_fill_detector import detect_manual_positions
from state_manager import STATE_DIR as ENTRY_STATE_DIR
from db import get_engine

# ====== 環境設定 ======
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("wallet_position_check")

API_KEY       = os.getenv("BYBIT_API_KEY")
API_SECRET    = os.getenv("BYBIT_API_SECRET")
HTTP_TIMEOUT  = float(os.getenv("BYBIT_HTTP_TIMEOUT", "5"))
SETTLE_COIN   = os.getenv("SETTLE_COIN", "USDT")
TIMEZONE      = timezone(timedelta(hours=9))  # JST

MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", os.getenv("MAX_POS", "3")))
CAND_FILE     = Path(os.getenv("CANDIDATE_FILE", "entry_candidates.json"))
LOCK_PATH     = os.getenv("WALLET_POS_LOCK", "/run/wallet_position_check.lock")
MANUAL_MAX_SHOW = 3

TRADE_TABLE = os.getenv("TRADE_TABLE", "yaona_short_v1_tradehis")
SNAP_TABLE  = os.getenv("SNAP_TABLE",  "v1_wallet_snapshots")  # 既定テーブル名

# ====== ヘルパ ======
def jst_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(TIMEZONE)

def jst_now_str() -> str:
    return jst_now().strftime("%Y-%m-%d %H:%M:%S JST")

def fmt_usdt(x: Optional[float]) -> str:
    if x is None or math.isnan(x) or math.isinf(x):
        return "N/A"
    return f"{x:,.2f} USDT"

def f(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

def get_wallet_balance(client: HTTP) -> float:
    try:
        return float(risk_control.get_wallet_balance())
    except Exception as e:
        logger.warning("risk_control.get_wallet_balance 失敗: %s → APIフォールバック", e)
    try:
        r = client.get_wallet_balance(accountType="UNIFIED", coin=SETTLE_COIN)
        coins = ((r or {}).get("result", {}) or {}).get("list", [])
        if coins:
            for c in coins[0].get("coin", []):
                if (c.get("coin") or "").upper() == SETTLE_COIN.upper():
                    return f(c.get("walletBalance"))
    except Exception as e:
        logger.warning("get_wallet_balance API失敗: %s", e)
    return 0.0

def get_positions(client: HTTP) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        resp = client.get_positions(category="linear", settleCoin=SETTLE_COIN)
        for p in (resp.get("result", {}).get("list", []) or []):
            if f(p.get("size")) > 0 or f(p.get("positionValue")) > 0:
                out.append(p)
    except Exception as e:
        logger.warning("get_positions 失敗: %s", e)
    return out

def count_candidates() -> int:
    try:
        if not CAND_FILE.exists():
            return 0
        arr = json.loads(CAND_FILE.read_text() or "[]")
        return len(arr)
    except Exception:
        return 0

def build_positions_field(positions: List[Dict[str, Any]], balance: float, limit: int = 10) -> Tuple[str, list]:
    details_json = []
    if not positions:
        return "（開ポジなし）", details_json
    positions = sorted(positions, key=lambda p: abs(f(p.get("positionValue"))), reverse=True)
    lines: List[str] = []
    for p in positions[:limit]:
        sym   = p.get("symbol", "?")
        side  = p.get("side", "?")
        size  = f(p.get("size"))
        avg   = f(p.get("avgPrice"))
        mark  = f(p.get("markPrice"))
        upnl  = f(p.get("unrealisedPnl"))
        liq   = f(p.get("liqPrice"))
        pct   = (upnl / balance * 100.0) if balance else 0.0
        lines.append(
            "・{sym} {side} size={size:g} @ {avg:.6f} | mark {mark:.6f} | uPnL {upnl:+.2f} ({pct:+.2f}%) | liq {liq:.6f}".format(
                sym=sym, side=side, size=size, avg=avg, mark=mark, upnl=upnl, pct=pct, liq=liq
            )
        )
        details_json.append({
            "symbol": sym, "side": side, "size": size,
            "avgPrice": avg, "markPrice": mark,
            "unrealisedPnl": upnl, "unrealisedPnlPct": pct,
            "liqPrice": liq
        })
    if len(positions) > limit:
        lines.append(f"…他 {len(positions)-limit} 件")
    return "\n".join(lines), details_json

def calc_daily_pnl(current_wallet: float) -> Tuple[Optional[float], Optional[float]]:
    try:
        s = risk_control.load_state()
        sod = s.get("start_of_day_wallet")
        if not sod:
            return None, None
        pnl = current_wallet - float(sod)
        ret = (pnl / float(sod) * 100.0) if float(sod) != 0 else None
        return pnl, ret
    except Exception:
        return None, None

def detect_manuals() -> List[Dict[str, Any]]:
    try:
        return detect_manual_positions(CAND_FILE, ENTRY_STATE_DIR)
    except Exception as e:
        logger.debug("manual detect failed: %s", e)
        return []

# ====== DB: get_engine() 失敗時にフォールバック & キャッシュ ======
_ENGINE_CACHE = None

def _get_engine_any():
    global _ENGINE_CACHE
    if _ENGINE_CACHE is not None:
        return _ENGINE_CACHE
    # 1) 既存の get_engine()
    try:
        eng = get_engine()
        with eng.connect() as c:
            c.execute(sa.text("SELECT 1"))
        _ENGINE_CACHE = eng
        return eng
    except Exception as e:
        logger.warning("get_engine() failed; trying fallback hosts: %r", e)
    # 2) 代表的なホストに順次トライ
    user = (os.getenv("PGUSER", "admin") or "admin").strip()
    pwd  = (os.getenv("PGPASSWORD", "admin") or "admin").strip()
    port = (os.getenv("PGPORT", "5432") or "5432").strip()
    dbn  = (os.getenv("PGDATABASE", "crypto_db") or "crypto_db").strip()
    host0= (os.getenv("PGHOST", "") or "").strip()
    candidates = [h for h in [host0, "127.0.0.1", "localhost", "timescaledb", "db", "postgres", "host.docker.internal"] if h]
    last = None
    for h in candidates:
        try:
            url = f"postgresql+psycopg2://{user}:{quote_plus(pwd)}@{h}:{port}/{dbn}"
            eng = sa.create_engine(url, pool_pre_ping=True, future=True)
            with eng.connect() as c:
                c.execute(sa.text("SELECT 1"))
            logger.info("DB connected via host=%s", h)
            _ENGINE_CACHE = eng
            return eng
        except Exception as e:
            last = e
            logger.warning("DB connect failed host=%s (%r)", h, e)
    raise last or RuntimeError("DB connection unavailable")

# ====== DBユーティリティ ======
def ensure_tables():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SNAP_TABLE} (
        ts              TIMESTAMPTZ PRIMARY KEY DEFAULT now(),
        wallet_balance  DOUBLE PRECISION,
        total_upnl      DOUBLE PRECISION,
        total_pos_value DOUBLE PRECISION,
        open_positions  INTEGER,
        pos_details     JSONB
    );
    CREATE INDEX IF NOT EXISTS {SNAP_TABLE}_ts_idx ON {SNAP_TABLE}(ts DESC);
    """
    with _get_engine_any().begin() as conn:
        for stmt in ddl.strip().split(";"):
            if stmt.strip():
                conn.exec_driver_sql(stmt)

def insert_snapshot(ts: datetime, wallet: float, tot_upnl: float, tot_val: float, open_cnt: int, pos_details: list):
    sql = sa.text(f"""
        INSERT INTO {SNAP_TABLE} (ts, wallet_balance, total_upnl, total_pos_value, open_positions, pos_details)
        VALUES (:ts, :wb, :tu, :tv, :oc, CAST(:pd AS JSONB))
        ON CONFLICT (ts) DO NOTHING
    """)
    with _get_engine_any().begin() as conn:
        conn.execute(sql, {
            "ts": ts,
            "wb": wallet,
            "tu": tot_upnl,
            "tv": tot_val,
            "oc": open_cnt,
            "pd": json.dumps(pos_details),
        })

def get_last_snapshot() -> Optional[dict]:
    sql = sa.text(f"SELECT ts, wallet_balance, open_positions, pos_details FROM {SNAP_TABLE} ORDER BY ts DESC LIMIT 1")
    with _get_engine_any().connect() as conn:
        row = conn.execute(sql).fetchone()
        if not row:
            return None
        return {"ts": row[0], "wallet_balance": row[1], "open_positions": row[2], "pos_details": row[3]}

def sum_exits_between(start_ts: datetime, end_ts: datetime) -> Tuple[float, float]:
    sql = sa.text(f"""
        SELECT
          COALESCE(SUM(CASE WHEN COALESCE(net_pnl, pnl - fee) > 0 THEN COALESCE(net_pnl, pnl - fee) END), 0) AS pos_sum,
          COALESCE(SUM(CASE WHEN COALESCE(net_pnl, pnl - fee) < 0 THEN COALESCE(net_pnl, pnl - fee) END), 0) AS neg_sum
        FROM {TRADE_TABLE}
        WHERE exit_time IS NOT NULL
          AND exit_time >= :s AND exit_time < :e
    """)
    with _get_engine_any().connect() as conn:
        r = conn.execute(sql, {"s": start_ts, "e": end_ts}).fetchone()
        return (float(r[0] or 0.0), float(r[1] or 0.0))

def sum_exits_last_24h(end_ts: datetime) -> Tuple[float, float]:
    start = end_ts - timedelta(hours=24)
    return sum_exits_between(start, end_ts)

def list_new_positions(prev_symbols: set, current_symbols: set) -> List[str]:
    return sorted(list(current_symbols - prev_symbols))

# ====== Discord（walletnews優先） ======
def send_card_walletnews(title: str, desc: str, fields: list, color: int = 0x1e90ff) -> bool:
    url = os.getenv("DISCORD_URL_walletnews")
    if not url:
        return False
    embeds = [{
        "title": title,
        "description": desc,
        "color": color,
        "fields": [{"name": f.get("name", ""), "value": f.get("value", ""), "inline": bool(f.get("inline", False))}
                   for f in fields],
        "timestamp": datetime.now(timezone.utc).isoformat()  # ← ここを修正
    }]
    try:
        r = requests.post(url, json={"embeds": embeds}, timeout=7)
        return 200 <= r.status_code < 300
    except Exception as e:
        logger.warning("walletnews 送信失敗: %s", e)
        return False

# ====== main ======
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", type=str, default="", help="カードのタイトルを明示指定")
    ap.add_argument("--startup", action="store_true", help="起動カード用タイトルを使用（--titleより優先）")
    args = ap.parse_args()

    ensure_tables()

    client = HTTP(api_key=API_KEY, api_secret=API_SECRET, timeout=HTTP_TIMEOUT, testnet=False)

    # 日次ローリング確実化（閾値到達時は停止反映）
    try:
        risk_control.check_daily_drawdown()
    except Exception as e:
        logger.debug("daily rollover check skipped: %s", e)

    trading_active = bool(risk_control.ok_to_trade())

    # 収集
    balance     = get_wallet_balance(client)
    positions   = get_positions(client)
    cands       = count_candidates()
    manual_list = detect_manuals()

    # 集計
    open_cnt      = len(positions)
    total_pos_val = sum(f(p.get("positionValue")) for p in positions)
    total_upnl    = sum(f(p.get("unrealisedPnl")) for p in positions)
    day_pnl, day_ret = calc_daily_pnl(balance)

    pos_field_text, pos_details = build_positions_field(positions, balance, limit=10)

    now_ts = jst_now()

    # 前回スナップショット取得 → イベント集計
    last_snap = get_last_snapshot()
    if last_snap:
        prev_ts = last_snap["ts"]
        prev_symbols = set([d.get("symbol") for d in (last_snap.get("pos_details") or [])])
    else:
        prev_ts = now_ts - timedelta(minutes=5)
        prev_symbols = set()

    current_symbols = set([d["symbol"] for d in pos_details])
    new_syms = list_new_positions(prev_symbols, current_symbols)

    # exit PnL（前回→今回）、および直近24h
    pos_sum_now, neg_sum_now = sum_exits_between(prev_ts, now_ts)
    pos_sum_24h, neg_sum_24h = sum_exits_last_24h(now_ts)

    # スナップショット保存（可視化用）
    insert_snapshot(now_ts, balance, total_upnl, total_pos_val, open_cnt, pos_details)

    # カード構築
    title = "🚀 起動カード（ウォレット & ポジション）" if args.startup else (args.title or "📋 ウォレット & ポジション サマリ")

    # イベントサマリ文
    ev_lines = []
    if new_syms:
        ev_lines.append(f"🆕 新規ポジ +{len(new_syms)}: {', '.join(new_syms)}")
    if pos_sum_now or neg_sum_now:
        if pos_sum_now != 0:
            ev_lines.append(f"✅ 直近Exit 利益合算: +{pos_sum_now:.2f} USDT")
        if neg_sum_now != 0:
            ev_lines.append(f"❌ 直近Exit 損失合算: {neg_sum_now:.2f} USDT")
    if not ev_lines:
        ev_lines.append("（直近5分のイベントなし）")
    ev_text = "\n".join(ev_lines)

    fields: List[Dict[str, Any]] = [
        {"name": "💰 ウォレット残高", "value": fmt_usdt(balance), "inline": True},
        {"name": "📊 ポジション/上限", "value": f"{open_cnt}/{MAX_POSITIONS} 件", "inline": True},
        {"name": "📝 指値候補", "value": f"{cands} 件", "inline": True},
        {"name": "📈 評価額 / 評価損益", "value": f"{fmt_usdt(total_pos_val)} / {total_upnl:+.2f} USDT", "inline": False},
    ]

    if day_pnl is not None:
        s = f"{day_pnl:+.2f} USDT"
        if day_ret is not None:
            s += f"  ({day_ret:+.2f}%)"
        fields.append({"name": "📅 日次損益（残高ベース）", "value": s, "inline": False})

    fields.append({"name": "⚙ 稼働状態", "value": ("🟢 稼働中" if trading_active else "🔴 停止中"), "inline": True})
    fields.append({"name": "🧭 直近イベント（前回→今回）", "value": ev_text, "inline": False})
    fields.append({"name": "🕓 24時間Exit合算", "value": f"利益 +{pos_sum_24h:.2f} / 損失 {neg_sum_24h:.2f} USDT", "inline": False})
    fields.append({"name": "📦 明細（上位10件）", "value": pos_field_text, "inline": False})

    manual_text = "（検出なし）" if not manual_list else \
        "\n".join([f"・{m['symbol']} {m['side']} qty={m['qty']} @ {m['entry_price']:.6f}" for m in manual_list[:MANUAL_MAX_SHOW]]) + \
        (f"\n…他 {len(manual_list)-MANUAL_MAX_SHOW} 件" if len(manual_list) > MANUAL_MAX_SHOW else "")
    fields.append({"name": "🧭 手動ポジ検出", "value": manual_text, "inline": False})

    fields.append({"name": "🕐 実行情報", "value": f"JST {jst_now_str()} | PID:{os.getpid()}", "inline": False})

    # 送信：walletnews → フォールバック
    sent = send_card_walletnews(title, "━━━━━━━━━━━━━━━━━━", fields)
    if not sent:
        notify_news_card(title, "━━━━━━━━━━━━━━━━━━", fields, ok=trading_active)

if __name__ == "__main__":
    lock = FileLock(LOCK_PATH)
    try:
        lock.acquire(timeout=0)
    except Timeout:
        logging.getLogger("wallet_position_check").warning("duplicate run detected; skip")
        raise SystemExit(0)
    try:
        main()
    finally:
        try:
            lock.release()
        except Exception:
            pass
