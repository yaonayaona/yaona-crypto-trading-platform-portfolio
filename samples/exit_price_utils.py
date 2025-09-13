#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
exit_price_utils.py  ── filled_time ベース & 分割決済対応 “上限なし” 版

■ 変更点（2025-XX-XX）
  * 最終決済の orderId を返すように _aggregate を拡張
  * get_closed_pnl() が exit_order_id を含めて返却
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Tuple, List, Optional

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

# ── Bybit クライアント ─────────────────────────────
load_dotenv()
client = HTTP(
    api_key    = os.getenv("BYBIT_API_KEY"),
    api_secret = os.getenv("BYBIT_API_SECRET"),
    testnet    = False,
)

# ── ロガー ────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ────────────────────────────────────────────────
#  ユーティリティ
# ────────────────────────────────────────────────
def _fetch_closed_pnls(symbol: str, start_ms: int, end_ms: int, limit: int = 50) -> List[dict]:
    """Closed-PnL API 呼び出し。失敗時は空リスト"""
    try:
        resp = client.get_closed_pnl(
            category="linear",
            symbol=symbol,
            startTime=start_ms,
            endTime=end_ms,
            limit=limit,
        )
        return resp.get("result", {}).get("list", [])
    except Exception as e:
        logger.warning(f"[closed_pnl] API error for {symbol}: {e}")
        print(f"[DEBUG] API error: {e}")
        return []

def _filter_since_filled(items: List[dict], filled_ts: int) -> List[dict]:
    """filled_time 以降の履歴だけ残す（上限なし）"""
    return [i for i in items if int(i.get("updatedTime", 0)) >= filled_ts]

def _aggregate(items: List[dict]) -> Tuple[float, float, int, Optional[str]]:
    """
    部分約定レコードを合算
      → (exit_price, closed_pnl, last_updatedTime(ms), last_order_id)
    """
    if not items:
        return 0.0, 0.0, 0, None

    total_qty      = 0.0
    total_value    = 0.0
    total_pnl      = 0.0
    last_ts        = 0
    last_order_id  = None

    for rec in items:
        qty   = float(rec.get("closedSize") or rec.get("cumExecQty") or 0)
        price = float(rec.get("avgExitPrice") or 0)
        pnl   = float(rec.get("closedPnl")    or 0)
        ts    = int(rec.get("updatedTime")    or 0)
        oid   = rec.get("orderId")

        total_qty      += qty
        total_value    += price * qty
        total_pnl      += pnl
        if ts > last_ts:
            last_ts       = ts
            last_order_id = oid

    exit_price = total_value / total_qty if total_qty else 0.0
    return exit_price, total_pnl, last_ts, last_order_id

# ────────────────────────────────────────────────
#  メイン API
# ────────────────────────────────────────────────
def get_closed_pnl(
    symbol: str,
    state: dict,
    retry: int = 3,
    retry_wait: float = 2.0
) -> Tuple[float, float, datetime, Optional[str]]:
    """
    filled_time 以降のすべての決済履歴を合算し、
      → exit_price, closed_pnl, exit_time(datetime, UTC), exit_order_id を返す。
    """
    filled_iso  = state.get("filled_time")
    entry_price = float(state.get("entry_price", 0.0))

    try:
        filled_ts = int(datetime.fromisoformat(filled_iso).timestamp() * 1000)
    except Exception:
        filled_ts = int(time.time() * 1000) - 300_000   # fallback: 5 分前

    logger.debug(f"[closed_pnl] start: {symbol}, filled_ts={filled_ts}")

    # API 取得範囲：filled_time の 30 秒前から現在まで
    start_ms = filled_ts - 30_000

    for attempt in range(1, retry + 1):
        end_ms = int(time.time() * 1000)
        items  = _fetch_closed_pnls(symbol, start_ms, end_ms)

        logger.debug(f"[closed_pnl] attempt {attempt}: items={len(items)}")

        matches = _filter_since_filled(items, filled_ts)
        logger.debug(f"[closed_pnl] matches={len(matches)}")

        if matches:
            exit_price, closed_pnl, last_ts, exit_oid = _aggregate(matches)
            logger.debug(f"[closed_pnl] aggregated: price={exit_price}, pnl={closed_pnl}, oid={exit_oid}")

            exit_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
            return exit_price, closed_pnl, exit_dt, exit_oid

        if attempt < retry:
            time.sleep(retry_wait)

    # すべて失敗 → フォールバック
    logger.warning(f"[closed_pnl] Fallback: symbol={symbol}")
    exit_dt = datetime.now(timezone.utc)
    return entry_price, 0.0, exit_dt, None
