#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ws_listener.py ― Bybit プライベート WebSocket リスナー（order + execution 両対応）

– order_stream と execution_stream は callback だけ渡します
– symbols キーワードは使いません
"""

from __future__ import annotations
import os, time, json, logging
from datetime import datetime, timezone
from typing import Dict, Any

import requests
from dotenv import load_dotenv
from filelock import FileLock
from pybit.unified_trading import WebSocket

# ───────── 環境 & ロガー ─────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ws_listener")

API_KEY     = os.getenv("BYBIT_API_KEY")
API_SECRET  = os.getenv("BYBIT_API_SECRET")
WEBHOOK_URL = os.getenv("DISCORD_URL_shortlog") or os.getenv("DISCORD_WEBHOOK_URL")

QUEUE_PATH = "filled_queue.jsonl"
Q_LOCK     = FileLock(f"{QUEUE_PATH}.lock")

# ───────── キュー helper ─────────
def enqueue(item: Dict[str, Any]) -> None:
    with Q_LOCK:
        with open(QUEUE_PATH, "a") as fp:
            fp.write(json.dumps(item, ensure_ascii=False) + "\n")

def notify(msg: str) -> None:
    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={"content": msg}, timeout=5)
        except requests.RequestException:
            logger.warning("Discord notify failed")

# ───────── コールバック整形 ─────────
def _common_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp"  : datetime.now(timezone.utc).isoformat(),
        "symbol"     : raw["symbol"],
        "side"       : raw["side"],
        "qty"        : raw.get("execQty") or raw.get("qty"),
        "price"      : raw.get("execPrice") or raw.get("avgPrice") or raw.get("price"),
        "orderId"    : raw["orderId"],
        "createType" : raw.get("createType"),
        "execType"   : raw.get("execType"),
    }

def handle_order(msg: Dict[str, Any]) -> None:
    # 注文トピックで Filled 状態をキャッチ
    if msg.get("topic") != "order.linear":
        return
    raw = msg["data"][0]
    if raw.get("orderStatus") != "Filled":
        return
    enqueue(_common_payload(raw))
    logger.info(
        "OrderFilled: %s %s@%s (%s)",
        raw["symbol"],
        raw["qty"],
        raw.get("avgPrice") or raw.get("price"),
        raw.get("createType"),
    )

def handle_exec(msg: Dict[str, Any]) -> None:
    # execution トピックで約定をキャッチ
    if msg.get("topic") != "execution.linear":
        return
    raw = msg["data"][0]
    if raw.get("execType") not in ("Trade", "BustTrade"):
        return
    enqueue(_common_payload(raw))
    logger.info(
        "Exec: %s %s@%s (%s / %s)",
        raw["symbol"],
        raw["execQty"],
        raw["execPrice"],
        raw.get("createType"),
        raw.get("execType"),
    )

# ───────── 接続ループ ─────────
def _ws_listener() -> None:
    logger.info("Connecting WebSocket…")
    ws = WebSocket(
        channel_type="private",
        testnet=False,
        api_key=API_KEY,
        api_secret=API_SECRET,
        trace_logging=False,
    )
    # callback のみを渡す
    ws.order_stream(handle_order)
    ws.execution_stream(handle_exec)
    logger.info("WebSocket connected")
    # keep alive
    while True:
        time.sleep(30)

def main() -> None:
    while True:
        try:
            _ws_listener()
        except Exception as e:
            logger.exception("WS listener crashed: %s", e)
            notify(f"🔴 ws_listener crashed: {e}\n5s later reconnect")
            time.sleep(5)

if __name__ == "__main__":
    main()
