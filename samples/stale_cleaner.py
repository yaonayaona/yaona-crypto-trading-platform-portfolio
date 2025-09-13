#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stale_cleaner.py ― 期限切れ注文の自動キャンセル

変更点:
- 通知は position_notifier.notify_stale() を使用（DISCORD_URL_positionnews を最優先）
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv
from filelock import FileLock
from pybit.unified_trading import HTTP

from file_utils import load_json, save_json
from position_notifier import notify_stale

load_dotenv()
logger = logging.getLogger("stale_cleaner")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
client     = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

STALE_MINUTES = int(os.getenv("STALE_MINUTES", "7"))
DRY_RUN       = (os.getenv("DRY_RUN", "1") == "1")

def _age_minutes(iso_ts: str) -> int:
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except Exception:
        return 10 ** 9
    return int((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() // 60)

def _cancel(symbol: str, order_id: str) -> bool:
    if DRY_RUN:
        return True
    try:
        resp = client.cancel_order(category="linear", symbol=symbol, orderId=order_id)
        ok = (resp.get("retCode") == 0)
        if not ok:
            logger.warning("cancel_order NG %s %s: %s", symbol, order_id, resp)
        return ok
    except Exception as e:
        logger.warning("cancel_order error %s %s: %s", symbol, order_id, e)
        return False

def clean_stale(candidates_file: Path) -> None:
    """entry_candidates.json を見て STALE_MINUTES 超過をキャンセル・通知"""
    lock = FileLock(f"{candidates_file}.lock")
    try:
        with lock:
            arr: List[Dict[str, Any]] = load_json(candidates_file)
            if not arr:
                return

            keep: List[Dict[str, Any]] = []
            for c in arr:
                ts = c.get("timestamp") or c.get("ts")
                sym = c.get("symbol")
                oid = c.get("order_id")
                if not (ts and sym and oid):
                    keep.append(c); continue

                age = _age_minutes(ts)
                if age >= STALE_MINUTES:
                    if _cancel(sym, oid):
                        notify_stale(sym, oid, age)
                    # キャンセル済みは残さない
                else:
                    keep.append(c)

            if keep != arr:
                save_json(candidates_file, keep)
    except Exception as e:
        logger.warning("clean_stale error: %s", e)

if __name__ == "__main__":
    clean_stale(Path("entry_candidates.json"))
