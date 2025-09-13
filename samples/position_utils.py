#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
position_utils.py

ポジション解放待ちおよび緊急停止機能を提供するユーティリティモジュール。
"""
import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

from file_utils import load_json, save_json
from order_utils import safe_cancel_order, safe_place_order

# 環境変数読み込み・Bybitクライアント初期化
load_dotenv()
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
client     = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

logger = logging.getLogger(__name__)


def wait_until_cleared(symbol: str, poll_interval: int):
    """
    指定シンボルのポジションがすべてクリアされるまで待機する。
    """
    logger.debug(f"Waiting for position clear: {symbol}")
    while True:
        try:
            resp = client.get_positions(category="linear", symbol=symbol)
            open_list = [p for p in resp.get("result", {}).get("list", []) if float(p.get("size") or 0) != 0]
            if not open_list:
                logger.info(f"Position cleared: {symbol}")
                return
        except Exception as e:
            logger.warning(f"Error in wait_until_cleared for {symbol}: {e}")
        time.sleep(poll_interval)


def emergency_close_all(candidates_file: Path, poll_interval: int):
    """
    全エントリー指値をキャンセルし、すべての建玉をマーケット決済でクローズする。
    """
    # コンソール出力
    print("🔥 EMERGENCY STOP: clearing positions & entry orders")
    logger.warning("🔥 Emergency stop triggered: clearing positions & orders")

    # 1) entry_candidates.jsonの指値をすべてキャンセル
    candidates = load_json(candidates_file)
    for c in candidates:
        safe_cancel_order(c.get("order_id"), c.get("symbol"))
    save_json(candidates_file, [])

    # 2) 全建玉をマーケット注文で決済
    try:
        resp = client.get_positions(category="linear", settleCoin="USDT")
        for pos in resp.get("result", {}).get("list", []):
            size = float(pos.get("size") or 0)
            if size <= 0:
                continue
            sym = pos.get("symbol")
            opp = "Buy" if pos.get("side") == "Sell" else "Sell"
            params = {
                "category": "linear",
                "symbol": sym,
                "side": opp,
                "orderType": "Market",
                "qty": size,
                "reduceOnly": True
            }
            safe_place_order(params)
            logger.info(f"Emergency closed position: {sym}@{size} via {opp}")
    except Exception as e:
        logger.error(f"emergency_close_all market close failed: {e}")

    # プロセス停止
    exit(0)
