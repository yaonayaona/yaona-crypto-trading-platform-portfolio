# order_utils.py
# -*- coding: utf-8 -*-
"""
order_utils.py

安全なオーダー発注・TP/SL設定ユーティリティ
"""

import os
import time
import logging
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

# 環境変数読み込み
load_dotenv()
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
client     = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

# TP/SL幅設定
TP_PCT     = float(os.getenv("TP_PCT", "0.005"))
SL_PCT     = float(os.getenv("SL_PCT", "0.0025"))
MULTIPLIER = float(os.getenv("SL_MULTIPLIER", "1"))
EFFECTIVE_TP_PCT = TP_PCT * MULTIPLIER
EFFECTIVE_SL_PCT = SL_PCT * MULTIPLIER

# ロガー設定
logger = logging.getLogger(__name__)

# risk helpers
from risk import get_min_qty, round_to_lot


def safe_place_order(params: dict):
    """
    指値や成行などのplace_orderを3回リトライ付きで実行。
    失敗時はNoneを返す。
    """
    for _ in range(3):
        try:
            return client.place_order(**params)
        except Exception as e:
            logger.warning(f"place_order retry: {e}")
            time.sleep(1)
    logger.error(f"place_order failed: {params}")
    return None


def safe_set_trading_stop(params: dict):
    """
    set_trading_stop (SL/TPトレードストップ設定) を3回リトライ付きで実行。
    失敗時はNoneを返す。
    """
    for _ in range(3):
        try:
            return client.set_trading_stop(**params)
        except Exception as e:
            logger.warning(f"set_trading_stop retry: {e}")
            time.sleep(1)
    logger.error(f"set_trading_stop failed: {params}")
    return None


def safe_cancel_order(order_id: str, symbol: str):
    """
    cancel_orderを試行。例外発生時はwarningログを残す。
    """
    try:
        client.cancel_order(category="linear", symbol=symbol, orderId=order_id)
        logger.info(f"Cancelled order: {symbol} {order_id}")
    except Exception as e:
        logger.warning(f"Failed cancel_order for {symbol}:{order_id} -> {e}")


def place_tp_sl(state: dict):
    """
    stateからsymbol, side, qty, entry_priceを読み取り、
    TP/SL（Limitおよびstop-loss）の発注を安全関数を使って実行。
    発注価格はstateに'tp_price', 'sl_price'として格納。
    """
    sym         = state['symbol']
    side        = state['side']
    raw_qty     = state['qty']
    entry_price = state['entry_price']

    # ---- 最小ロット/ステップ合わせ ----
    min_qty = get_min_qty(sym)
    qty     = round_to_lot(raw_qty, min_qty)
    if qty < min_qty:
        logger.info(
            "Skip TP/SL for %s: qty %.10g below min_qty %.10g",
            sym, raw_qty, min_qty
        )
        return
    # -----------------------------------

    # 反対売買
    opp = "Buy" if side == "Sell" else "Sell"

    # TP価格設定
    tp_price = round(
        entry_price * (1 - EFFECTIVE_TP_PCT) if side == "Sell"
        else entry_price * (1 + EFFECTIVE_TP_PCT),
        6
    )
    state['tp_price'] = tp_price
    safe_place_order({
        "category": "linear",
        "symbol": sym,
        "side": opp,
        "orderType": "Limit",
        "qty": qty,
        "price": tp_price,
        "timeInForce": "PostOnly",
        "reduceOnly": True
    })
    logger.info(f"TP order placed: {sym}@{tp_price}")

    # SL価格設定
    sl_price = round(
        entry_price * (1 + EFFECTIVE_SL_PCT) if side == "Sell"
        else entry_price * (1 - EFFECTIVE_SL_PCT),
        6
    )
    state['sl_price'] = sl_price
    safe_set_trading_stop({
        "category": "linear",
        "symbol": sym,
        "stopLoss": str(sl_price),
        "positionIdx": 0
    })
    logger.info(f"SL set: {sym}@{sl_price}")
