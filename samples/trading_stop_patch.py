# trading_stop_patch.py
# -*- coding: utf-8 -*-
"""
Bybit V5 set_trading_stop モンキーパッチ（例外 & retCode 両対応）
  * retCode / 例外 いずれの形でも 34040 を正常扱いにする
"""
from __future__ import annotations
import logging
from pybit.unified_trading import HTTP

logger = logging.getLogger(__name__)

_orig_set_trading_stop = HTTP.set_trading_stop

def _patched_set_trading_stop(self: HTTP, *, category: str, symbol: str,
                              stopLoss: str | float, positionIdx: int = 0, **kw):
    try:
        # 通常はここで resp(dict) が返る
        resp = _orig_set_trading_stop(
            self, category=category, symbol=symbol,
            stopLoss=str(stopLoss), positionIdx=positionIdx, **kw
        )
        code = resp.get("retCode", resp.get("ret_code", 0))
        if code in (0, 34040):
            logger.info("SL set (code=%s): %s @ %s", code, symbol, stopLoss)
            return resp
        # 非 0・34040 はエラーにする
        raise RuntimeError(f"retCode={code} retMsg={resp.get('retMsg')}")
    except Exception as e:
        # ライブラリが Exception で 34040 を返すケースを吸収
        if "34040" in str(e):
            logger.info("SL already set (exception 34040 absorbed): %s @ %s", symbol, stopLoss)
            return {"retCode": 34040, "retMsg": "not modified"}
        # それ以外は再スロー
        raise

HTTP.set_trading_stop = _patched_set_trading_stop
logger.info("✅ trading_stop_patch applied (34040 absorbed, warnings消滅)")
