# order_patch.py
# -*- coding: utf-8 -*-
"""
Bybit V5 place_order モンキーパッチ（例外版）

■ 目的
  * HTTP.place_order が ErrCode=110017 のとき例外を握りつぶし、
    正常扱いとして返す。
  * それ以外の例外はそのまま再スローして上位に通知。

■ 使い方
  position_monitor.py の冒頭に

      from __future__ import annotations
      import trading_stop_patch   # 34040吸収
      import order_patch          # 110017吸収
      ...

  と入れるだけ。
"""

from __future__ import annotations
import logging
from pybit.unified_trading import HTTP

logger = logging.getLogger(__name__)

# 元メソッドを保持
_original_place_order = HTTP.place_order

def _patched_place_order(self: HTTP, **params):
    """
    HTTP.place_order をラップし、
    - ErrCode=110017 ("orderQty will be truncated to zero") の例外は握りつぶす
    - それ以外は再スロー
    """
    try:
        # 正常系
        return _original_place_order(self, **params)
    except Exception as e:
        msg = str(e)
        # 例外メッセージに 110017 を含むなら正常扱い
        if "110017" in msg:
            logger.info("Order skipped (qty too small, code 110017): %s %s qty=%s",
                        params.get("symbol"), params.get("side"), params.get("qty"))
            # ダミーの戻り値を返して safe_place_order の retry を止める
            return {"retCode": 110017, "retMsg": "qty truncated to zero"}
        # それ以外はそのまま例外を再スロー
        raise

# モンキーパッチ適用
HTTP.place_order = _patched_place_order
logger.info("✅ order_patch applied (ErrCode 110017 exceptions will be suppressed)")
