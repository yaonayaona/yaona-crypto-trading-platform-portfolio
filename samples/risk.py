# risk.py
# -*- coding: utf-8 -*-
"""
リスク・口座ユーティリティ（Bybit UTA 2.0対応・安全化版）

このファイルは yaona_short_v1 系から以下を提供します：
- `session` : pybit.unified_trading.HTTP のシングルトン
- `DEFAULT_LEVERAGE`, `DEFAULT_EQUITY_PCT`, `DEFAULT_MIN_QTY`
- `get_wallet_balance()` : UNIFIED口座の totalEquity を60sキャッシュ
- `get_instrument(symbol)` / `get_min_qty(symbol)` / `get_qty_step(symbol)`
- `round_to_lot(qty, min_qty, symbol=None)` : lot stepで丸め（不足はmin補正）
- `ensure_margin_mode(symbol)` : （安全のため）原則NO-OP
- `ensure_leverage(symbol, desired)` : PM/Crossではスキップ、同値は叩かない
- `calc_order_qty(symbol, price, leverage=None, equity_pct=None)`
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import os
import time
import math
import logging
from functools import lru_cache
from typing import Optional, Dict, Any

from pybit.unified_trading import HTTP

# ────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ────────────────────────────────────────────────────────────
# HTTP client (singleton)
# ────────────────────────────────────────────────────────────
TESTNET = bool(int(os.getenv("BYBIT_TESTNET", "0")))
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
RECV_WINDOW = int(os.getenv("BYBIT_RECV_WINDOW", "5000"))

session = HTTP(
    testnet=TESTNET,
    api_key=API_KEY,
    api_secret=API_SECRET,
    recv_window=RECV_WINDOW,
)

# ────────────────────────────────────────────────────────────
# Defaults
# ────────────────────────────────────────────────────────────
DEFAULT_LEVERAGE   = int(os.getenv("DEFAULT_LEVERAGE", "2"))
DEFAULT_EQUITY_PCT = float(os.getenv("DEFAULT_EQUITY_PCT", "0.01"))   # 1%
DEFAULT_MIN_QTY    = float(os.getenv("DEFAULT_MIN_QTY",  "0.001"))
DEFAULT_QTY_STEP   = float(os.getenv("DEFAULT_QTY_STEP", "0.001"))
WALLET_TTL_SEC     = int(os.getenv("WALLET_TTL_SEC", "60"))

# ────────────────────────────────────────────────────────────
# Instruments cache helpers
# ────────────────────────────────────────────────────────────
@lru_cache(maxsize=1024)
def get_instrument(symbol: str) -> Optional[Dict[str, Any]]:
    """linear(USDT無期限) の銘柄仕様を取得。失敗時 None"""
    try:
        r = session.get_instruments_info(category="linear", symbol=symbol)
        lst = (r.get("result") or {}).get("list") or []
        return lst[0] if lst else None
    except Exception as e:
        logger.debug(f"get_instruments_info failed for {symbol}: {e}")
        return None

def _to_float(d: Dict[str, Any], *keys, default: float = 0.0) -> float:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    try:
        return float(cur)
    except Exception:
        return default

def get_qty_step(symbol: str) -> float:
    inst = get_instrument(symbol) or {}
    return _to_float(inst, "lotSizeFilter", "qtyStep", default=DEFAULT_QTY_STEP) or DEFAULT_QTY_STEP

def get_min_qty(symbol: str) -> float:
    inst = get_instrument(symbol) or {}
    # Bybitは minOrderQty あるいは minOrderQty が無い場合は最小stepを採用
    mn = _to_float(inst, "lotSizeFilter", "minOrderQty", default=0.0)
    if mn <= 0:
        mn = get_qty_step(symbol)
    return max(mn, DEFAULT_MIN_QTY)

# ────────────────────────────────────────────────────────────
# Wallet (60秒キャッシュ)
# ────────────────────────────────────────────────────────────
_bal_cache_val: float = 0.0
_bal_cache_ts: float = 0.0

def get_wallet_balance() -> float:
    """UNIFIED口座の USDT totalEquity を返す（TTL 60s）"""
    global _bal_cache_val, _bal_cache_ts
    now = time.time()
    if now - _bal_cache_ts <= WALLET_TTL_SEC and _bal_cache_val > 0:
        return _bal_cache_val
    try:
        r = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        val = float(((r.get("result") or {}).get("list") or [{}])[0].get("totalEquity") or 0.0)
        if val > 0:
            _bal_cache_val = val
            _bal_cache_ts = now
        return val
    except Exception as e:
        logger.warning(f"get_wallet_balance failed: {e}")
        return _bal_cache_val or 0.0

# ────────────────────────────────────────────────────────────
# Rounding
# ────────────────────────────────────────────────────────────
def round_to_step(qty: float, step: float) -> float:
    if step <= 0:
        return max(qty, 0.0)
    # avoid FP drift
    units = math.floor(qty / step + 1e-12)
    return round(units * step, 12)

def round_to_lot(qty: float, min_qty: float, symbol: Optional[str] = None) -> float:
    """数量を lotStep で丸め、min 未満ならminまで底上げ"""
    step = get_qty_step(symbol) if symbol else max(min_qty, DEFAULT_QTY_STEP)
    q = round_to_step(max(qty, 0.0), step)
    if 0 < q < min_qty:
        q = min_qty
    return q

# ────────────────────────────────────────────────────────────
# Margin mode / Leverage
# ────────────────────────────────────────────────────────────
_last_skip: Dict[str, float] = {}  # for cooling down noisy logs

def ensure_margin_mode(symbol: str) -> bool:
    """
    UTA2.0/PM環境ではレバ/モードの強制変更はバリデーションが厳しく、
    不要 or 不可な場合が多い。安全のため **NO-OP** とする。
    """
    # ここで何かしたい場合は、switch-isolated/cross などを実装する。
    # 現状は余計なエラー増殖を避ける目的で True を返すだけ。
    return True

def ensure_leverage(symbol: str, desired: float | int | str) -> bool:
    """
    - 銘柄が linear に存在しない → スキップ
    - PM/Cross などで leverage 無効（空文字/None） → スキップ
    - 既設定と同じ → API叩かない
    - それ以外 → maxLeverage にクランプして set_leverage
    - 10001/110036/110038 → “禁止/不適合”としてスキップ（1分クールダウンで静かに）
    """
    inst = get_instrument(symbol)
    if not inst:
        return False

    # 現在レバ取得（PMだと空文字が返る）
    try:
        pos = session.get_positions(category="linear", symbol=symbol)
        cur_list = ((pos.get("result") or {}).get("list") or [])
        lev_now = cur_list[0].get("leverage") if cur_list else None
    except Exception as e:
        logger.debug(f"get_positions failed {symbol}: {e}")
        lev_now = None

    if lev_now in ("", None):
        # PM/Cross など leverage 概念が無効
        t0 = _last_skip.get(symbol, 0.0)
        if time.time() - t0 > 60:
            logger.info(f"[ensure_leverage] skip {symbol}: leverage unavailable (PM/Cross likely)")
            _last_skip[symbol] = time.time()
        return False

    # 同値なら何もしない
    try:
        if float(lev_now) == float(desired):
            return True
    except Exception:
        pass

    # 上限にクランプ + 文字列で送る
    max_lev = _to_float(inst, "leverageFilter", "maxLeverage", default=float(desired))
    lev = str(int(min(float(desired), max_lev)))

    try:
        r = session.set_leverage(category="linear", symbol=symbol,
                                 buyLeverage=lev, sellLeverage=lev)
        rc = r.get("retCode", -1)
        if rc == 0:
            return True
        if rc in (10001, 110036, 110038):
            t0 = _last_skip.get(symbol, 0.0)
            if time.time() - t0 > 60:
                logger.info(f"[ensure_leverage] skip {symbol}: retCode={rc}")
                _last_skip[symbol] = time.time()
            return False
        # その他は上位へ
        raise RuntimeError(f"set_leverage failed for {symbol}: {r}")
    except Exception as e:
        logger.warning(f"set_leverage exception for {symbol}: {e}")
        return False

# ────────────────────────────────────────────────────────────
# Sizing
# ────────────────────────────────────────────────────────────
def calc_order_qty(
    symbol: str,
    price: float,
    leverage: Optional[float] = None,
    equity_pct: Optional[float] = None,
) -> float:
    """
    口座残高 × pct × leverage / price を lot へ丸め。
    - leverage を指定しない場合は DEFAULT_LEVERAGE
    - equity_pct を指定しない場合は DEFAULT_EQUITY_PCT
    """
    lev = float(leverage) if leverage is not None else float(DEFAULT_LEVERAGE)
    pct = float(equity_pct) if equity_pct is not None else float(DEFAULT_EQUITY_PCT)

    # レバ設定は安全に（PM/Cross時はスキップされる設計）
    ensure_leverage(symbol, lev)

    equity = float(get_wallet_balance())
    min_qty = float(get_min_qty(symbol))
    raw_qty = (equity * pct * lev) / float(price)
    qty = round_to_lot(raw_qty, min_qty, symbol=symbol)

    if qty < min_qty:
        # 最小注文に満たない場合は最小サイズを返す（上位側で弾くか判断）
        logger.warning(f"Calculated qty {qty} below min_qty {min_qty}, using min_qty")
        qty = min_qty

    logger.info(f"Order qty for {symbol}: {qty} (equity={equity:.4f}, pct={pct}, lev={lev}, price={price})")
    return qty
