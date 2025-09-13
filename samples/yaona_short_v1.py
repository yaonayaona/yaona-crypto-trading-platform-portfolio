#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
yaona_short_v1.py – one-shotショートBot（カード通知対応版）

[修正履歴]
- 2025-08-16: カード通知対応、停止理由判定、戦略ヒット数表示
"""

from __future__ import annotations

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
import requests
from dotenv import load_dotenv
from filelock import FileLock, Timeout
from pybit.unified_trading import HTTP

# ──────────────────────────────
# .env 読み込み & ユーティリティ
# ──────────────────────────────
load_dotenv()

def senv(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

logging.basicConfig(
    level=senv("LOG_LEVEL", "INFO") or "INFO",
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ──────────────────────────────
# 環境変数（すべて senv 経由）
# ──────────────────────────────
API_KEY        = senv("BYBIT_API_KEY")
API_SECRET     = senv("BYBIT_API_SECRET")
# ★ 通知先：positionnews を最優先
WEBHOOK_URL    = (
    senv("DISCORD_URL_positionnews")
    or senv("DISCORD_URL_shortlog")
    or senv("DISCORD_WEBHOOK_URL")
    or ""
)
HTTP_TIMEOUT   = float(senv("BYBIT_HTTP_TIMEOUT", "3") or "3")

DRY_RUN        = (senv("DRY_RUN", "1") or "1") == "1"
STRATEGY_PATH  = senv("STRATEGY_PATH", "active.json") or "active.json"
BAR_COUNT_EACH = int(senv("BAR_COUNT_EACH", "30") or "30")
ENTRY_OFFSET   = float(senv("ENTRY_OFFSET", "0.003") or "0.003")
PROCESS_LOCK_PATH = senv("PROCESS_LOCK_PATH", "/run/yaona_short.lock") or "/run/yaona_short.lock"
# 上限制御
MAX_POSITIONS  = int(senv("MAX_POSITIONS", senv("MAX_POS", "3")) or "3")

# TP/SL設定
TP_PCT     = float(senv("TP_PCT", "0.005") or "0.005")
SL_PCT     = float(senv("SL_PCT", "0.0025") or "0.0025")
MULTIPLIER = float(senv("SL_MULTIPLIER", "1") or "1")
EFFECTIVE_TP_PCT = TP_PCT * MULTIPLIER
EFFECTIVE_SL_PCT = SL_PCT * MULTIPLIER

TIMEFRAMES     = ["5m", "15m", "1h", "4h"]

# ──────────────────────────────
# 通知（フォールバック用）
# ──────────────────────────────
def notify(msg: str) -> None:
    """Discord へ通知（PID + JST を自動付与）"""
    if not WEBHOOK_URL:
        return
    try:
        pid = os.getpid()
        jst = datetime.now(timezone.utc).astimezone(
            timezone(timedelta(hours=9))
        ).strftime("%H:%M:%S")
        content = f"{msg}\n🔎 PID:{pid} JST:{jst}"
        requests.post(WEBHOOK_URL, json={"content": content}, timeout=3)
    except Exception as e:
        logging.warning("notify failed: %s", e)

# ──────────────────────────────
# 依存モジュール
# ──────────────────────────────
import data_provider as DP
from backtest_engine.strategy_builder import build_strategy
import risk_control
from risk import calc_order_qty, ensure_margin_mode, ensure_leverage, get_wallet_balance
from order_utils import safe_place_order
from file_utils import load_json, save_json
from position_notifier import notify_entry_candidate

# state_manager（entry_states管理）— 存在しない環境でも動くようフォールバック
try:
    from state_manager import list_symbols as _list_states
except Exception:
    def _list_states() -> List[str]:
        try:
            return [p.stem for p in Path("entry_states").glob("*.json")]
        except Exception:
            return []

# ──────────────────────────────
# ストラテジ
# ──────────────────────────────
try:
    STRATEGY_CONFIG = json.load(open(STRATEGY_PATH))
    STRATEGY_FN = build_strategy(STRATEGY_CONFIG)
    STRATEGY_NAME = STRATEGY_CONFIG.get("name", "Unknown")
except Exception as e:
    notify(f"🔴 active.json load error → {e}")
    STRATEGY_NAME = "Fallback"
    def STRATEGY_FN(df: pd.DataFrame):
        return df.assign(entry=False, tp=0.02, sl=0.03)

# ──────────────────────────────
# Bybit クライアント
# ──────────────────────────────
client = HTTP(api_key=API_KEY, api_secret=API_SECRET, timeout=HTTP_TIMEOUT)

def _get_symbols() -> List[str]:
    return [d["symbol"] for d in client.get_tickers(category="linear")["result"]["list"]]

def _get_price_map() -> Dict[str, float]:
    return {d["symbol"]: float(d["lastPrice"]) for d in client.get_tickers(category="linear")["result"]["list"]}

# ──────────────────────────────
# 開ポジ検出（REST + ローカル state 合算）
# ──────────────────────────────
def get_open_symbols() -> Set[str]:
    syms: Set[str] = set()

    # 1) Unified get_positions（第一候補）
    try:
        resp = client.get_positions(category="linear", settleCoin="USDT")
        for p in resp.get("result", {}).get("list", []):
            sz = p.get("size") or p.get("positionAmt") or "0"
            try:
                if float(sz or 0) > 0:
                    syms.add(p["symbol"]); continue
            except Exception:
                pass
            if float(p.get("positionValue") or 0) > 0 or float(p.get("avgPrice") or 0) > 0:
                syms.add(p["symbol"])
    except Exception as e:
        logging.warning("get_positions failed: %s", e)

    # 2) 旧API名のフォールバック
    if not syms:
        try:
            r2 = client.get_position_list(category="linear", settleCoin="USDT")
            for p in r2.get("result", {}).get("list", []):
                try:
                    if float(p.get("size") or 0) > 0:
                        syms.add(p["symbol"])
                except Exception:
                    pass
        except Exception as e2:
            logging.warning("get_position_list failed: %s", e2)

    # 3) ローカル entry_states も合算
    try:
        syms |= set(_list_states())
    except Exception as e:
        logging.debug("state_manager list_symbols failed: %s", e)

    return syms

# ──────────────────────────────
# 🆕 停止理由判定
# ──────────────────────────────
def get_stop_reason() -> str:
    """リスク制御の停止理由を取得"""
    try:
        state = risk_control.load_state()
        if not state.get("stopped", False):
            return ""
        
        # 連敗チェック
        consec_losses = state.get("consec_losses", 0)
        max_losses = int(os.getenv("MAX_CONSEC_LOSSES", "5"))
        if consec_losses >= max_losses:
            return f"連敗停止中({consec_losses}/{max_losses}回)"
        
        # ドローダウンチェック
        try:
            if risk_control.check_daily_drawdown():
                max_dd = float(os.getenv("MAX_DRAWDOWN", "0.02"))
                return f"DD停止中(-{max_dd*100:.1f}%)"
        except Exception:
            pass
        
        return "停止中"
    except Exception:
        return "状態不明"

# ──────────────────────────────
# 候補ファイル
# ──────────────────────────────
CANDIDATE_FILE = Path("entry_candidates.json")
CAND_LOCK      = FileLock(f"{CANDIDATE_FILE}.lock")

# ──────────────────────────────
# メイン（ワンショット）
# ──────────────────────────────
def main_once() -> None:
    t0 = time.time()
    sent = 0

    # ── リスク制御
    is_trading_active = risk_control.ok_to_trade()
    stop_reason = get_stop_reason() if not is_trading_active else ""

    # ── スナップショット prime
    try:
        DP.prime_cache_from_snapshot(TIMEFRAMES)
    except Exception as e:
        logging.warning("[DP] prime miss: %s", e)

    # ── 差分 refresh（失敗しても継続：snapshot-only）
    refreshed = False
    try:
        DP.refresh_cache(TIMEFRAMES)
        refreshed = True
    except Exception as e:
        logging.warning("[DP] refresh failed → snapshot-only mode: %s", e)
        notify(f"🟡 DB unreachable? snapshot-only mode で継続: {e}")

    # ── スナップショット保存（更新できた時だけ）
    if refreshed:
        try:
            DP.save_snapshot(TIMEFRAMES)
        except Exception as e:
            logging.warning("[DP] save_snapshot failed: %s", e)

    # ── シンボル & 価格
    try:
        symbols = _get_symbols()
        if not symbols:
            notify("ℹ️ 対象銘柄が0件のため終了")
            return
        price_map = _get_price_map()
        total_symbols = len(symbols)
    except Exception as e:
        notify(f"🔴 ticker fetch error: {e}")
        return

    # ── 直近 n 本取得
    try:
        recent_map = DP.get_latest_all(
            timeframes=TIMEFRAMES,
            n=BAR_COUNT_EACH,
            symbols=symbols,
            reset_index=True
        )
    except Exception as e:
        notify(f"🔴 get_latest_all error: {e}")
        return

    # ── TF 別 dict へ整理（tf → {symbol: DataFrame}）
    tf_map: Dict[str, Dict[str, pd.DataFrame]] = {}
    for tf, df in recent_map.items():
        tf_map[tf] = {
            sym: g.sort_values("time").reset_index(drop=True)
            for sym, g in df.groupby("symbol")
        }

    # ── 上限制御
    open_syms = get_open_symbols()
    with CAND_LOCK:
        active_cands = [c for c in load_json(CANDIDATE_FILE) if c.get("side") == "Sell"]
        cand_syms    = {c.get("symbol") for c in active_cands if c.get("symbol")}

    current_positions = len(open_syms)
    remaining = MAX_POSITIONS - current_positions - len(active_cands)

    # ── 戦略ヒット数カウント
    strategy_hits = 0
    first_hit_symbol = None
    
    for sym in symbols:
        # 全TFが揃っているかチェック
        dfs: List[pd.DataFrame] = []
        for tf in TIMEFRAMES:
            d = tf_map.get(tf, {}).get(sym)
            if d is None or d.empty:
                break
            dfs.append(d)
        else:
            df_all = pd.concat(dfs, ignore_index=True).sort_values("time")
            sig = STRATEGY_FN(df_all)

            if bool(sig["entry"].iloc[-1]):
                strategy_hits += 1
                if first_hit_symbol is None and sym not in open_syms and sym not in cand_syms:
                    first_hit_symbol = sym

    # ── ウォレット残高取得
    try:
        wallet_balance = get_wallet_balance()
    except Exception as e:
        logging.warning("wallet balance fetch failed: %s", e)
        wallet_balance = 10000.0  # フォールバック

    # ── シグナル評価（発注処理）
    if remaining <= 0:
        # 上限到達でも最初のヒット1件だけカード通知
        if first_hit_symbol and strategy_hits > 0:
            sym = first_hit_symbol
            last_price = price_map.get(sym)
            if last_price:
                ensure_margin_mode(sym)
                ensure_leverage(sym, 2)
                qty = calc_order_qty(sym, last_price)
                limit_price = round(last_price * (1 + ENTRY_OFFSET), 6)
                
                # TP/SL計算
                tp_price = round(limit_price * (1 - EFFECTIVE_TP_PCT), 6)
                sl_price = round(limit_price * (1 + EFFECTIVE_SL_PCT), 6)
                tp_pnl = (limit_price - tp_price) * qty
                sl_pnl = (limit_price - sl_price) * qty
                
                notify_entry_candidate(
                    symbol=sym,
                    current_price=last_price,
                    limit_price=limit_price,
                    qty=qty,
                    wallet_balance=wallet_balance,
                    strategy_name=STRATEGY_NAME,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    tp_pnl=tp_pnl,
                    sl_pnl=sl_pnl,
                    current_positions=current_positions,
                    max_positions=MAX_POSITIONS,
                    strategy_hits=strategy_hits,
                    total_symbols=total_symbols,
                    is_trading_active=is_trading_active,
                    stop_reason=stop_reason,
                    actually_entered=False  # 🆕 上限到達のため実際はエントリーしていない
                )
        else:
            notify(f"⚠️ 上限 {MAX_POSITIONS} 件到達、戦略ヒット: {strategy_hits}/{total_symbols}銘柄")
        return

    if not is_trading_active:
        # 停止中でも最初のヒット1件だけカード通知
        if first_hit_symbol and strategy_hits > 0:
            sym = first_hit_symbol
            last_price = price_map.get(sym)
            if last_price:
                ensure_margin_mode(sym)
                ensure_leverage(sym, 2)
                qty = calc_order_qty(sym, last_price)
                limit_price = round(last_price * (1 + ENTRY_OFFSET), 6)
                
                # TP/SL計算
                tp_price = round(limit_price * (1 - EFFECTIVE_TP_PCT), 6)
                sl_price = round(limit_price * (1 + EFFECTIVE_SL_PCT), 6)
                tp_pnl = (limit_price - tp_price) * qty
                sl_pnl = (limit_price - sl_price) * qty
                
                notify_entry_candidate(
                    symbol=sym,
                    current_price=last_price,
                    limit_price=limit_price,
                    qty=qty,
                    wallet_balance=wallet_balance,
                    strategy_name=STRATEGY_NAME,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    tp_pnl=tp_pnl,
                    sl_pnl=sl_pnl,
                    current_positions=current_positions,
                    max_positions=MAX_POSITIONS,
                    strategy_hits=strategy_hits,
                    total_symbols=total_symbols,
                    is_trading_active=is_trading_active,
                    stop_reason=stop_reason,
                    actually_entered=False  # 🆕 停止中のため実際はエントリーしていない
                )
        else:
            notify(f"⚠️ {stop_reason}、戦略ヒット: {strategy_hits}/{total_symbols}銘柄")
        return

    # ── 実際の発注処理
    for sym in symbols:
        if remaining <= 0:
            break
        if sym in open_syms:
            continue
        if sym in cand_syms:
            continue

        # 全TFが揃っているかチェック
        dfs: List[pd.DataFrame] = []
        for tf in TIMEFRAMES:
            d = tf_map.get(tf, {}).get(sym)
            if d is None or d.empty:
                break
            dfs.append(d)
        else:
            df_all = pd.concat(dfs, ignore_index=True).sort_values("time")
            sig = STRATEGY_FN(df_all)

            if not bool(sig["entry"].iloc[-1]):
                continue

            last_price = price_map.get(sym)
            if last_price is None:
                continue

            ensure_margin_mode(sym)
            ensure_leverage(sym, 2)
            qty = calc_order_qty(sym, last_price)
            limit_price = round(last_price * (1 + ENTRY_OFFSET), 6)

            order_id: str | None = None
            if DRY_RUN:
                order_id = f"DRY-{sym}-{int(time.time())}"
            else:
                resp = safe_place_order({
                    "category":    "linear",
                    "symbol":      sym,
                    "side":        "Sell",
                    "orderType":   "Limit",
                    "qty":         qty,
                    "price":       limit_price,
                    "timeInForce": "PostOnly",
                    "reduceOnly":  False,
                })
                if resp:
                    order_id = resp.get("result", {}).get("orderId")

            if order_id:
                with CAND_LOCK:
                    arr = load_json(CANDIDATE_FILE)
                    arr.append({
                        "symbol":    sym,
                        "side":      "Sell",
                        "qty":       float(qty),
                        "price":     float(limit_price),
                        "order_id":  order_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    save_json(CANDIDATE_FILE, arr)

                # TP/SL計算
                tp_price = round(limit_price * (1 - EFFECTIVE_TP_PCT), 6)
                sl_price = round(limit_price * (1 + EFFECTIVE_SL_PCT), 6)
                tp_pnl = (limit_price - tp_price) * qty
                sl_pnl = (limit_price - sl_price) * qty

                # カード通知
                notify_entry_candidate(
                    symbol=sym,
                    current_price=last_price,
                    limit_price=limit_price,
                    qty=qty,
                    wallet_balance=wallet_balance,
                    strategy_name=STRATEGY_NAME,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    tp_pnl=tp_pnl,
                    sl_pnl=sl_pnl,
                    current_positions=current_positions,
                    max_positions=MAX_POSITIONS,
                    strategy_hits=strategy_hits,
                    total_symbols=total_symbols,
                    is_trading_active=is_trading_active,
                    stop_reason=stop_reason,
                    actually_entered=True  # 🆕 実際にエントリーした
                )

                sent += 1
                remaining -= 1
                cand_syms.add(sym)

    # ── 心拍（発注なしの場合）
    if sent == 0:
        notify(f"ℹ️ 戦略ヒット: {strategy_hits}/{total_symbols}銘柄、発注なし")

    logging.info("done in %.2fs", time.time() - t0)

# ──────────────────────────────
# エントリーポイント（多重起動ガード）
# ──────────────────────────────
if __name__ == "__main__":
    lock = FileLock(PROCESS_LOCK_PATH)
    try:
        lock.acquire(timeout=0)
    except Timeout:
        logging.warning("duplicate run detected; skip")
        sys.exit(0)

    try:
        main_once()
    finally:
        try:
            lock.release()
        except Exception:
            pass