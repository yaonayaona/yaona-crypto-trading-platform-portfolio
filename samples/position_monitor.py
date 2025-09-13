#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
position_monitor.py  ―  ポジション成立時 INSERT／決済時 UPDATE 版
  • 再起動前の建玉を re-attach して追跡
  • WebSocket / 手動 / 強制清算を含め exit_reason を正確に記録
"""
from __future__ import annotations
import trading_stop_patch  # これだけでパッチ適用される
import order_patch   # 次に errCode 110017 を吸収
import os, json, time, logging, threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from filelock import FileLock
from pybit.unified_trading import HTTP

from state_manager import (
    save_state, delete_state, reattach_all,
    STATE_DIR as ENTRY_STATE_DIR,
)
from file_utils        import load_json, cancel_candidate
from order_utils       import place_tp_sl
from position_utils    import wait_until_cleared, emergency_close_all
from exit_price_utils  import get_closed_pnl
from pnl_calculator    import PnlCalculator
from trade_logger      import log_trade, update_exit
from notifier          import notify
from position_notifier import notify_tpsl, notify_news_card  # 追加：カード通知用
from stale_cleaner       import clean_stale            # ← 修正：monitors. を外す
from manual_fill_detector import detect_manual_positions  # ← 修正：monitors. を外す
from ws_listener       import QUEUE_PATH                      # キューのパス
import risk_control


# ────────────────────────────────────────
# 環境・定数
# ────────────────────────────────────────
load_dotenv()
API_KEY, API_SECRET = os.getenv("BYBIT_API_KEY"), os.getenv("BYBIT_API_SECRET")
client = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

BASE_DIR        = Path(__file__).parent
CANDIDATES_FILE = BASE_DIR / "entry_candidates.json"
CAND_LOCK       = FileLock(f"{CANDIDATES_FILE}.lock")
STOP_FLAG       = BASE_DIR / ".risk_stopped"
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "5"))
MAX_WORKERS     = int(os.getenv("MAX_MON_WORKERS", "5"))

logging.basicConfig(
    filename=BASE_DIR / "monitor.log",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

active_monitors: dict[str, bool] = {}
monitor_lock    = threading.Lock()
executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# ────────────────────────────────────────
# WebSocket キュー util
# ────────────────────────────────────────
Q_LOCK = FileLock(f"{QUEUE_PATH}.lock")
def pop_filled_queue() -> list[dict]:
    if not Path(QUEUE_PATH).exists():
        return []
    with Q_LOCK:
        lines = Path(QUEUE_PATH).read_text().splitlines()
        Path(QUEUE_PATH).write_text("")          # consume
    return [json.loads(l) for l in lines if l]

# ────────────────────────────────────────
# REST fallback: 指値約定確認
# ────────────────────────────────────────
def check_order_filled(order_id: str, symbol: str) -> bool:
    try:
        resp = client.get_order_history(
            category="linear", symbol=symbol, orderId=order_id
        )
        return any(
            o.get("orderStatus") == "Filled"
            or float(o.get("cumExecQty") or 0) > 0
            for o in resp.get("result", {}).get("list", [])
        )
    except Exception as e:
        logger.warning("check_order_filled error %s %s: %s", symbol, order_id, e)
        notify(f"⚠ check_order_filled error {symbol}: {e}")
        return False

# ────────────────────────────────────────
# Filled ハンドラ
# ────────────────────────────────────────
def handle_filled(cand: dict) -> None:
    """建玉成立〜決済完了までを 1 ポジション単位で処理"""
    # まずはペイロードを丸出しでログ → manual 判定のデバッグに便利
    logger.info("🔍 handle_filled payload: %s", cand)

    sym = cand["symbol"]
    try:
        entry_price = float(cand.get("price") or cand.get("entry_price"))
        oid         = cand.get("orderId") or cand.get("order_id")

        # ── エントリーステート保存（再起動用） ──
        state = {
            "symbol"      : sym,
            "side"        : cand["side"],
            "qty"         : float(cand["qty"]),
            "entry_price" : entry_price,
            "order_id"    : oid,
            "filled_time" : cand.get("filled_time")
                            or datetime.now(timezone.utc).isoformat(),
        }
        save_state(sym, state)

        # ── DB INSERT ──
        log_trade(
            order_id    = oid,
            symbol      = sym,
            side        = state["side"],
            qty         = state["qty"],
            entry_price = entry_price,
            entry_time  = datetime.fromisoformat(state["filled_time"]),
        )

        # ── TP/SL 発注 & entry_candidates から削除 ──
        place_tp_sl(state)
        cancel_candidate(sym, CANDIDATES_FILE)

        # ── 決済完了までブロック ──
        wait_until_cleared(sym, POLL_INTERVAL)

        # ── Closed-PnL 取得 ──
        exit_price, pnl_net, exit_dt, exit_oid = get_closed_pnl(sym, state)
        calc = PnlCalculator().calculate(state, exit_price)

        # ── exit_reason 判定 ──
        if cand.get("reason"):                       # manual_fill_detector
            reason = cand["reason"]

        elif cand.get("execType") == "BustTrade":    # 強制清算
            reason = "liquidation"

        elif cand.get("createType") == "CreateByUser" or oid.startswith("MANUAL-"):
            reason = "manual"

        elif cand.get("createType") == "CreateByApi":
            reason = "api"

        else:
            # SL or TP を判定
            if (state["side"] == "Sell" and exit_price > entry_price) or \
               (state["side"] == "Buy"  and exit_price < entry_price):
                reason = "sl"
            else:
                reason = "tp"

        # ── DB UPDATE ──
        update_exit(
            order_id      = oid,
            exit_price    = exit_price,
            exit_time     = exit_dt,
            pnl           = pnl_net,
            fee           = calc.fee,
            exit_order_id = exit_oid,
            exit_reason   = reason,
        )

        # ── リスク制御フック（記録 → 停止判定 → 非常停止） ──
        stop_triggered = False
        try:
            # 1) 今回損益を記録。停止へ遷移したら True
            stop_triggered = bool(risk_control.record_outcome(pnl_net))
            # 2) 独立の停止条件（日次DD等）
            stop_triggered = stop_triggered or bool(risk_control.check_daily_drawdown())
        except Exception as e:
            logger.warning("risk_control hook error: %s", e)

        if stop_triggered:
            # 二重実行防止（センチネル）
            already = False
            try:
                already = STOP_FLAG.exists()
            except Exception:
                pass

            if not already:
                try:
                    STOP_FLAG.write_text(datetime.now(timezone.utc).isoformat())
                except Exception:
                    pass

                notify("🛑 Risk control stop. Emergency closing all.")
                # 全キャンセル→全クローズ
                emergency_close_all(CANDIDATES_FILE, poll_interval=POLL_INTERVAL)

        # ── TPSLカード通知（停止の有無に関わらず必ず出す） ──
        entry_dt = datetime.fromisoformat(state["filled_time"]) if state.get("filled_time") else exit_dt
        hold_s   = int((exit_dt - entry_dt).total_seconds()) if entry_dt else 0
        notify_tpsl(sym, pnl_net, hold_s, reason or "不明", exit_dt=exit_dt)

    except Exception as e:
        logger.exception("handle_filled error for %s: %s", sym, e)
        notify(f"⚠ handle_filled error {sym}: {e}")

    finally:
        delete_state(sym)                    # state ファイル掃除
        with monitor_lock:
            active_monitors.pop(sym, None)

# ────────────────────────────────────────
# 起動時ニュースカード関数
# ────────────────────────────────────────
def _count_open_positions() -> int:
    try:
        resp = client.get_positions(category="linear", settleCoin="USDT")
        lst  = resp.get("result", {}).get("list", []) or []
        cnt  = 0
        for p in lst:
            try:
                if float(p.get("size") or 0) > 0 or float(p.get("positionValue") or 0) > 0:
                    cnt += 1
            except Exception:
                continue
        return cnt
    except Exception:
        return 0

def send_boot_news() -> None:
    # リスク状態
    st = {}
    try:
        st = risk_control.load_state()
    except Exception:
        st = {}

    consec_losses   = int(st.get("consec_losses", 0))
    max_losses      = int(os.getenv("MAX_CONSEC_LOSSES", "5"))
    emergency_count = int(st.get("emergency_trips", 0))
    manual_stop     = bool(st.get("manual_stop", False))
    emergency_stop  = bool(st.get("emergency_stop", False) or manual_stop)
    trading_active  = bool(risk_control.ok_to_trade())

    # ポジション
    open_cnt  = _count_open_positions()
    max_pos   = int(os.getenv("MAX_POSITIONS", os.getenv("MAX_POS", "3")))

    # 通知設定の自己診断
    notif_ok = bool(os.getenv("DISCORD_WEBHOOK_URL") or os.getenv("DISCORD_URL_positionnews"))
    notif_label = "正常" if notif_ok else "未設定"

    # タイトル/本文
    ok_state = trading_active and not emergency_stop
    title = "✅ Bot起動: 正常状態" if ok_state else "🟠 Bot起動: 注意"
    desc  = (
        "システムが起動しました\n"
        f"連続損失: {consec_losses}, 緊急停止: {emergency_count}, 停止: {str(not trading_active)}"
    )

    # カードのフィールド（シンプル版）
    fields = [
        {"name": "📕 連続損失", "value": f"{consec_losses}/{max_losses}回", "inline": True},
        {"name": "🧯 緊急停止", "value": "有効" if emergency_stop else "無効", "inline": True},
        {"name": "🚫 取引停止", "value": "有効" if (not trading_active) else "無効", "inline": True},

        {"name": "📊 ポジション数", "value": f"{open_cnt}/{max_pos}", "inline": True},
        {"name": "📣 通知設定", "value": notif_label, "inline": True},
    ]

    notify_news_card(title, desc, fields, ok=ok_state)

# ────────────────────────────────────────
# Monitor loop
# ────────────────────────────────────────
def run_monitor() -> None:
    # 1) WebSocket キュー
    for cand in pop_filled_queue():
        if cand["symbol"] not in active_monitors:
            executor.submit(handle_filled, cand)
            active_monitors[cand["symbol"]] = True

    # 2) 手動ポジション検出
    for cand in detect_manual_positions(CANDIDATES_FILE, ENTRY_STATE_DIR):
        if cand["symbol"] not in active_monitors:
            executor.submit(handle_filled, cand)
            active_monitors[cand["symbol"]] = True

    # 3) REST fallback（指値監視）
    with CAND_LOCK:
        for cand in load_json(CANDIDATES_FILE):
            sym, oid = cand.get("symbol"), cand.get("order_id")
            if sym and sym not in active_monitors and check_order_filled(oid, sym):
                executor.submit(handle_filled, cand)
                active_monitors[sym] = True

# ────────────────────────────────────────
# main
# ────────────────────────────────────────
def main() -> None:
    notify("🟢 position_monitor started")
    # ★ 追加：起動ニュースを送る
    try:
        send_boot_news()
    except Exception as e:
        logger.warning("send_boot_news failed: %s", e)

    # 再起動前の建玉を re-attach
    reattach_all(handle_filled, executor, active_monitors, logger)

    while True:
        clean_stale(CANDIDATES_FILE)
        run_monitor()
        time.sleep(POLL_INTERVAL)

# ────────────────────────────────────────
if __name__ == "__main__":
    logger.info("スクリプト開始")
    try:
        main()
    finally:
        logger.info("スクリプト終了")