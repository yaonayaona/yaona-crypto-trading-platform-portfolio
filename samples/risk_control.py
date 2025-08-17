#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
risk_control.py

連敗制御と日次ドローダウン制御を提供するモジュール。

環境変数:
  RISK_STATE_FILE      : ステートファイルパス (default: risk_state.json)
  MAX_CONSEC_LOSSES    : 連敗で停止する回数 (default: 5)
  MAX_DRAWDOWN         : 日次ドローダウン閾値 (0.02 = 2%)
  BYBIT_API_KEY        : Bybit API キー
  BYBIT_API_SECRET     : Bybit API シークレット
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from requests.exceptions import ConnectionError

load_dotenv()

# --- 設定読み込み ---
STATE_FILE       = os.getenv("RISK_STATE_FILE", "risk_state.json")
MAX_LOSSES       = int(os.getenv("MAX_CONSEC_LOSSES", "5"))
MAX_DRAWDOWN     = float(os.getenv("MAX_DRAWDOWN", "0.02"))

BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
client           = HTTP(api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET, testnet=False)

# 微小誤差吸収用のしきい値
EPSILON = 1e-8

# キャッシュおよびリトライ用のモジュール変数
_last_balance = None
_last_ts      = 0.0


def load_state() -> dict:
    """state ファイルをロード。キーがなければ初期化付きで返す。"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            s = json.load(f)
        # 新フィールドがない場合は追加
        if "start_of_day_wallet" not in s:
            s["start_of_day_wallet"] = None
        if "start_of_day_date" not in s:
            s["start_of_day_date"] = None
        return s
    # 初期状態
    return {
        "consec_losses": 0,
        "stopped": False,
        "start_of_day_wallet": None,
        "start_of_day_date": None
    }


def save_state(s: dict):
    """state ファイルに書き込み。"""
    with open(STATE_FILE, "w") as f:
        json.dump(s, f, indent=2)


def ok_to_trade() -> bool:
    """取引許可判定。stopped フラグに基づく。"""
    s = load_state()
    return not s.get("stopped", False)


def record_outcome(pnl: float) -> bool:
    """
    P/L を受け取って連敗制御を更新。
    - pnl < -EPSILON の場合は連敗カウントを増加、閾値到達で停止フラグを立てる。
    - pnl > EPSILON の場合は連敗カウントをリセットし、停止フラグをクリアする。
    - |pnl| <= EPSILON のときは連敗カウントを変えずにそのまま。
    戻り値: 現在の停止状態
    """
    s = load_state()
    if pnl < -EPSILON:
        s["consec_losses"] = s.get("consec_losses", 0) + 1
        if s["consec_losses"] >= MAX_LOSSES:
            s["stopped"] = True
    elif pnl > EPSILON:
        s["consec_losses"] = 0
        s["stopped"] = False
    # pnl がほぼゼロのときはノーカウント
    save_state(s)
    return s["stopped"]


def get_wallet_balance() -> float:
    """
    Bybit 統合アカウントの USDT 残高を取得（キャッシュ・リトライ付き）。
    accountType="UNIFIED" の結果から totalWalletBalance を返す。
    """
    global _last_balance, _last_ts

    # キャッシュが有効ならそのまま返す（60秒以内）
    if _last_balance is not None and (time.time() - _last_ts) < 60:
        return _last_balance

    delay = 0.5
    for _ in range(3):
        try:
            resp = client.get_wallet_balance(accountType="UNIFIED")
            balances = resp.get("result", {}).get("list", [])
            # USDT 残高を探索
            bal = 0.0
            for entry in balances:
                if entry.get("coin") == "USDT":
                    bal = float(entry.get("totalWalletBalance", 0))
                    break
            else:
                # USDT がなければ最初のエントリをフォールバック
                if balances:
                    bal = float(balances[0].get("totalWalletBalance", 0))
            # キャッシュ更新
            _last_balance = bal
            _last_ts      = time.time()
            return bal

        except ConnectionError as e:
            logging.warning(f"wallet_balance retry in {delay:.1f}s: {e}")
            time.sleep(delay)
            delay *= 2

    # 3回失敗したら前回値を返す
    logging.error("wallet_balance failed 3 times, using last known value")
    return _last_balance if _last_balance is not None else 0.0


def check_daily_drawdown() -> bool:
    """
    日次ドローダウンをチェック。
    - 新しい日になったら start_of_day_wallet を更新してリセット。
    - それ以外は (current - start) / start <= -MAX_DRAWDOWN で停止フラグを立てる。
    戻り値: 停止状態に遷移したかどうか
    """
    s = load_state()
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()

    # 日付が変わっていれば開始時残高をリセット
    if s.get("start_of_day_date") != today:
        start_balance = get_wallet_balance()
        s["start_of_day_wallet"] = start_balance
        s["start_of_day_date"]   = today
        save_state(s)
        return False

    # ドローダウン計算
    start_balance   = s.get("start_of_day_wallet") or get_wallet_balance()
    current_balance = get_wallet_balance()
    drawdown = (current_balance - start_balance) / start_balance if start_balance else 0.0

    if drawdown <= -MAX_DRAWDOWN:
        s["stopped"] = True
        save_state(s)
        return True

    return False
