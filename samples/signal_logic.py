# signal_logic.py
# -*- coding: utf-8 -*-
"""
yaona_short_v1 用 ― 緩め のトレンド順張り＋短期デッドクロス ショートシグナル

■ ロジック概要（2025-05-09 暫定）
1. 長期トレンド判定 (4H)
   - EMA7, EMA14, EMA28 の傾き（前足との差）がすべて負（下降継続）
2. 短期デッドクロス検知 (5M)
   - 直近足で EMA7 が EMA14 を下抜け (クロスダウン)
   - かつ EMA7 < EMA28
3. 勢い (RSI)
   - RSI6 が 45〜75 の範囲
4. 出来高フィルター (ゆるめ)
   - volume が vol_ma3 × 0.3〜3.0 倍

満たしたときに True を返します。
"""

import pandas as pd

# ---- パラメータ ----
MIN_RSI, MAX_RSI = 45, 75
VOL_MIN_MULT, VOL_MAX_MULT = 0.3, 3.0

# ---- メイン関数 ----

def short_signal(df4h: pd.DataFrame, df5m: pd.DataFrame) -> bool:
    """デッドクロス戻り売りシグナル: 条件を満たせば True"""
    if len(df4h) < 2 or len(df5m) < 2:
        return False

    # 4H 下向きトレンド判定
    cur4h, prev4h = df4h.iloc[-1], df4h.iloc[-2]
    slope7  = cur4h["EMA7"]  - prev4h["EMA7"]
    slope14 = cur4h["EMA14"] - prev4h["EMA14"]
    slope28 = cur4h["EMA28"] - prev4h["EMA28"]
    trend_bear = (slope7 < 0 and slope14 < 0 and slope28 < 0)

    # 5M デッドクロス
    cur5m, prev5m = df5m.iloc[-1], df5m.iloc[-2]
    cross_down = (
        prev5m["EMA7"] > prev5m["EMA14"] and cur5m["EMA7"] <= cur5m["EMA14"]
    )
    below_long = cur5m["EMA7"] < cur5m["EMA28"]

    # RSI フィルター
    rsi_ok = MIN_RSI <= cur5m["RSI6"] <= MAX_RSI

    # 出来高フィルター
    vol_ma3 = cur5m.get("vol_ma3", 0) or 0
    vol_ok = (VOL_MIN_MULT * vol_ma3 <= cur5m["volume"] <= VOL_MAX_MULT * vol_ma3)

    return trend_bear and cross_down and below_long and rsi_ok and vol_ok


