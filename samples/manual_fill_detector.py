#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monitors/manual_fill_detector.py

手動または外部ツールで建てたポジションを検出し、
Bot が追跡していない銘柄を「手動ポジション候補」として返す。
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

from file_utils import load_json

# ─────────────────────────────────────────
# .env 読み込み & Bybit クライアント
# ─────────────────────────────────────────
load_dotenv()
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")

client = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

# ─────────────────────────────────────────
# メイン関数
# ─────────────────────────────────────────
def detect_manual_positions(
    candidates_file: Path,
    entry_state_dir: Path,
) -> List[Dict]:
    """
    Bybit 上の現在建玉（linear / USDT 建て）を調べ、
    entry_candidates.json と entry_states に登録されていない
    “手動ポジション” をリストで返す。

    Returns
    -------
    List[dict]
        [{
            "symbol"     : "BTCUSDT",
            "side"       : "Buy",
            "qty"        : 0.01,
            "entry_price": 65000.0,
            "order_id"   : "MANUAL-BTCUSDT",
            "timestamp"  : "2025-07-12T12:34:56.789Z",
            "reason"     : "manual"
        }, ...]
    """
    # 1) Bybit 現在建玉取得
    resp = client.get_positions(category="linear", settleCoin="USDT")
    positions = resp.get("result", {}).get("list", [])

    # 2) すでに追跡中のシンボルを収集
    tracked: set[str] = set()
    for c in load_json(candidates_file):
        tracked.add(c.get("symbol"))
    for f in entry_state_dir.glob("*.json"):
        tracked.add(f.stem)

    # 3) 手動ポジション抽出
    manual_fills: List[Dict] = []
    for p in positions:
        sym  = p.get("symbol")
        size = float(p.get("size") or 0)

        if size <= 0 or sym in tracked:
            continue  # 建玉なし / 追跡済みはスキップ

        side        = p.get("side")            # "Buy" / "Sell"
        entry_price = float(p.get("avgPrice") or 0)

        manual_fills.append({
            "symbol"     : sym,
            "side"       : side,
            "qty"        : size,
            "entry_price": entry_price,
            "order_id"   : f"MANUAL-{sym}",
            "timestamp"  : datetime.now(timezone.utc).isoformat(),
            "reason"     : "manual",            # ← exit_reason 用
        })

    return manual_fills
