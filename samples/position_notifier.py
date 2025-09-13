#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
position_notifier.py  ―  通知統合モジュール（カード対応版）

新機能:
- notify_entry_candidate(): エントリー候補のカード通知
"""
from __future__ import annotations

import os
import time
import logging
from typing import Optional
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger("position_notifier")

# ──────────────────────────────────────────────────────────────
# Webhook URL 取得
# ──────────────────────────────────────────────────────────────
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")           # 汎用
DISCORD_POS     = os.getenv("DISCORD_URL_position")          # 取引イベント（従来）
DISCORD_TRASH   = os.getenv("DISCORD_URL_positiontrash")     # 旧: ステイル
DISCORD_STALE   = os.getenv("DISCORD_URL_stale")             # 新: ステイル
DISCORD_POSNEWS = os.getenv("DISCORD_URL_positionnews")      # ★ 新: ニュース/通知集約
DISCORD_TPSL    = os.getenv("DISCORD_URL_tpsl") or DISCORD_POS or DISCORD_WEBHOOK

DEDUP_SEC   = float(os.getenv("NOTIFY_DEDUP_SEC", "1.0"))
RETRY_COUNT = int(os.getenv("NOTIFY_RETRY_COUNT", "3"))
TIMEOUT_SEC = int(os.getenv("NOTIFY_TIMEOUT_SEC", "10"))

# ──────────────────────────────────────────────────────────────
# 色定義
# ──────────────────────────────────────────────────────────────
COLORS = {
    'profit': 0x00ff00, 'loss': 0xff0000,
    'profit_tp': 0x32cd32, 'loss_sl': 0xff6347,
    'manual_profit': 0x90ee90, 'manual_loss': 0xffa07a,
    'forced_profit': 0x228b22, 'forced_loss': 0x8b0000,
    'emergency': 0xdc143c, 'entry': 0xffffff,
    'stale': 0xff8c00, 'normal': 0x0099ff,
    'warning': 0xffff00, 'info': 0x808080,
    'candidate': 0x1e90ff,  # 候補通知用
}

_last_content: str = ""
_last_ts: float = 0.0

def _send_with_retry(url: str | None, payload: dict, descr: str) -> bool:
    if not url:
        logger.warning("URL未設定のため送信スキップ: %s", descr)
        return False
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.post(url, json=payload, timeout=TIMEOUT_SEC)
            r.raise_for_status()
            logger.info("✅ Discord送信成功 (%s)", descr)
            return True
        except Exception as e:
            logger.warning("送信失敗[%d/%d] %s: %s", attempt, RETRY_COUNT, descr, e)
            if attempt < RETRY_COUNT:
                time.sleep(1)
    logger.error("❌ Discord送信全失敗: %s", descr)
    return False

def _isoformat_utc(dt: datetime | None) -> str:
    if not dt:
        return datetime.now(timezone.utc).isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _send_embed(url: str | None, title: str, desc: str, color: int,
                fields: Optional[list] = None, timestamp: datetime | None = None) -> None:
    global _last_content, _last_ts
    now = time.time()
    content_for_dedup = f"{title}:{desc}"
    if content_for_dedup == _last_content and now - _last_ts < DEDUP_SEC:
        return
    _last_content, _last_ts = content_for_dedup, now

    embed = {
        "title": title,
        "description": desc,
        "color": color,
        "timestamp": _isoformat_utc(timestamp),
    }
    if fields:
        embed["fields"] = fields

    _send_with_retry(url, {"embeds": [embed]}, title)

def _fmt_hms(sec: int) -> str:
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _get_jst_time() -> str:
    """現在時刻をJST形式で取得"""
    jst = timezone(timedelta(hours=9))
    return datetime.now(timezone.utc).astimezone(jst).strftime("%H:%M:%S")

# ──────────────────────────────────────────────────────────────
# 🆕 新機能：エントリー候補カード通知
# ──────────────────────────────────────────────────────────────
def notify_entry_candidate(
    symbol: str,
    current_price: float,
    limit_price: float,
    qty: float,
    wallet_balance: float,
    strategy_name: str,
    tp_price: float,
    sl_price: float,
    tp_pnl: float,
    sl_pnl: float,
    current_positions: int,
    max_positions: int,
    strategy_hits: int,
    total_symbols: int,
    is_trading_active: bool,
    stop_reason: str = "",
    actually_entered: bool = False
) -> None:
    """エントリー候補のカード通知を送信"""
    
    # 計算
    entry_offset_pct = ((limit_price - current_price) / current_price) * 100
    usdt_amount = qty * limit_price
    wallet_pct = (usdt_amount / wallet_balance) * 100 if wallet_balance > 0 else 0
    tp_pct = ((tp_price - limit_price) / limit_price) * 100
    sl_pct = ((sl_price - limit_price) / limit_price) * 100
    
    # 状態判定
    if is_trading_active:
        if current_positions >= max_positions:
            status_icon = "🟡"
            status_text = "ポジ上限停止中"
        else:
            status_icon = "🟢"
            status_text = "稼働中"
    else:
        status_icon = "🔴"
        status_text = stop_reason or "停止中"
    
    # 残り枠
    remaining_slots = max(0, max_positions - current_positions)
    
    # カード色とタイトル判定
    can_enter = is_trading_active and remaining_slots > 0
    if actually_entered:
        # 実際にエントリーした場合
        card_color = COLORS['candidate']  # 青
        title = "🎯 ショートエントリー候補"
        show_details = True
    elif can_enter:
        # エントリー可能だが何らかの理由で見送った場合
        card_color = COLORS['candidate']  # 青
        title = "🎯 ショートエントリー候補"
        show_details = True
    else:
        # エントリー停止中
        show_details = False
        if current_positions >= max_positions:
            card_color = COLORS['warning']  # 黄色
            title = "⚠️ エントリー停止中（上限到達）"
        else:
            card_color = COLORS['loss']  # 赤色（リスク停止）
            title = "⚠️ エントリー停止中"
    
    desc = "━━━━━━━━━━━━━━━━━━"
    
    # フィールド構築
    fields = []
    
    # 停止中は銘柄詳細とリスク設定を非表示
    if show_details:
        fields.extend([
            {
                "name": "💰 " + symbol,
                "value": f"└ 投入額: {wallet_pct:.1f}%",
                "inline": False
            },
            {
                "name": "📊 戦略",
                "value": f"└ {strategy_name}",
                "inline": False
            },
            {
                "name": "🛡️ リスク設定",
                "value": (
                    f"├ 利確: {tp_pct:+.1f}%\n"
                    f"└ 損切: {sl_pct:+.1f}%"
                ),
                "inline": False
            }
        ])
    else:
        # 停止中は戦略のみ表示
        fields.append({
            "name": "📊 戦略",
            "value": f"└ {strategy_name}",
            "inline": False
        })
    
    # ポートフォリオ情報（常に表示）
    fields.append({
        "name": "📈 ポートフォリオ",
        "value": (
            f"├ 現在ポジ: {current_positions}/{max_positions} "
            f"({'残り' + str(remaining_slots) + '枠' if remaining_slots > 0 else '上限到達'})\n"
            f"├ 戦略ヒット: {strategy_hits:,}/{total_symbols:,}銘柄\n"
            f"└ 取引状態: {status_icon} {status_text}"
        ),
        "inline": False
    })
    
    # 実行情報（常に表示）
    fields.append({
        "name": "🕐 実行情報",
        "value": f"JST {_get_jst_time()} | PID:{os.getpid()}",
        "inline": False
    })
    
    # 通知先URL（positionnews優先）
    url = DISCORD_POSNEWS or DISCORD_POS or DISCORD_WEBHOOK
    
    _send_embed(url, title, desc, card_color, fields)

# ──────────────────────────────────────────────────────────────
# 既存の通知関数（そのまま）
# ──────────────────────────────────────────────────────────────
def notify_stale(symbol: str, order_id: str, age_minutes: int) -> None:
    """ステイル注文キャンセルの通知。positionnews を最優先に送る。"""
    url = DISCORD_POSNEWS or DISCORD_STALE or DISCORD_TRASH or DISCORD_WEBHOOK
    title = "🗑️ 期限切れ注文キャンセル"
    desc  = f"**{symbol}** の指値注文がタイムアウトしました"
    fields = [
        {"name": "OrderID", "value": (order_id[:12] + "…") if len(order_id) > 12 else order_id, "inline": True},
        {"name": "経過時間", "value": f"{age_minutes}分", "inline": True},
    ]
    _send_embed(url, title, desc, COLORS['stale'], fields)

def notify_entry(symbol: str, side: str, qty: float, price: float) -> None:
    title = f"🟢 エントリー: {symbol}"
    desc  = f"{side} {qty} @ {price}"
    _send_embed(DISCORD_POS, title, desc, COLORS['entry'])

def notify_exit(symbol: str, pnl: float, hold_s: int,
                reason: str = 'tp', add_info: str | None = None) -> None:
    col = COLORS['profit_tp'] if pnl >= 0 else COLORS['loss_sl']
    title = f"🔻 決済: {symbol}"
    desc  = f"PnL: {pnl:.4f}  保有: {_fmt_hms(hold_s)}  理由: {reason}"
    fields = ([{"name": "詳細", "value": add_info, "inline": False}] if add_info else None)
    _send_embed(DISCORD_POS, title, desc, col, fields)

def notify_tpsl(symbol: str, pnl: float, hold_s: int, reason: str,
                exit_dt: datetime | None = None) -> None:
    is_profit = (pnl >= 0)
    title = "✅ 利益確定！" if is_profit else "⚠️ 損切り実行"
    desc  = f"**{symbol}** で{'利益を獲得しました' if is_profit else '損失が発生しました'}"
    color = COLORS['profit_tp'] if is_profit else COLORS['loss_sl']
    fields = [
        {"name": "損益",     "value": f"{pnl:+.3f} USDT",    "inline": True},
        {"name": "保有時間", "value": _fmt_hms(int(hold_s)), "inline": True},
        {"name": "決済理由", "value": (reason or '不明'),     "inline": True},
    ]
    _send_embed(DISCORD_TPSL, title, desc, color, fields, timestamp=exit_dt)

def notify_warning(msg: str) -> None:
    _send_embed(DISCORD_WEBHOOK, "⚠️ 警告", msg, COLORS['warning'])

def notify_info(msg: str) -> None:
    _send_embed(DISCORD_WEBHOOK, "ℹ️ 情報", msg, COLORS['info'])

def notify_news_card(title: str, desc: str, fields: list, ok: bool = True) -> None:
    """
    運用ニュース（起動・状態報告など）のカード通知。
    送信先は DISCORD_WEBHOOK_URL を最優先。
    カードの色は青色固定。
    """
    url   = DISCORD_WEBHOOK or DISCORD_POSNEWS
    color = 0x1e90ff  # 青色固定
    _send_embed(url, title, desc, color, fields)