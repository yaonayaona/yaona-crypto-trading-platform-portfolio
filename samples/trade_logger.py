#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_logger.py ― Exit Reason 対応版
  • yaona_short_v1_tradehis に exit_reason TEXT 列がある前提
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional
import psycopg2
from Orderbook3 import DB_CONFIG

TABLE = "yaona_short_v1_tradehis"

# ---------------------------------------------------------------------------
# エントリー確定時 INSERT
# ---------------------------------------------------------------------------
def log_trade(
    *,
    order_id: str,
    symbol: str,
    side: str,
    qty: float,
    entry_price: float,
    entry_time: datetime,
    exit_price: Optional[float] = None,
    exit_time: Optional[datetime] = None,
    pnl: Optional[float] = None,
    fee: Optional[float] = None,
    exit_order_id: Optional[str] = None,
    exit_reason: Optional[str] = None,  # ★ 追加
) -> None:
    duration_seconds = (
        int((exit_time - entry_time).total_seconds()) if exit_time else None
    )
    net_pnl = (pnl - fee) if (pnl is not None and fee is not None) else None

    sql = f"""
        INSERT INTO {TABLE}
            (order_id, symbol, side, qty,
             entry_price, exit_price,
             entry_time, exit_time,
             pnl, fee, net_pnl, duration_seconds,
             exit_order_id, exit_reason)
        VALUES (%s,%s,%s,%s, %s,%s, %s,%s, %s,%s,%s,%s, %s,%s)
        ON CONFLICT (order_id) DO UPDATE SET
            symbol            = EXCLUDED.symbol,
            side              = EXCLUDED.side,
            qty               = EXCLUDED.qty,
            entry_price       = EXCLUDED.entry_price,
            exit_price        = COALESCE(EXCLUDED.exit_price , {TABLE}.exit_price),
            entry_time        = EXCLUDED.entry_time,
            exit_time         = COALESCE(EXCLUDED.exit_time  , {TABLE}.exit_time),
            pnl               = COALESCE(EXCLUDED.pnl        , {TABLE}.pnl),
            fee               = COALESCE(EXCLUDED.fee        , {TABLE}.fee),
            net_pnl           = COALESCE(EXCLUDED.net_pnl    , {TABLE}.net_pnl),
            duration_seconds  = COALESCE(EXCLUDED.duration_seconds , {TABLE}.duration_seconds),
            exit_order_id     = COALESCE(EXCLUDED.exit_order_id , {TABLE}.exit_order_id),
            exit_reason       = COALESCE(EXCLUDED.exit_reason    , {TABLE}.exit_reason)
    """

    vals = (
        order_id, symbol, side, qty,
        entry_price, exit_price,
        entry_time, exit_time,
        pnl, fee, net_pnl, duration_seconds,
        exit_order_id, exit_reason,
    )

    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(sql, vals)
    except Exception as e:
        print(f"[ERROR] log_trade failed: {e}")

# ---------------------------------------------------------------------------
# 決済後 UPDATE
# ---------------------------------------------------------------------------
def update_exit(
    order_id: str,
    *,
    exit_price: float,
    exit_time: datetime,
    pnl: float,
    fee: float,
    exit_order_id: str,
    exit_reason: str,              # ★ 追加
) -> None:
    """order_id をキーに決済情報を上書きし、exit_reason も保存"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT entry_time FROM {TABLE} WHERE order_id = %s", (order_id,)
            )
            row = cur.fetchone()

            if not row:
                # entry が無ければ INSERT
                log_trade(
                    order_id=order_id,
                    symbol="", side="", qty=0.0,
                    entry_price=0.0, entry_time=exit_time,
                    exit_price=exit_price, exit_time=exit_time,
                    pnl=pnl, fee=fee,
                    exit_order_id=exit_order_id,
                    exit_reason=exit_reason,
                )
                return

            entry_time: datetime = row[0]
            duration_seconds = int((exit_time - entry_time).total_seconds())
            net_pnl = pnl - fee

            cur.execute(
                f"""
                UPDATE {TABLE}
                   SET exit_price       = %s,
                       exit_time        = %s,
                       pnl              = %s,
                       fee              = %s,
                       net_pnl          = %s,
                       duration_seconds = %s,
                       exit_order_id    = %s,
                       exit_reason      = %s
                 WHERE order_id = %s
                """,
                (
                    exit_price, exit_time,
                    pnl, fee, net_pnl, duration_seconds,
                    exit_order_id, exit_reason, order_id,
                ),
            )
    except Exception as e:
        print(f"[ERROR] update_exit failed: {e}")
