# backtest_engine/strategy_parts.py  (timeframe-aware)
"""
Predicate library with optional `timeframe` filtering.
All functions accept `timeframe: str | None = None`.
If the incoming DataFrame has a column `tf`, the predicate will first
filter rows where `df["tf"] == timeframe`.  If `timeframe` is None, the
full DataFrame is used.  This lets one JSON strategy mix multiple
candlestick resolutions while keeping each predicate self‑contained.
"""
from __future__ import annotations
from typing import Any, Callable, Dict, Union
import operator as _op
import numpy as np
import pandas as pd

# ────────────────────────────────
# Helper : timeframe filter
# ────────────────────────────────

def _filter_tf(df: pd.DataFrame, tf: str | None) -> pd.DataFrame:
    if tf is None:
        return df
    if "tf" not in df.columns:
        raise ValueError("DataFrame lacks 'tf' column for timeframe filtering")
    return df[df["tf"] == tf]

# ────────────────────────────────
# Price helpers (EMA)
# ────────────────────────────────

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def ema_cross_down(df: pd.DataFrame, *, fast: int, slow: int,
                   timeframe: str | None = None) -> np.ndarray:
    sub = _filter_tf(df, timeframe)
    f = _ema(sub["close"], fast)
    s = _ema(sub["close"], slow)
    mask = (f.shift(1) > s.shift(1)) & (f <= s)
    return mask.reindex(df.index, fill_value=False).to_numpy()


def ema_fast_below_slow(df: pd.DataFrame, *, fast: int, slow: int,
                        timeframe: str | None = None) -> np.ndarray:
    sub = _filter_tf(df, timeframe)
    f = _ema(sub["close"], fast)
    s = _ema(sub["close"], slow)
    mask = f < s
    return mask.reindex(df.index, fill_value=False).to_numpy()

# ────────────────────────────────
# RSI range
# ────────────────────────────────

def rsi_range(df: pd.DataFrame, *, period: int, low: float, high: float,
              timeframe: str | None = None) -> np.ndarray:
    sub = _filter_tf(df, timeframe)
    diff = sub["close"].diff()
    gain = diff.clip(lower=0).rolling(period).mean()
    loss = (-diff.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    mask = (rsi >= low) & (rsi <= high)
    return mask.fillna(False).reindex(df.index, fill_value=False).to_numpy()

# ────────────────────────────────
# OI Z‑score
# ────────────────────────────────

def oi_zscore(df: pd.DataFrame, *, lookback: int, min_z: float, max_z: float,
              timeframe: str | None = None) -> np.ndarray:
    sub = _filter_tf(df, timeframe)
    oi = sub["oi"]
    mean = oi.rolling(lookback).mean()
    std = oi.rolling(lookback).std()
    z = (oi - mean) / std
    mask = (z >= min_z) & (z <= max_z)
    return mask.fillna(False).reindex(df.index, fill_value=False).to_numpy()

# ────────────────────────────────
# Correlation predicate
# ────────────────────────────────
from backtest_engine.indicators import rolling_corr

OP_MAP = {">": _op.gt, ">=": _op.ge, "<": _op.lt, "<=": _op.le}

def corr(df: pd.DataFrame, *, pair: str, lookback: int,
         op: str, threshold: float, timeframe: str | None = None) -> np.ndarray:
    sub = _filter_tf(df, timeframe)
    price = sub["close"]
    volume = sub["volume"]
    oi = sub["oi"]
    s1, s2 = {
        "price_volume": (price, volume),
        "volume_oi":    (volume, oi),
        "price_oi":     (price, oi),
    }[pair]
    r = rolling_corr(s1, s2, lookback)
    mask = OP_MAP[op](r, threshold)
    return mask.fillna(False).reindex(df.index, fill_value=False).to_numpy()

# ────────────────────────────────
# custom_col_true : external flag column
# ────────────────────────────────

def custom_col_true(df: pd.DataFrame, *, col: str) -> np.ndarray:
    if col not in df.columns:
        raise KeyError(f"column '{col}' not found for custom_col_true")
    return df[col].astype(bool).to_numpy()

# ────────────────────────────────
__all__ = [
    "ema_cross_down",
    "ema_fast_below_slow",
    "rsi_range",
    "oi_zscore",
    "corr",
    "custom_col_true",
]
