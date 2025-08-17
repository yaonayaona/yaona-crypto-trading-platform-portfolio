# backtest_engine/strategy_builder.py
"""
strategy_builder.py ― v0.4.0 (side‑aware)
────────────────────────────────────────────────────────
* JSON 定義から戦略関数 (df → DataFrame[entry,tp,sl,side]) を生成
* meta キー (tp/sl/side/name/portfolio_config/exit_rules) は
  JSON‑Schema 検証から除外
"""

from __future__ import annotations

from typing import Any, Callable, Dict

import numpy as np
import pandas as pd

from backtest_engine import strategy_parts as sp
from backtest_engine.validate import validate_cfg_schema

# ─────────────────────────────────────────────
# Predicate registry
# ─────────────────────────────────────────────
_PREDICATE_MAP: Dict[str, Callable[..., np.ndarray]] = {
    name: fn
    for name, fn in vars(sp).items()
    if callable(fn) and not name.startswith("_")
}

_ALIASES: Dict[str, str] = {
    "ema_dc": "ema_cross_down",
    "ema_fast_lt_slow": "ema_fast_below_slow",
}
_PREDICATE_MAP.update({alias: _PREDICATE_MAP[target] for alias, target in _ALIASES.items()})

# ─────────────────────────────────────────────
#  再帰的に cfg を評価 → bool ndarray
# ─────────────────────────────────────────────

def _eval_cfg(node: Any, df: pd.DataFrame) -> np.ndarray:  # noqa: C901  (complexity)
    """Recursively evaluate **predicate logic** subtree and return a boolean mask."""
    if isinstance(node, bool):
        return np.full(len(df), node, dtype=bool)

    # ---------- dict ---------------------------------------------------
    if isinstance(node, dict):
        # --- logical operators ----------------------------------------
        if "and" in node:
            return np.logical_and.reduce([_eval_cfg(c, df) for c in node["and"]])
        if "or" in node:
            return np.logical_or.reduce([_eval_cfg(c, df) for c in node["or"]])
        if "not" in node:
            return np.logical_not(_eval_cfg(node["not"], df))

        # --- predicate leaf -------------------------------------------
        if len(node) != 1:
            raise ValueError(f"Ambiguous predicate node: {node}")
        name, params = next(iter(node.items()))
        if name not in _PREDICATE_MAP:
            raise KeyError(f"Unknown predicate '{name}'")
        return _PREDICATE_MAP[name](df, **(params or {}))

    # ---------- list ---------------------------------------------------
    if isinstance(node, list):
        raise ValueError("Top‑level list is not supported")

    raise ValueError(f"Invalid cfg node: {node}")


# ─────────────────────────────────────────────
#  静的検証 : 未知 predicate を早期検出
# ─────────────────────────────────────────────

def _validate_pred_names(node: Any) -> None:
    """Walk cfg tree and assert that every predicate exists in registry."""
    if isinstance(node, dict):
        if {"and", "or", "not"} & node.keys():  # logical operator case
            for key in ("and", "or"):
                if key in node:
                    for sub in node[key]:
                        _validate_pred_names(sub)
            if "not" in node:
                _validate_pred_names(node["not"])
        else:  # predicate leaf
            if len(node) != 1:
                raise ValueError(f"Ambiguous predicate node: {node}")
            key = next(iter(node))
            if key not in _PREDICATE_MAP:
                raise KeyError(f"Unknown predicate '{key}'")
    elif isinstance(node, list):
        for sub in node:
            _validate_pred_names(sub)


# ─────────────────────────────────────────────
#  Public API
# ─────────────────────────────────────────────

def build_strategy(cfg: Dict[str, Any]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Compile *cfg* into a strategy function.

    The returned function accepts a *pandas* DataFrame and produces another
    DataFrame with (at minimum) the following columns::

        entry : bool   ― エントリーシグナル
        price : float  ― close 値をコピー
        tp    : float  ― 利確幅 (割合)
        sl    : float  ― 損切幅 (割合)
        side  : str    ― "long" or "short"

    Any existing ``symbol`` / ``time`` columns are propagated for convenience.
    """
    if not isinstance(cfg, dict):
        raise TypeError("cfg must be dict")

    # ----- 1) メタキーを除いたコピーで JSON‑Schema 検証 ----------------
    META_KEYS = {"tp", "sl", "side", "name", "portfolio_config", "exit_rules"}
    logic_only = {k: v for k, v in cfg.items() if k not in META_KEYS}
    if not logic_only:
        raise ValueError("cfg must contain predicate logic")

    validate_cfg_schema(logic_only)

    # ----- 2) tp / sl / side を取得（デフォルト long） ---------------
    tp_val = float(cfg.get("tp", 0.02))
    sl_val = float(cfg.get("sl", 0.03))

    side_val = str(cfg.get("side", "long")).lower()
    if side_val not in {"long", "short"}:
        raise ValueError("side must be either 'long' or 'short'")

    # ----- 3) 未知 predicate 名を静的チェック --------------------------
    _validate_pred_names(logic_only)

    # ----- 4) ランタイム関数を構築 ------------------------------------
    def _strategy(df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 (simple docstring)
        """Evaluate strategy on *df* and return signals."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be pandas DataFrame")

        entry_mask = _eval_cfg(logic_only, df)

        out = pd.DataFrame(index=df.index)
        if "symbol" in df.columns:
            out["symbol"] = df["symbol"]
        if "time" in df.columns:
            out["time"] = df["time"]
        out["price"] = df["close"].to_numpy()
        out["entry"] = entry_mask.astype(bool)
        out["tp"] = tp_val
        out["sl"] = sl_val
        out["side"] = side_val
        return out

    _strategy.__name__ = cfg.get("name", "generated_strategy")
    _strategy.__doc__ = f"Strategy generated from cfg: {cfg}"
    return _strategy
