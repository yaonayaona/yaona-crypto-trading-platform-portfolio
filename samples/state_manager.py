#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
state_manager.py

`entry_states/` ディレクトリに置かれた JSON ファイルを一元管理するユーティリティ。
  • save_state(symbol, data)     : 上書き保存
  • load_state(symbol)           : dict で取得（無ければ None）
  • delete_state(symbol)         : ファイル削除
  • list_symbols()               : 監視中シンボル一覧
  • reattach_all(handler, pool, active, logger)
        起動直後に呼ぶことで “再起動前に残った state” を自動でハンドラへ再投入
"""

from __future__ import annotations
import json, logging
from pathlib import Path
from typing import Dict, List, Callable, Any

# ─────────────────────────────────────────
#  設定
# ─────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
STATE_DIR   = BASE_DIR / "entry_states"
STATE_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  低レベル I/O
# ─────────────────────────────────────────
def _path(symbol: str) -> Path:
    return STATE_DIR / f"{symbol}.json"

def save_state(symbol: str, data: Dict[str, Any]) -> None:
    try:
        _path(symbol).write_text(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        logger.warning("save_state(%s) failed: %s", symbol, e)

def load_state(symbol: str) -> Dict[str, Any] | None:
    p = _path(symbol)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        logger.warning("load_state(%s) failed: %s", symbol, e)
        return None

def delete_state(symbol: str) -> None:
    try:
        _path(symbol).unlink(missing_ok=True)
    except Exception as e:
        logger.warning("delete_state(%s) failed: %s", symbol, e)

def list_symbols() -> List[str]:
    return [f.stem for f in STATE_DIR.glob("*.json")]

# ─────────────────────────────────────────
#  起動直後の再アタッチ用ヘルパ
# ─────────────────────────────────────────
def reattach_all(
    handler: Callable[[Dict[str, Any]], None],
    pool,                          # ThreadPoolExecutor など submit() を持つオブジェクト
    active_monitors: Dict[str, bool],
    log: logging.Logger | None = None,
) -> None:
    """
    entry_states/*.json を読み込み、まだ追跡していないシンボルを
    ハンドラに渡して “監視を復活” させる。

    Parameters
    ----------
    handler : Callable
        handle_filled など、state dict → None の関数
    pool :
        submit(func, arg) メソッドを持つ並列実行プール
    active_monitors : dict
        position_monitor 側で使っている {symbol: True} マップ
    log : logging.Logger | None
        ロガー（指定が無ければモジュールロガー）
    """
    _log = log or logger
    for sym in list_symbols():
        if sym in active_monitors:
            continue
        st = load_state(sym)
        if not st:
            continue
        pool.submit(handler, st)
        active_monitors[sym] = True
        _log.info("Reattached state for %s", sym)
