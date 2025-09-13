# file_utils.py

import json
from pathlib import Path
import logging

# ログ取得
logger = logging.getLogger(__name__)

# JSONロード

def load_json(path: Path):
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except Exception as e:
        logger.warning(f"Failed to load JSON from {path}: {e}")
        return []

# JSON保存

def save_json(path: Path, data):
    try:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        logger.warning(f"Failed to save JSON to {path}: {e}")

# entry_candidates.jsonから特定symbolをキャンセル（削除）

def cancel_candidate(symbol: str, candidates_path: Path):
    arr = load_json(candidates_path)
    arr = [c for c in arr if c.get("symbol") != symbol]
    save_json(candidates_path, arr)
    logger.info(f"Cancelled candidate: {symbol}")
