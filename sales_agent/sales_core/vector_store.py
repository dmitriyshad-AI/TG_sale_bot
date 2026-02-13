from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


def read_vector_store_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            loaded = json.load(fh)
    except (json.JSONDecodeError, OSError, ValueError, TypeError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def write_vector_store_meta(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_vector_store_id(path: Path) -> Optional[str]:
    meta = read_vector_store_meta(path)
    vector_store_id = meta.get("vector_store_id")
    if isinstance(vector_store_id, str) and vector_store_id.strip():
        return vector_store_id.strip()
    return None
