import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from settings import settings

def _ensure_dir():
    os.makedirs(settings.DATA_DIR, exist_ok=True)

def _path(name: str) -> str:
    _ensure_dir()
    return os.path.join(settings.DATA_DIR, name)

def write_json(name: str, obj: Any) -> None:
    with open(_path(name), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def read_json(name: str) -> Optional[Any]:
    p = _path(name)
    if not os.path.exists(p):
        return None
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def timestamp() -> str:
    return datetime.utcnow().isoformat() + "Z"

# high-level helpers
def save_seasons(seasons: List[Dict]) -> None:
    write_json("seasons.json", {"updated_at": timestamp(), "seasons": seasons})

def load_seasons() -> Dict[str, Any]:
    return read_json("seasons.json") or {"updated_at": None, "seasons": []}

def save_episodes(episodes_by_season: Dict[str, List[Dict]]) -> None:
    write_json("episodes.json", {"updated_at": timestamp(), "episodes_by_season": episodes_by_season})

def load_episodes() -> Dict[str, Any]:
    return read_json("episodes.json") or {"updated_at": None, "episodes_by_season": {}}
