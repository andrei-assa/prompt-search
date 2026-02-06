from __future__ import annotations

import os
from pathlib import Path


def default_sessions_dir() -> Path:
    env = os.environ.get("PROMPT_SEARCH_SESSIONS_DIR")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".codex" / "sessions"


def default_data_dir() -> Path:
    env = os.environ.get("PROMPT_SEARCH_DATA_DIR")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".prompt-search"


def db_path(data_dir: Path) -> Path:
    return data_dir / "db.duckdb"

