from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    # Codex JSONL uses ISO strings like "2025-11-05T02:19:10.108Z".
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def json_dumps_pretty(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, indent=2, sort_keys=True)


@dataclass(frozen=True)
class ExtractedDoc:
    # Stable primary key (derived from file/line/segment).
    doc_id: str

    session_id: str | None
    file_path: str
    line_no: int
    event_ts: datetime | None
    event_type: str | None
    inner_type: str | None
    role: str | None
    kind: str
    text: str

