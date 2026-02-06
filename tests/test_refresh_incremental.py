from __future__ import annotations

import json
from pathlib import Path

import duckdb

from prompt_search.ingest import refresh
from prompt_search.paths import db_path
from prompt_search.search import search as search_impl
from prompt_search.db import connect, ensure_schema, try_enable_fts, is_fts_available


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=True) + "\n")


def _append_jsonl(path: Path, events: list[dict]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=True) + "\n")


def test_refresh_incremental_and_search(tmp_path: Path) -> None:
    sessions_dir = tmp_path / "sessions"
    data_dir = tmp_path / "data"

    f1 = sessions_dir / "2025/11/04/test-1.jsonl"

    base_events = [
        {
            "timestamp": "2025-11-05T02:19:10.108Z",
            "type": "session_meta",
            "payload": {"id": "sess-1", "timestamp": "2025-11-05T02:19:10.079Z", "cwd": "/tmp"},
        },
        {
            "timestamp": "2025-11-05T02:19:11.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "hello world"}],
            },
        },
    ]
    _write_jsonl(f1, base_events)

    s1 = refresh(sessions_dir=sessions_dir, data_dir=data_dir, full=False, reindex=False)
    assert s1.files_scanned == 1
    assert s1.docs_inserted >= 1

    # Append another user message and ensure only new docs are inserted.
    _append_jsonl(
        f1,
        [
            {
                "timestamp": "2025-11-05T02:19:12.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "duckdb search test"}],
                },
            }
        ],
    )

    s2 = refresh(sessions_dir=sessions_dir, data_dir=data_dir, full=False, reindex=False)
    assert s2.files_scanned == 1
    assert s2.docs_inserted >= 1

    con = connect(db_path(data_dir))
    ensure_schema(con)

    results, mode = search_impl(
        con, query="duckdb", limit=10, include_assistant=False, include_internal=False
    )
    # Search should find the appended message at least via substring fallback.
    assert any("duckdb" in r.snippet.lower() for r in results)
    assert mode in ("fts", "substring")


def test_fts_optional(tmp_path: Path) -> None:
    # This doesn't require fts; it simply ensures our fts detection doesn't crash.
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    con = connect(db_path(data_dir))
    ensure_schema(con)
    ok = try_enable_fts(con)
    assert is_fts_available(con) == ok

