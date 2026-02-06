from __future__ import annotations

import duckdb

from prompt_search.search import extract_context_lines, search as search_impl
from prompt_search.db import ensure_schema, set_setting


def _setup(con: duckdb.DuckDBPyConnection) -> None:
    ensure_schema(con)
    # Mark FTS unavailable to force substring behavior for deterministic tests.
    set_setting(con, "fts_available", "0")
    con.execute("DELETE FROM docs")
    con.execute("DELETE FROM sessions")


def test_extract_context_lines_basic() -> None:
    txt = "aaa\nbbb match here\nccc\nddd\n"
    out = extract_context_lines(txt, "match", 1)
    assert out == "aaa\nbbb match here\nccc"


def test_substring_sort_relevance_uses_match_pos() -> None:
    con = duckdb.connect(":memory:")
    _setup(con)
    con.execute(
        """
        INSERT INTO docs(doc_id, session_id, file_path, line_no, event_ts, event_type, inner_type, role, kind, text, text_len)
        VALUES
          ('1', 's1', 'f', 1, '2026-01-01 00:00:00', 'response_item', 'message', 'user', 'message_content', 'zzz needle', 10),
          ('2', 's1', 'f', 2, '2026-01-02 00:00:00', 'response_item', 'message', 'user', 'message_content', 'needle zzz', 10)
        """
    )

    results, mode = search_impl(
        con,
        query="needle",
        limit=10,
        include_assistant=False,
        include_internal=False,
        sort="relevance",
        include_text=True,
    )
    assert mode == "substring"
    assert [r.doc_id for r in results] == ["2", "1"]  # earlier match position first


def test_substring_sort_recent_uses_timestamp() -> None:
    con = duckdb.connect(":memory:")
    _setup(con)
    con.execute(
        """
        INSERT INTO docs(doc_id, session_id, file_path, line_no, event_ts, event_type, inner_type, role, kind, text, text_len)
        VALUES
          ('1', 's1', 'f', 1, '2026-01-01 00:00:00', 'response_item', 'message', 'user', 'message_content', 'needle here', 11),
          ('2', 's1', 'f', 2, '2026-01-03 00:00:00', 'response_item', 'message', 'user', 'message_content', 'needle there', 12)
        """
    )

    results, mode = search_impl(
        con,
        query="needle",
        limit=10,
        include_assistant=False,
        include_internal=False,
        sort="recent",
        include_text=True,
    )
    assert mode == "substring"
    assert [r.doc_id for r in results] == ["2", "1"]  # newest first
