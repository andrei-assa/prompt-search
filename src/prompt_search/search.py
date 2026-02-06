from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .db import get_setting, is_fts_available, try_enable_fts


@dataclass(frozen=True)
class SearchResult:
    doc_id: str
    session_id: str | None
    event_ts: datetime | None
    role: str | None
    kind: str
    file_path: str
    line_no: int
    score: float | None
    snippet: str


def _make_snippet(text: str, query: str, max_len: int = 180) -> str:
    t = " ".join(text.split())
    if len(t) <= max_len:
        return t
    q = query.strip().lower()
    idx = t.lower().find(q) if q else -1
    if idx < 0:
        return t[: max_len - 1] + "…"
    start = max(0, idx - max_len // 3)
    end = min(len(t), start + max_len)
    s = t[start:end]
    if start > 0:
        s = "…" + s
    if end < len(t):
        s = s + "…"
    return s


def search(
    con: Any,
    *,
    query: str,
    limit: int,
    include_assistant: bool,
    include_internal: bool,
) -> tuple[list[SearchResult], str]:
    """
    Returns (results, mode) where mode is "fts" or "substring".
    """
    q = query.strip()
    if not q:
        return ([], "fts" if is_fts_available(con) else "substring")

    # Ensure we have an up-to-date sense of fts availability for this connection.
    if not is_fts_available(con):
        try_enable_fts(con)

    role_clause = "role = 'user'"
    if include_assistant:
        role_clause = "(role = 'user' OR role = 'assistant' OR role IS NULL)"

    kind_clause = "(kind IN ('message_content','message_summary'))"
    if include_internal:
        kind_clause = "(1=1)"

    fts_index_ready = get_setting(con, "fts_index_ready") == "1"

    if is_fts_available(con) and fts_index_ready:
        sql = f"""
        SELECT
          d.doc_id,
          d.session_id,
          d.event_ts,
          d.role,
          d.kind,
          d.file_path,
          d.line_no,
          fts_main_docs.match_bm25(d.doc_id, ?) AS score,
          d.text
        FROM docs d
        WHERE {role_clause}
          AND {kind_clause}
          AND fts_main_docs.match_bm25(d.doc_id, ?) IS NOT NULL
        ORDER BY score DESC NULLS LAST, d.event_ts DESC NULLS LAST
        LIMIT ?
        """
        try:
            rows = con.execute(sql, [q, q, limit]).fetchall()
        except Exception:
            # Most commonly: index not created yet (no refresh --reindex) or extension limitations.
            rows = []
            fts_index_ready = False
        results: list[SearchResult] = []
        for (doc_id, session_id, event_ts, role, kind, file_path, line_no, score, text) in rows:
            results.append(
                SearchResult(
                    doc_id=str(doc_id),
                    session_id=session_id if isinstance(session_id, str) else None,
                    event_ts=event_ts,
                    role=role if isinstance(role, str) else None,
                    kind=str(kind),
                    file_path=str(file_path),
                    line_no=int(line_no),
                    score=float(score) if score is not None else None,
                    snippet=_make_snippet(str(text), q),
                )
            )
        if results:
            return (results, "fts")
        # Fall through to substring if FTS yields nothing due to missing index.

    # Fallback: substring search
    rows = con.execute(
        f"""
        SELECT
          doc_id, session_id, event_ts, role, kind, file_path, line_no, text
        FROM docs
        WHERE {role_clause}
          AND {kind_clause}
          AND lower(text) LIKE '%' || lower(?) || '%'
        ORDER BY event_ts DESC NULLS LAST
        LIMIT ?
        """,
        [q, limit],
    ).fetchall()
    results = []
    for (doc_id, session_id, event_ts, role, kind, file_path, line_no, text) in rows:
        results.append(
            SearchResult(
                doc_id=str(doc_id),
                session_id=session_id if isinstance(session_id, str) else None,
                event_ts=event_ts,
                role=role if isinstance(role, str) else None,
                kind=str(kind),
                file_path=str(file_path),
                line_no=int(line_no),
                score=None,
                snippet=_make_snippet(str(text), q),
            )
        )
    return (results, "substring")
