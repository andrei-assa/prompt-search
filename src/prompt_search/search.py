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
    match_pos: int | None
    snippet: str
    text: str | None


def _make_snippet(text: str, query: str, max_len: int = 180) -> str:
    t = " ".join(text.split())
    if len(t) <= max_len:
        return t
    q_raw = query.strip()
    if not q_raw:
        return t[: max_len - 1] + "…"

    # Prefer centering on any matching token, but keep substring UX by checking the full query too.
    needles = [q_raw] + [p for p in q_raw.split() if len(p) >= 2]
    h = t.lower()
    idx = -1
    for n in needles:
        j = h.find(n.lower())
        if j >= 0 and (idx < 0 or j < idx):
            idx = j
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


def _normalize_needles(query: str) -> list[str]:
    q = query.strip()
    if not q:
        return []
    needles = [q]
    for p in q.split():
        if len(p) >= 2:
            needles.append(p)
    # Deduplicate case-insensitively while preserving order.
    seen = set()
    out = []
    for n in needles:
        key = n.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out


def extract_context_lines(text: str, query: str, n: int) -> str:
    if n <= 0:
        return _make_snippet(text, query)

    lines = text.splitlines()
    if len(lines) <= 1:
        return _make_snippet(text, query)

    needles = _normalize_needles(query)
    if not needles:
        return "\n".join(lines[: min(len(lines), 2 * n + 1)])

    low_lines = [ln.lower() for ln in lines]
    best_i = None
    best_pos = None
    for i, ln in enumerate(low_lines):
        for needle in needles:
            pos = ln.find(needle.lower())
            if pos < 0:
                continue
            if best_pos is None or pos < best_pos or (pos == best_pos and (best_i is None or i < best_i)):
                best_pos = pos
                best_i = i

    if best_i is None:
        return "\n".join(lines[: min(len(lines), 2 * n + 1)])

    start = max(0, best_i - n)
    end = min(len(lines), best_i + n + 1)
    return "\n".join(lines[start:end])


def search(
    con: Any,
    *,
    query: str,
    limit: int,
    include_assistant: bool,
    include_internal: bool,
    sort: str = "relevance",
    include_text: bool = False,
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
        order = "score DESC NULLS LAST, d.event_ts DESC NULLS LAST"
        if sort == "recent":
            order = "d.event_ts DESC NULLS LAST, score DESC NULLS LAST"
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
          d.text,
          NULL::INTEGER AS match_pos
        FROM docs d
        WHERE {role_clause}
          AND {kind_clause}
          AND fts_main_docs.match_bm25(d.doc_id, ?) IS NOT NULL
        ORDER BY {order}
        LIMIT ?
        """
        try:
            rows = con.execute(sql, [q, q, limit]).fetchall()
        except Exception:
            # Most commonly: index not created yet (no refresh --reindex) or extension limitations.
            rows = []
            fts_index_ready = False
        results: list[SearchResult] = []
        for (doc_id, session_id, event_ts, role, kind, file_path, line_no, score, text, match_pos) in rows:
            raw_text = str(text)
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
                    match_pos=None,
                    snippet=_make_snippet(raw_text, q),
                    text=raw_text if include_text else None,
                )
            )
        if results:
            return (results, "fts")
        # Fall through to substring if FTS yields nothing due to missing index.

    # Fallback: substring search
    order = "match_pos ASC NULLS LAST, event_ts DESC NULLS LAST"
    if sort == "recent":
        order = "event_ts DESC NULLS LAST, match_pos ASC NULLS LAST"

    text_expr = "text"
    if not include_text:
        # Still needed for snippet generation, but we can keep it in the row and drop later.
        text_expr = "text"

    rows = con.execute(
        f"""
        SELECT
          doc_id, session_id, event_ts, role, kind, file_path, line_no,
          {text_expr} AS text,
          instr(lower(text), lower(?)) AS match_pos
        FROM docs
        WHERE {role_clause}
          AND {kind_clause}
          AND lower(text) LIKE '%' || lower(?) || '%'
        ORDER BY {order}
        LIMIT ?
        """,
        [q, q, limit],
    ).fetchall()
    results = []
    for (doc_id, session_id, event_ts, role, kind, file_path, line_no, text, match_pos) in rows:
        raw_text = str(text)
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
                match_pos=int(match_pos) if match_pos is not None else None,
                snippet=_make_snippet(raw_text, q),
                text=raw_text if include_text else None,
            )
        )
    return (results, "substring")
