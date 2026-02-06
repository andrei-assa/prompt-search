from __future__ import annotations

import json
import re
from dataclasses import asdict
from typing import Iterable

from rich.console import Console
from rich.table import Table
from rich.text import Text

from .search import SearchResult


OUTPUT_FORMATS = ("table", "text", "json", "markdown")
COLOR_MODES = ("auto", "always", "never")

def _short_id(value: str | None, n: int = 8) -> str:
    if not value:
        return "-"
    s = str(value)
    return s if len(s) <= n else s[:n]


def build_console(color: str) -> Console:
    c = color.lower()
    if c == "always":
        return Console(force_terminal=True, no_color=False)
    if c == "never":
        return Console(no_color=True)
    return Console()


def _normalize_needles(query: str) -> list[str]:
    q = query.strip()
    if not q:
        return []
    # Treat typical input as a substring, but highlight individual terms too.
    parts = [p for p in re.split(r"\s+", q) if p]
    needles: list[str] = []
    # Keep the whole query first (best UX for substring-style searching).
    needles.append(q)
    # Then highlight tokens.
    for p in parts:
        if len(p) < 2:
            continue
        if p.lower() == q.lower():
            continue
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
        if len(out) >= 8:
            break
    return out


def find_match_spans(haystack: str, needles: Iterable[str], *, case_insensitive: bool = True) -> list[tuple[int, int]]:
    text = haystack
    if not text:
        return []

    spans: list[tuple[int, int]] = []
    for needle in needles:
        if not needle:
            continue
        if case_insensitive:
            h = text.lower()
            n = needle.lower()
        else:
            h = text
            n = needle
        start = 0
        while True:
            idx = h.find(n, start)
            if idx < 0:
                break
            spans.append((idx, idx + len(needle)))
            start = idx + max(1, len(needle))

    if not spans:
        return []

    spans.sort()
    merged: list[tuple[int, int]] = []
    cur_s, cur_e = spans[0]
    for s, e in spans[1:]:
        if s <= cur_e:
            cur_e = max(cur_e, e)
        else:
            merged.append((cur_s, cur_e))
            cur_s, cur_e = s, e
    merged.append((cur_s, cur_e))
    return merged


def highlight_snippet_rich(snippet: str, query: str) -> Text:
    needles = _normalize_needles(query)
    spans = find_match_spans(snippet, needles, case_insensitive=True)
    t = Text(snippet)
    # High contrast that reads well on most terminals.
    for s, e in spans:
        t.stylize("bold black on yellow", s, e)
    return t


def highlight_snippet_markdown(snippet: str, query: str) -> str:
    needles = _normalize_needles(query)
    spans = find_match_spans(snippet, needles, case_insensitive=True)
    if not spans:
        return snippet
    s = snippet
    # Insert from end to start to keep indexes stable.
    for a, b in reversed(spans):
        s = s[:a] + "**" + s[a:b] + "**" + s[b:]
    return s


def render_search_results(
    *,
    results: list[SearchResult],
    mode: str,
    query: str,
    output_format: str,
    color: str,
) -> str | None:
    fmt = output_format.lower()
    if fmt not in OUTPUT_FORMATS:
        raise ValueError(f"unknown format: {output_format}")

    if fmt == "json":
        payload = []
        for r in results:
            payload.append(
                {
                    "doc_id": r.doc_id,
                    "session_id": r.session_id,
                    "event_ts": r.event_ts.isoformat() if r.event_ts else None,
                    "role": r.role,
                    "kind": r.kind,
                    "file_path": r.file_path,
                    "line_no": r.line_no,
                    "score": r.score,
                    "snippet": r.snippet,
                }
            )
        return json.dumps({"mode": mode, "results": payload}, ensure_ascii=True, indent=2, sort_keys=True)

    if fmt == "markdown":
        # Keep it clean and portable; no ANSI. Use bold for highlights.
        lines = []
        lines.append("| ts | score | session | role | snippet |")
        lines.append("|---:|---:|---|---|---|")
        for r in results:
            ts = r.event_ts.isoformat() if r.event_ts else "-"
            score = f"{r.score:.3f}" if r.score is not None else "-"
            sid = r.session_id or "-"
            role = r.role or "-"
            snippet = highlight_snippet_markdown(" ".join(r.snippet.split()), query)
            snippet = snippet.replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {ts} | {score} | `{sid}` | `{role}` | {snippet} |")
        return "\n".join(lines)

    console = build_console(color)

    if fmt == "table":
        table = Table(title=None, show_header=True, header_style="bold cyan")
        table.add_column("ts", style="dim", no_wrap=True)
        table.add_column("score", justify="right", style="magenta", no_wrap=True)
        # Use a short session id to keep tables readable in narrow terminals.
        table.add_column("session", style="cyan", no_wrap=True)
        table.add_column("role", style="green", no_wrap=True)
        table.add_column("snippet", overflow="fold")

        if mode != "fts":
            console.print(Text("(fts unavailable; using substring search)", style="dim yellow"))

        for r in results:
            ts = r.event_ts.isoformat() if r.event_ts else "-"
            score = f"{r.score:.3f}" if r.score is not None else "-"
            sid = _short_id(r.session_id, 8)
            role = r.role or "-"
            table.add_row(ts, score, sid, role, highlight_snippet_rich(r.snippet, query))

        console.print(table)
        return None

    # text mode
    if mode != "fts":
        console.print(Text("(fts unavailable; using substring search)", style="dim yellow"))
    for r in results:
        ts = r.event_ts.isoformat() if r.event_ts else "-"
        sid = _short_id(r.session_id, 8)
        role = r.role or "-"
        score = f"{r.score:.3f}" if r.score is not None else "-"
        prefix = Text(f"{ts}  {score}  {sid}  {role}  ", style="dim")
        console.print(prefix + highlight_snippet_rich(r.snippet, query))
    return None


def render_sessions(
    *,
    rows: list[dict],
    output_format: str,
    color: str,
) -> str | None:
    fmt = output_format.lower()
    if fmt not in OUTPUT_FORMATS:
        raise ValueError(f"unknown format: {output_format}")

    if fmt == "json":
        return json.dumps(rows, ensure_ascii=True, indent=2, sort_keys=True)

    if fmt == "markdown":
        lines = []
        lines.append("| last_ts | session_id | user | assistant | internal | cwd |")
        lines.append("|---:|---|---:|---:|---:|---|")
        for r in rows:
            last_ts = r.get("last_ts") or "-"
            sid = r.get("session_id") or "-"
            cwd = (r.get("cwd") or "-").replace("|", "\\|")
            lines.append(
                f"| {last_ts} | `{sid}` | {r.get('user_docs',0)} | {r.get('assistant_docs',0)} | {r.get('internal_docs',0)} | {cwd} |"
            )
        return "\n".join(lines)

    console = build_console(color)
    table = Table(title=None, show_header=True, header_style="bold cyan")
    table.add_column("last_ts", style="dim", no_wrap=True)
    table.add_column("session", style="cyan", no_wrap=True)
    table.add_column("user", justify="right", no_wrap=True)
    table.add_column("assistant", justify="right", no_wrap=True)
    table.add_column("internal", justify="right", no_wrap=True)
    table.add_column("cwd", overflow="fold")

    for r in rows:
        table.add_row(
            r.get("last_ts") or "-",
            _short_id(r.get("session_id"), 8),
            str(r.get("user_docs", 0)),
            str(r.get("assistant_docs", 0)),
            str(r.get("internal_docs", 0)),
            r.get("cwd") or "-",
        )

    if fmt == "table":
        console.print(table)
        return None

    # text: print one per line with mild color
    for r in rows:
        console.print(
            Text(r.get("last_ts") or "-", style="dim")
            + Text("  ")
            + Text(r.get("session_id") or "-", style="cyan")
            + Text(
                f"  user={r.get('user_docs',0)} assistant={r.get('assistant_docs',0)} internal={r.get('internal_docs',0)}  ",
                style="green",
            )
            + Text(f"cwd={r.get('cwd') or '-'}", style="dim")
        )
    return None
