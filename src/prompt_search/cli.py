from __future__ import annotations

import time
from pathlib import Path

import typer

from . import db as dbmod
from .ingest import refresh as refresh_impl
from .paths import db_path, default_data_dir, default_sessions_dir
from .search import search as search_impl
from .util import json_dumps_compact
from .render import COLOR_MODES, OUTPUT_FORMATS, render_search_results, render_sessions


app = typer.Typer(add_completion=False, no_args_is_help=True)

def _normalize_choice(value: str, allowed: tuple[str, ...], flag: str) -> str:
    v = (value or "").strip().lower()
    if v in allowed:
        return v
    typer.echo(f"invalid {flag}: {value!r} (choose one of: {', '.join(allowed)})", err=True)
    raise typer.Exit(code=2)


def _open_db_ro(data_dir: Path):
    p = db_path(data_dir)
    if not p.exists():
        typer.echo(f"database not found at {p}; run `prompt-search refresh` first", err=True)
        raise typer.Exit(code=1)
    # DuckDB takes an exclusive lock even for read-only connections while a writer is active.
    # We retry briefly to make `prompt-search search` resilient if a refresh just finished.
    last_err: Exception | None = None
    for _ in range(40):  # ~4s total
        try:
            return dbmod.connect_read_only(p)
        except Exception as e:
            msg = str(e)
            if "Conflicting lock is held" not in msg:
                raise
            last_err = e
            time.sleep(0.1)
    typer.echo(f"database is busy (locked); try again in a moment: {last_err}", err=True)
    raise typer.Exit(code=1)


@app.command()
def refresh(
    sessions_dir: Path = typer.Option(
        None, "--sessions-dir", help="Codex sessions directory (default: ~/.codex/sessions)."
    ),
    data_dir: Path = typer.Option(
        None, "--data-dir", help="prompt-search data directory (default: ~/.prompt-search)."
    ),
    full: bool = typer.Option(False, "--full", help="Drop all ingested data and re-ingest everything."),
    reindex: bool = typer.Option(True, "--reindex/--no-reindex", help="Rebuild FTS index after ingest."),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output."),
):
    sdir = (sessions_dir or default_sessions_dir()).expanduser()
    ddir = (data_dir or default_data_dir()).expanduser()

    stats = refresh_impl(
        sessions_dir=sdir,
        data_dir=ddir,
        full=full,
        reindex=reindex,
        include_assistant_in_ingest=True,
        include_internal_in_ingest=True,
        verbose=verbose,
    )

    typer.echo(
        f"scanned={stats.files_scanned} updated={stats.files_updated} "
        f"lines_read={stats.lines_read} lines_ingested={stats.lines_ingested} "
        f"docs_inserted={stats.docs_inserted} sessions_upserted={stats.sessions_upserted} "
        f"fts_available={int(stats.fts_available)} fts_reindexed={int(stats.fts_reindexed)}"
    )


@app.command("list-sessions")
def list_sessions(
    data_dir: Path = typer.Option(None, "--data-dir", help="prompt-search data directory."),
    limit: int = typer.Option(50, "--limit", min=1, max=5000),
    output_format: str = typer.Option(
        "table",
        "--format",
        help="Output format: table, text, json, markdown.",
        show_choices=True,
        case_sensitive=False,
    ),
    color: str = typer.Option(
        "auto",
        "--color",
        help="Color mode: auto, always, never.",
        show_choices=True,
        case_sensitive=False,
    ),
    json_out: bool = typer.Option(False, "--json", help="Alias for --format json."),
):
    ddir = (data_dir or default_data_dir()).expanduser()
    output_format = _normalize_choice("json" if json_out else output_format, OUTPUT_FORMATS, "--format")
    color = _normalize_choice(color, COLOR_MODES, "--color")
    con = _open_db_ro(ddir)

    rows = con.execute(
        """
        SELECT
          s.session_id,
          s.first_ts,
          s.last_ts,
          s.cwd,
          COUNT(CASE WHEN d.role = 'user' AND d.kind IN ('message_content','message_summary') THEN 1 END) AS user_docs,
          COUNT(CASE WHEN d.role = 'assistant' AND d.kind IN ('message_content','message_summary') THEN 1 END) AS assistant_docs,
          COUNT(CASE WHEN d.kind NOT IN ('message_content','message_summary') THEN 1 END) AS internal_docs
        FROM sessions s
        LEFT JOIN docs d ON d.session_id = s.session_id
        GROUP BY 1,2,3,4
        ORDER BY s.last_ts DESC NULLS LAST
        LIMIT ?
        """,
        [limit],
    ).fetchall()

    out = []
    for sid, first_ts, last_ts, cwd, user_docs, assistant_docs, internal_docs in rows:
        item = {
            "session_id": sid,
            "first_ts": first_ts.isoformat() if first_ts else None,
            "last_ts": last_ts.isoformat() if last_ts else None,
            "cwd": cwd,
            "user_docs": int(user_docs or 0),
            "assistant_docs": int(assistant_docs or 0),
            "internal_docs": int(internal_docs or 0),
        }
        out.append(item)

    rendered = render_sessions(rows=out, output_format=output_format, color=color)
    if rendered is not None:
        typer.echo(rendered)


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query."),
    data_dir: Path = typer.Option(None, "--data-dir", help="prompt-search data directory."),
    limit: int = typer.Option(20, "--limit", min=1, max=5000),
    snippet_len: int = typer.Option(180, "--snippet-len", min=40, max=2000, help="Snippet length."),
    sort: str = typer.Option(
        "relevance",
        "--sort",
        help="Sort order: relevance, recent.",
        show_choices=True,
        case_sensitive=False,
    ),
    context_lines: int = typer.Option(
        0,
        "--context-lines",
        min=0,
        max=200,
        help="Show N surrounding lines around the match (0=snippet).",
    ),
    full_content: bool = typer.Option(False, "--full-content", help="Print full matched content (very long)."),
    include_assistant: bool = typer.Option(False, "--include-assistant", help="Include assistant messages."),
    include_internal: bool = typer.Option(False, "--include-internal", help="Include internal events."),
    auto_refresh: bool = typer.Option(False, "--auto-refresh", help="Run refresh before searching."),
    sessions_dir: Path = typer.Option(None, "--sessions-dir", help="Used with --auto-refresh."),
    no_reindex: bool = typer.Option(False, "--no-reindex", help="Used with --auto-refresh."),
    output_format: str = typer.Option(
        "table",
        "--format",
        help="Output format: table, text, json, markdown.",
        show_choices=True,
        case_sensitive=False,
    ),
    color: str = typer.Option(
        "auto",
        "--color",
        help="Color mode: auto, always, never.",
        show_choices=True,
        case_sensitive=False,
    ),
    json_out: bool = typer.Option(False, "--json", help="Alias for --format json."),
):
    ddir = (data_dir or default_data_dir()).expanduser()
    output_format = _normalize_choice("json" if json_out else output_format, OUTPUT_FORMATS, "--format")
    color = _normalize_choice(color, COLOR_MODES, "--color")
    sort = _normalize_choice(sort, ("relevance", "recent"), "--sort")

    if full_content and context_lines > 0:
        typer.echo("cannot combine --full-content with --context-lines > 0", err=True)
        raise typer.Exit(code=2)
    if full_content and output_format == "table":
        typer.echo("cannot use --full-content with --format table; use --format text|markdown|json", err=True)
        raise typer.Exit(code=2)

    if auto_refresh:
        sdir = (sessions_dir or default_sessions_dir()).expanduser()
        refresh_impl(
            sessions_dir=sdir,
            data_dir=ddir,
            full=False,
            reindex=(not no_reindex),
            include_assistant_in_ingest=True,
            include_internal_in_ingest=True,
            verbose=False,
        )

    con = _open_db_ro(ddir)

    results, mode = search_impl(
        con,
        query=query,
        limit=limit,
        include_assistant=include_assistant,
        include_internal=include_internal,
        sort=sort,
        include_text=bool(full_content or context_lines > 0),
    )

    # Resolve the content to display per row.
    if full_content:
        # renderer will use .snippet; override snippet with full text for display
        results = [
            type(r)(
                doc_id=r.doc_id,
                session_id=r.session_id,
                event_ts=r.event_ts,
                role=r.role,
                kind=r.kind,
                file_path=r.file_path,
                line_no=r.line_no,
                score=r.score,
                match_pos=r.match_pos,
                snippet=(r.text or r.snippet),
                text=r.text,
            )
            for r in results
        ]
    elif context_lines > 0:
        from .search import extract_context_lines

        results = [
            type(r)(
                doc_id=r.doc_id,
                session_id=r.session_id,
                event_ts=r.event_ts,
                role=r.role,
                kind=r.kind,
                file_path=r.file_path,
                line_no=r.line_no,
                score=r.score,
                match_pos=r.match_pos,
                snippet=extract_context_lines(r.text or r.snippet, query, context_lines),
                text=r.text,
            )
            for r in results
        ]

    # Apply snippet length clamp at the presentation layer to keep search logic simple.
    if (not full_content) and context_lines == 0 and snippet_len != 180:
        trimmed = []
        for r in results:
            if len(r.snippet) <= snippet_len:
                trimmed.append(r)
            else:
                trimmed.append(
                    type(r)(
                        doc_id=r.doc_id,
                        session_id=r.session_id,
                        event_ts=r.event_ts,
                        role=r.role,
                        kind=r.kind,
                        file_path=r.file_path,
                        line_no=r.line_no,
                        score=r.score,
                        match_pos=r.match_pos,
                        snippet=r.snippet[: snippet_len - 1] + "…",
                        text=r.text,
                    )
                )
        results = trimmed

    rendered = render_search_results(
        results=results,
        mode=mode,
        query=query,
        output_format=output_format,
        color=color,
    )
    if rendered is not None:
        typer.echo(rendered)


@app.command("debug-db")
def debug_db(
    data_dir: Path = typer.Option(None, "--data-dir", help="prompt-search data directory."),
):
    """Print a few DB stats (useful for troubleshooting)."""
    ddir = (data_dir or default_data_dir()).expanduser()
    con = _open_db_ro(ddir)
    fts = dbmod.is_fts_available(con)
    docs = con.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
    sessions = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    typer.echo(json_dumps_compact({"fts_available": fts, "docs": int(docs), "sessions": int(sessions)}))
