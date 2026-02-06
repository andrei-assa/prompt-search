from __future__ import annotations

import json
import time
from pathlib import Path

import typer

from . import db as dbmod
from .ingest import refresh as refresh_impl
from .paths import db_path, default_data_dir, default_sessions_dir
from .search import search as search_impl
from .util import json_dumps_compact


app = typer.Typer(add_completion=False, no_args_is_help=True)


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
    json_out: bool = typer.Option(False, "--json", help="Output JSON."),
):
    ddir = (data_dir or default_data_dir()).expanduser()
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

    if json_out:
        typer.echo(json.dumps(out, ensure_ascii=True, indent=2, sort_keys=True))
        return

    for item in out:
        typer.echo(
            f"{item['last_ts'] or '-'}  {item['session_id']}  "
            f"user={item['user_docs']} assistant={item['assistant_docs']} internal={item['internal_docs']}  "
            f"cwd={item['cwd'] or '-'}"
        )


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query."),
    data_dir: Path = typer.Option(None, "--data-dir", help="prompt-search data directory."),
    limit: int = typer.Option(20, "--limit", min=1, max=5000),
    include_assistant: bool = typer.Option(False, "--include-assistant", help="Include assistant messages."),
    include_internal: bool = typer.Option(False, "--include-internal", help="Include internal events."),
    auto_refresh: bool = typer.Option(False, "--auto-refresh", help="Run refresh before searching."),
    sessions_dir: Path = typer.Option(None, "--sessions-dir", help="Used with --auto-refresh."),
    no_reindex: bool = typer.Option(False, "--no-reindex", help="Used with --auto-refresh."),
    json_out: bool = typer.Option(False, "--json", help="Output JSON."),
):
    ddir = (data_dir or default_data_dir()).expanduser()

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
    )

    if json_out:
        payload = [
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
            for r in results
        ]
        typer.echo(json.dumps({"mode": mode, "results": payload}, ensure_ascii=True, indent=2, sort_keys=True))
        return

    if mode != "fts":
        typer.echo("(fts unavailable; using substring search)")

    for r in results:
        ts = r.event_ts.isoformat() if r.event_ts else "-"
        sid = r.session_id or "-"
        role = r.role or "-"
        score = f"{r.score:.3f}" if r.score is not None else "-"
        typer.echo(f"{ts}  {score}  {sid}  {role}  {r.snippet}")


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
