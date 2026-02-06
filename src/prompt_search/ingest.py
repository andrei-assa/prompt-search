from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from . import db as dbmod
from .paths import db_path
from .extract import extract_docs_from_event, extract_session_meta
from .util import ExtractedDoc, parse_ts, utcnow


@dataclass
class RefreshStats:
    files_scanned: int = 0
    files_updated: int = 0
    lines_read: int = 0
    lines_ingested: int = 0
    docs_inserted: int = 0
    sessions_upserted: int = 0
    fts_available: bool = False
    fts_reindexed: bool = False


def _iter_jsonl_paths(sessions_dir: Path) -> list[Path]:
    if not sessions_dir.exists():
        return []
    # Codex uses nested YYYY/MM/DD dirs, but we keep it generic.
    return sorted(p for p in sessions_dir.rglob("*.jsonl") if p.is_file())


def _delete_file_docs(con: Any, file_path: str) -> None:
    con.execute("DELETE FROM docs WHERE file_path = ?", [file_path])


def _upsert_session(con: Any, meta: dict[str, Any], event_ts_str: str | None) -> int:
    session_id = meta.get("id")
    if not isinstance(session_id, str) or not session_id:
        return 0

    ts = parse_ts(meta.get("timestamp")) or parse_ts(event_ts_str)
    cwd = meta.get("cwd")
    originator = meta.get("originator")
    cli_version = meta.get("cli_version")
    source = meta.get("source")
    model_provider = meta.get("model_provider")
    instructions = meta.get("instructions")

    # Insert or update, and also keep first_ts/last_ts conservative.
    con.execute(
        """
        INSERT INTO sessions(
          session_id, first_ts, last_ts, cwd, originator, cli_version, source, model_provider, instructions
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(session_id) DO UPDATE SET
          first_ts = LEAST(COALESCE(sessions.first_ts, excluded.first_ts), excluded.first_ts),
          last_ts = GREATEST(COALESCE(sessions.last_ts, excluded.last_ts), excluded.last_ts),
          cwd = COALESCE(excluded.cwd, sessions.cwd),
          originator = COALESCE(excluded.originator, sessions.originator),
          cli_version = COALESCE(excluded.cli_version, sessions.cli_version),
          source = COALESCE(excluded.source, sessions.source),
          model_provider = COALESCE(excluded.model_provider, sessions.model_provider),
          instructions = COALESCE(excluded.instructions, sessions.instructions)
        """,
        [
            session_id,
            ts,
            ts,
            cwd if isinstance(cwd, str) else None,
            originator if isinstance(originator, str) else None,
            cli_version if isinstance(cli_version, str) else None,
            source if isinstance(source, str) else None,
            model_provider if isinstance(model_provider, str) else None,
            instructions if isinstance(instructions, str) else None,
        ],
    )
    return 1


def _insert_docs(con: Any, docs: Iterable[ExtractedDoc]) -> int:
    n = 0
    for d in docs:
        con.execute(
            """
            INSERT OR IGNORE INTO docs(
              doc_id, session_id, file_path, line_no, event_ts, event_type, inner_type, role, kind, text, text_len
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                d.doc_id,
                d.session_id,
                d.file_path,
                d.line_no,
                d.event_ts,
                d.event_type,
                d.inner_type,
                d.role,
                d.kind,
                d.text,
                len(d.text),
            ],
        )
        n += 1
    return n


def _open_for_incremental(path: Path) -> tuple[object, int, float]:
    st = path.stat()
    # `mtime` from stat is float seconds; DuckDB TIMESTAMP accepts python datetime.
    return st, int(st.st_size), float(st.st_mtime)


def refresh(
    *,
    sessions_dir: Path,
    data_dir: Path,
    full: bool = False,
    reindex: bool = True,
    include_assistant_in_ingest: bool = True,
    include_internal_in_ingest: bool = True,
    verbose: bool = False,
) -> RefreshStats:
    data_dir.mkdir(parents=True, exist_ok=True)
    con = dbmod.connect(db_path(data_dir))
    dbmod.ensure_schema(con)

    stats = RefreshStats()

    if full:
        # Start fresh.
        con.execute("DELETE FROM docs")
        con.execute("DELETE FROM sessions")
        con.execute("DELETE FROM session_files")
        dbmod.set_setting(con, "fts_available", "0")
        dbmod.set_setting(con, "fts_index_ready", "0")

    fts_ok = dbmod.try_enable_fts(con)
    stats.fts_available = fts_ok

    paths = _iter_jsonl_paths(sessions_dir)
    stats.files_scanned = len(paths)

    # Use a transaction for speed and consistency.
    con.execute("BEGIN TRANSACTION")
    try:
        for p in paths:
            file_path = str(p)
            st = p.stat()
            size = int(st.st_size)
            # Store local/naive timestamp for display and store epoch seconds for stable comparisons.
            mtime_dt = datetime.fromtimestamp(st.st_mtime)
            mtime_epoch = float(st.st_mtime)

            row = con.execute(
                "SELECT size, mtime_epoch, mtime, last_offset, last_line_no, session_id FROM session_files WHERE path = ?",
                [file_path],
            ).fetchone()

            last_offset = 0
            last_line_no = 0
            known_session_id: str | None = None
            if row:
                prev_size, prev_mtime_epoch, prev_mtime, last_offset, last_line_no, known_session_id = row
                # Truncation: reset and delete docs for the file.
                if size < int(last_offset):
                    if verbose:
                        pass
                    _delete_file_docs(con, file_path)
                    last_offset = 0
                    last_line_no = 0

                # No change: skip.
                same_size = size == int(prev_size)
                same_mtime = False
                if prev_mtime_epoch is not None:
                    try:
                        same_mtime = abs(float(prev_mtime_epoch) - mtime_epoch) < 0.0005
                    except Exception:
                        same_mtime = False
                if same_size and same_mtime:
                    con.execute(
                        """
                        UPDATE session_files
                        SET last_seen_at = ?
                        WHERE path = ?
                        """,
                        [utcnow(), file_path],
                    )
                    continue

            # Ingest appended lines.
            stats.files_updated += 1

            docs_to_insert: list[ExtractedDoc] = []
            sessions_upsert = 0
            new_session_id: str | None = known_session_id

            # Binary read so we can maintain byte offsets precisely.
            offset = int(last_offset)
            line_no = int(last_line_no)
            last_good_offset = offset
            last_good_line_no = line_no

            with p.open("rb") as f:
                if offset > 0:
                    f.seek(offset)
                while True:
                    bline = f.readline()
                    if not bline:
                        break
                    stats.lines_read += 1
                    try:
                        line = bline.decode("utf-8", errors="replace").strip()
                    except Exception:
                        # Bail on decode errors, keep offset at last good line.
                        break

                    # Always advance line number for each newline-delimited record we see.
                    line_no += 1
                    offset = f.tell()

                    if not line:
                        last_good_offset = offset
                        last_good_line_no = line_no
                        continue

                    try:
                        event = json.loads(line)
                    except Exception:
                        # Likely partial write; stop and do not advance beyond last good JSON.
                        break

                    if not isinstance(event, dict):
                        last_good_offset = offset
                        last_good_line_no = line_no
                        continue

                    stats.lines_ingested += 1

                    meta = extract_session_meta(event)
                    if meta is not None:
                        sessions_upsert += _upsert_session(con, meta, event.get("timestamp"))
                        sid = meta.get("id")
                        if isinstance(sid, str) and sid:
                            new_session_id = sid

                    extracted = extract_docs_from_event(
                        event=event,
                        file_path=file_path,
                        line_no=line_no,
                        session_id_hint=new_session_id,
                        include_assistant=include_assistant_in_ingest,
                        include_internal=include_internal_in_ingest,
                    )
                    if extracted:
                        docs_to_insert.extend(extracted)

                    last_good_offset = offset
                    last_good_line_no = line_no

            if docs_to_insert:
                stats.docs_inserted += _insert_docs(con, docs_to_insert)

            stats.sessions_upserted += sessions_upsert

            con.execute(
                """
                INSERT INTO session_files(path, session_id, size, mtime, mtime_epoch, last_offset, last_line_no, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                  session_id = COALESCE(excluded.session_id, session_files.session_id),
                  size = excluded.size,
                  mtime = excluded.mtime,
                  mtime_epoch = excluded.mtime_epoch,
                  last_offset = excluded.last_offset,
                  last_line_no = excluded.last_line_no,
                  last_seen_at = excluded.last_seen_at
                """,
                [
                    file_path,
                    new_session_id,
                    size,
                    mtime_dt,
                    mtime_epoch,
                    last_good_offset,
                    last_good_line_no,
                    utcnow(),
                ],
            )

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    # Rebuild FTS if enabled and requested.
    if reindex and stats.docs_inserted > 0 and stats.fts_available:
        try:
            dbmod.rebuild_fts_index(con)
            stats.fts_reindexed = True
        except Exception:
            stats.fts_reindexed = False
    elif stats.docs_inserted > 0:
        # New docs arrived but we didn't rebuild the index; signal that FTS isn't usable yet.
        dbmod.set_setting(con, "fts_index_ready", "0")

    return stats
