from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .util import utcnow


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DbPaths:
    data_dir: Path
    db_file: Path


def connect(db_file: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_file))


def connect_read_only(db_file: Path) -> duckdb.DuckDBPyConnection:
    # Use read-only connections for read commands so they don't conflict with a concurrent refresh.
    return duckdb.connect(str(db_file), read_only=True)


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
          key VARCHAR PRIMARY KEY,
          value VARCHAR
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS session_files (
          path VARCHAR PRIMARY KEY,
          session_id VARCHAR,
          size BIGINT NOT NULL,
          mtime TIMESTAMP NOT NULL,
          mtime_epoch DOUBLE,
          last_offset BIGINT NOT NULL,
          last_line_no BIGINT NOT NULL,
          last_seen_at TIMESTAMP NOT NULL
        );
        """
    )

    # Backfill / forward-compat: older DBs may be missing new columns.
    try:
        con.execute("ALTER TABLE session_files ADD COLUMN IF NOT EXISTS mtime_epoch DOUBLE;")
    except Exception:
        # If ALTER isn't supported for some reason, we can still operate without mtime_epoch,
        # but refresh will be less efficient.
        pass

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
          session_id VARCHAR PRIMARY KEY,
          first_ts TIMESTAMP,
          last_ts TIMESTAMP,
          cwd VARCHAR,
          originator VARCHAR,
          cli_version VARCHAR,
          source VARCHAR,
          model_provider VARCHAR,
          instructions VARCHAR
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS docs (
          doc_id VARCHAR PRIMARY KEY,
          session_id VARCHAR,
          file_path VARCHAR NOT NULL,
          line_no BIGINT NOT NULL,
          event_ts TIMESTAMP,
          event_type VARCHAR,
          inner_type VARCHAR,
          role VARCHAR,
          kind VARCHAR NOT NULL,
          text VARCHAR NOT NULL,
          text_len BIGINT NOT NULL
        );
        """
    )

    # Set schema version if missing.
    cur = con.execute("SELECT value FROM settings WHERE key = 'schema_version'").fetchone()
    if cur is None:
        con.execute(
            "INSERT INTO settings(key, value) VALUES ('schema_version', ?)",
            [str(SCHEMA_VERSION)],
        )


def set_setting(con: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    con.execute(
        """
        INSERT INTO settings(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        [key, value],
    )


def get_setting(con: duckdb.DuckDBPyConnection, key: str) -> str | None:
    row = con.execute("SELECT value FROM settings WHERE key = ?", [key]).fetchone()
    if not row:
        return None
    return row[0]


def mark_fts_available(con: duckdb.DuckDBPyConnection, available: bool) -> None:
    set_setting(con, "fts_available", "1" if available else "0")


def is_fts_available(con: duckdb.DuckDBPyConnection) -> bool:
    v = get_setting(con, "fts_available")
    return v == "1"


def try_enable_fts(con: duckdb.DuckDBPyConnection) -> bool:
    # DuckDB FTS is an extension. We try LOAD first, and INSTALL+LOAD if needed.
    try:
        con.execute("LOAD fts")
        mark_fts_available(con, True)
        return True
    except Exception:
        pass

    try:
        con.execute("INSTALL fts")
        con.execute("LOAD fts")
        mark_fts_available(con, True)
        return True
    except Exception:
        mark_fts_available(con, False)
        return False


def rebuild_fts_index(con: duckdb.DuckDBPyConnection) -> None:
    # If the extension isn't available, this will raise; callers should guard.
    # DuckDB's FTS index must be rebuilt after inserts/updates.
    con.execute("PRAGMA create_fts_index('docs', 'doc_id', 'text', overwrite=1)")
    set_setting(con, "fts_reindexed_at", utcnow().isoformat())
    set_setting(con, "fts_index_ready", "1")


def exec_many(
    con: duckdb.DuckDBPyConnection, sql: str, rows: Iterable[Iterable[Any]]
) -> None:
    # `executemany` exists but isn't always surfaced consistently; using execute in a loop
    # is OK for our scale (tens of thousands of rows) inside a transaction.
    for r in rows:
        con.execute(sql, list(r))
