# prompt-search

Local full-text search for Codex session history stored under `~/.codex/sessions/**/*` (JSONL).

The tool ingests sessions into a local DuckDB database at `~/.prompt-search/db.duckdb` and provides:

```bash
prompt-search refresh
prompt-search search <query>
prompt-search list-sessions
```

## Install (recommended)

From this repo:

```bash
pipx install .
```

Or with uv:

```bash
uv tool install .
```

## Quickstart

```bash
prompt-search refresh
prompt-search search "my query"
prompt-search list-sessions
```

## Output formats

Both `search` and `list-sessions` support:

```bash
prompt-search search "Summarize" --format table
prompt-search search "Summarize" --format text
prompt-search search "Summarize" --format json
prompt-search search "Summarize" --format markdown
```

You can also control ANSI colors:

```bash
prompt-search search "Summarize" --color auto
prompt-search search "Summarize" --color always
prompt-search search "Summarize" --color never
```

## Notes

- If DuckDB's `fts` extension is unavailable (e.g. offline on first use), `search` will fall back to substring search.
- Default search scope is user-only. Use `--include-assistant` / `--include-internal` to widen.
