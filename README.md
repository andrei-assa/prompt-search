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

## Examples

### Refresh / ingest

Ingest sessions from `~/.codex/sessions` into `~/.prompt-search/db.duckdb`:

```bash
prompt-search refresh
```

Rebuild from scratch:

```bash
prompt-search refresh --full
```

Skip rebuilding the FTS index (faster ingest; search will still work, but may fall back to substring until the next indexed refresh):

```bash
prompt-search refresh --no-reindex
```

Ingest from a custom location:

```bash
prompt-search refresh --sessions-dir ~/path/to/sessions --data-dir ~/path/to/prompt-search-data
```

### Search (common)

Most common usage:

```bash
prompt-search search "Summarize my .github directory"
```

Auto-refresh before searching:

```bash
prompt-search search "duckdb" --auto-refresh
```

Search assistant messages too:

```bash
prompt-search search "regression" --include-assistant
```

Search internal events too (reasoning, plan output, etc.):

```bash
prompt-search search "Plan" --include-internal
```

### Sorting

Sort by most relevant (default):

```bash
prompt-search search "duckdb" --sort relevance
```

Sort by most recent:

```bash
prompt-search search "duckdb" --sort recent
```

### Match context

Control snippet length (default is `180`):

```bash
prompt-search search "duckdb" --snippet-len 400
```

Show N surrounding lines around the match (grep-like context):

```bash
prompt-search search "Error Message" --context-lines 2 --format text
```

Print the full matched content (use with `text`, `markdown`, or `json`):

```bash
prompt-search search "Module not found" --full-content --format text
prompt-search search "Module not found" --full-content --format json
```

### Output formats

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

### JSON output + jq

```bash
prompt-search search "duckdb" --format json \
  | jq '.results[] | {ts: .event_ts, session: .session_id, snippet: .snippet}'
```

### Markdown output (redirect to file)

```bash
prompt-search search "duckdb" --format markdown > results.md
prompt-search list-sessions --format markdown > sessions.md
```

### List sessions

Default table output:

```bash
prompt-search list-sessions --limit 20
```

JSON output:

```bash
prompt-search list-sessions --format json
```

## Notes

- If DuckDB's `fts` extension is unavailable (e.g. offline on first use), `search` will fall back to substring search.
- Default search scope is user-only. Use `--include-assistant` / `--include-internal` to widen.
