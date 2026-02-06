# Contributing

Thanks for your interest in contributing to `prompt-search`.

## Development setup

This project targets Python 3.10+.

Using `uv` (recommended):

```bash
uv venv
uv pip install -e ".[dev]"
```

Or using `pip` + `venv`:

```bash
python3 -m venv .venv
./.venv/bin/python -m ensurepip --upgrade
./.venv/bin/python -m pip install -e ".[dev]"
```

## Running tests

```bash
python -m pytest
```

## Linting

```bash
ruff check .
```

## Packaging checks

```bash
python -m build
twine check dist/*
```

## Pull requests

- Keep changes focused and add/adjust tests when behavior changes.
- Update `README.md` examples when you add or change CLI flags.

