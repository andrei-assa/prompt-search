from __future__ import annotations

from prompt_search.render import find_match_spans, highlight_snippet_markdown


def test_find_match_spans_basic() -> None:
    s = "hello duckdb duckdb"
    spans = find_match_spans(s, ["duckdb"])
    assert spans == [(6, 12), (13, 19)]


def test_find_match_spans_case_insensitive() -> None:
    s = "DuckDB duckdb"
    spans = find_match_spans(s, ["duckdb"])
    assert spans == [(0, 6), (7, 13)]


def test_find_match_spans_merge_overlaps() -> None:
    s = "foobar"
    spans = find_match_spans(s, ["foo", "foobar"])
    assert spans == [(0, 6)]


def test_highlight_markdown_inserts_bold() -> None:
    s = "hello duckdb world"
    out = highlight_snippet_markdown(s, "duckdb")
    assert out == "hello **duckdb** world"

