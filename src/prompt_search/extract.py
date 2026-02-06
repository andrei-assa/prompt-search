from __future__ import annotations

from typing import Any, Iterable

from .util import ExtractedDoc, json_dumps_pretty, parse_ts


def _iter_message_texts(content: Any) -> Iterable[str]:
    if not isinstance(content, list):
        return
    for item in content:
        if not isinstance(item, dict):
            continue
        txt = item.get("text")
        if isinstance(txt, str) and txt.strip():
            yield txt


def _iter_summary_texts(summary: Any) -> Iterable[str]:
    if not isinstance(summary, list):
        return
    for item in summary:
        if not isinstance(item, dict):
            continue
        txt = item.get("text")
        if isinstance(txt, str) and txt.strip():
            yield txt


def extract_docs_from_event(
    *,
    event: dict[str, Any],
    file_path: str,
    line_no: int,
    session_id_hint: str | None,
    include_assistant: bool,
    include_internal: bool,
) -> list[ExtractedDoc]:
    """
    Convert a single JSONL event into 0..N extracted text docs.

    We ingest broadly, then filter at query-time. However, we still allow caller knobs
    so `refresh --user-only` style behaviors can be implemented later without schema changes.
    """
    event_type = event.get("type")
    event_ts = parse_ts(event.get("timestamp"))
    payload = event.get("payload")
    out: list[ExtractedDoc] = []

    def add_doc(*, seg_idx: int, role: str | None, kind: str, inner_type: str | None, text: str):
        if not text.strip():
            return
        doc_id = f"{file_path}:{line_no}:{seg_idx}"
        out.append(
            ExtractedDoc(
                doc_id=doc_id,
                session_id=session_id_hint,
                file_path=file_path,
                line_no=line_no,
                event_ts=event_ts,
                event_type=event_type,
                inner_type=inner_type,
                role=role,
                kind=kind,
                text=text,
            )
        )

    if event_type == "response_item" and isinstance(payload, dict):
        inner_type = payload.get("type")
        if inner_type == "message":
            role = payload.get("role")
            # We always ingest, but allow skipping now if desired.
            if role == "assistant" and not include_assistant:
                return []

            seg_idx = 0
            for txt in _iter_message_texts(payload.get("content")):
                seg_idx += 1
                add_doc(
                    seg_idx=seg_idx,
                    role=role if isinstance(role, str) else None,
                    kind="message_content",
                    inner_type=inner_type,
                    text=txt,
                )

            # Some sessions store encrypted content but still include plaintext summary.
            for txt in _iter_summary_texts(payload.get("summary")):
                seg_idx += 1
                add_doc(
                    seg_idx=seg_idx,
                    role=role if isinstance(role, str) else None,
                    kind="message_summary",
                    inner_type=inner_type,
                    text=txt,
                )

        return out

    if event_type == "event_msg" and isinstance(payload, dict):
        inner_type = payload.get("type")
        # Avoid duplicating user/assistant messages which typically exist as response_item already.
        if inner_type in ("user_message", "agent_message"):
            return []
        if inner_type == "agent_reasoning":
            if not include_internal:
                return []
            txt = payload.get("text")
            if isinstance(txt, str):
                add_doc(
                    seg_idx=1,
                    role=None,
                    kind="agent_reasoning",
                    inner_type=inner_type,
                    text=txt,
                )
            return out

        if inner_type == "item_completed":
            if not include_internal:
                return []
            item = payload.get("item")
            if isinstance(item, dict):
                txt = item.get("text")
                if isinstance(txt, str):
                    add_doc(
                        seg_idx=1,
                        role=None,
                        kind="item_completed",
                        inner_type=inner_type,
                        text=txt,
                    )
            return out

        if inner_type == "exited_review_mode":
            if not include_internal:
                return []
            review_output = payload.get("review_output")
            if review_output is not None:
                add_doc(
                    seg_idx=1,
                    role=None,
                    kind="review_output",
                    inner_type=inner_type,
                    text=json_dumps_pretty(review_output),
                )
            return out

        return []

    return []


def extract_session_meta(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("type") != "session_meta":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    sid = payload.get("id")
    if not isinstance(sid, str) or not sid:
        return None
    return payload

