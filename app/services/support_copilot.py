from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from ..models import SupportSession, SupportSuggestion
from .ai_gateway import generate_text

_WORD_RE = re.compile(r"[a-z0-9_]+", re.IGNORECASE)
_KB_CACHE: list[dict] | None = None


def _load_kb_documents() -> list[dict]:
    global _KB_CACHE
    if _KB_CACHE is not None:
        return _KB_CACHE

    data_dir = Path(__file__).resolve().parents[1] / "data"
    files = [
        data_dir / "fix_guides.json",
        data_dir / "bypass_categories.json",
    ]
    docs: list[dict] = []
    for file_path in files:
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        docs.extend(_flatten_doc_payload(payload, file_path.name))
    _KB_CACHE = docs
    return docs


def _flatten_doc_payload(payload: object, source_name: str, prefix: str = "") -> list[dict]:
    docs: list[dict] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            docs.extend(_flatten_doc_payload(value, source_name, next_prefix))
        if prefix and ("summary" in payload or "title" in payload or "steps" in payload):
            text_chunks: list[str] = []
            for field in ("title", "summary", "notes", "warnings"):
                value = payload.get(field)
                if isinstance(value, list):
                    text_chunks.extend(str(item) for item in value if item)
                elif value:
                    text_chunks.append(str(value))
            for step in payload.get("steps") or []:
                if isinstance(step, dict):
                    text_chunks.append(str(step.get("title") or ""))
                    text_chunks.append(str(step.get("description") or ""))
            text = " ".join(chunk for chunk in text_chunks if chunk).strip()
            if text:
                docs.append(
                    {
                        "id": f"{source_name}:{prefix}",
                        "source": source_name,
                        "path": prefix,
                        "text": text,
                    }
                )
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            next_prefix = f"{prefix}[{index}]"
            docs.extend(_flatten_doc_payload(value, source_name, next_prefix))
    return docs


def _tokenize(text: str) -> set[str]:
    return {token.lower() for token in _WORD_RE.findall(text or "") if len(token) >= 2}


def _retrieve_context(query: str, top_k: int = 4) -> list[dict]:
    documents = _load_kb_documents()
    if not documents:
        return []
    query_tokens = _tokenize(query)
    if not query_tokens:
        return []
    scored: list[tuple[int, dict]] = []
    for doc in documents:
        doc_tokens = _tokenize(str(doc.get("text") or ""))
        overlap = len(query_tokens.intersection(doc_tokens))
        if overlap > 0:
            scored.append((overlap, doc))
    scored.sort(key=lambda item: (-item[0], str(item[1].get("id") or "")))
    return [doc for _, doc in scored[:top_k]]


def _ensure_session(
    db: Session,
    *,
    user_id: Optional[str],
    session_id: Optional[str],
    topic: Optional[str],
    context: dict,
) -> SupportSession:
    if session_id:
        row = db.query(SupportSession).filter(SupportSession.id == session_id).first()
        if row is not None:
            return row
    row = SupportSession(
        user_id=user_id,
        topic=(topic or "")[:120] or None,
        status="open",
        context_payload=context or {},
    )
    db.add(row)
    db.flush()
    return row


def suggest_response(
    db: Session,
    *,
    user_id: Optional[str],
    session_id: Optional[str],
    topic: Optional[str],
    message: str,
    context: dict,
    preferred_provider: Optional[str],
    preferred_model: Optional[str],
) -> tuple[SupportSession, SupportSuggestion, list[str]]:
    session = _ensure_session(
        db,
        user_id=user_id,
        session_id=session_id,
        topic=topic,
        context=context,
    )

    kb_docs = _retrieve_context(message)
    kb_section = "\n".join(
        f"- [{doc.get('source')}::{doc.get('path')}] {str(doc.get('text') or '')[:320]}"
        for doc in kb_docs
    )
    context_json = json.dumps(context or {}, ensure_ascii=False)
    prompt = (
        "Ticket message:\n"
        f"{message.strip()}\n\n"
        "Context JSON:\n"
        f"{context_json}\n\n"
        "Knowledge snippets:\n"
        f"{kb_section or '- No matching snippet found.'}\n\n"
        "Output format:\n"
        "1) Problem summary (1 sentence)\n"
        "2) Recommended steps (3-6 bullets)\n"
        "3) Verification checklist (2-4 bullets)\n"
        "4) Escalation condition (1 sentence)\n"
    )
    system_prompt = (
        "You are OTOSHI Support Copilot. "
        "Provide safe, actionable, non-destructive troubleshooting guidance. "
        "Do not claim guaranteed fixes. "
        "Keep recommendations reviewable by human support staff."
    )
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()

    existing = (
        db.query(SupportSuggestion)
        .filter(
            SupportSuggestion.session_id == session.id,
            SupportSuggestion.prompt_hash == prompt_hash,
        )
        .order_by(SupportSuggestion.created_at.desc())
        .first()
    )
    if existing is not None:
        existing.cached = True
        db.flush()
        return session, existing, ["cache_hit"]

    gateway_result = generate_text(
        prompt=prompt,
        system_prompt=system_prompt,
        preferred_provider=preferred_provider,
        preferred_model=preferred_model,
        cache_namespace="support_copilot",
    )

    suggestion = SupportSuggestion(
        session_id=session.id,
        user_id=user_id,
        provider=gateway_result.provider,
        model=gateway_result.model,
        prompt_hash=prompt_hash,
        input_text=message.strip(),
        suggestion_text=gateway_result.text,
        confidence=0.85 if gateway_result.provider != "fallback" else 0.45,
        cached=gateway_result.cached,
        payload={
            "reason_codes": gateway_result.reason_codes,
            "kb_hits": [doc.get("id") for doc in kb_docs],
            "estimated_cost_usd": gateway_result.cost_estimate_usd,
        },
    )
    db.add(suggestion)
    db.flush()
    return session, suggestion, list(gateway_result.reason_codes)

