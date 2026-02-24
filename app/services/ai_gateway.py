from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

import requests

from ..core.cache import cache_client
from ..core.config import (
    AI_BUDGET_MONTHLY_USD,
    AI_GATEWAY_CACHE_TTL_SECONDS,
    AI_GATEWAY_MAX_REQUESTS_PER_MINUTE,
    AI_PROVIDER_ORDER,
    GEMINI_API_BASE_URL,
    GEMINI_API_KEY,
    GEMINI_MODEL,
    GITHUB_MODELS_API_KEY,
    GITHUB_MODELS_BASE_URL,
    GITHUB_MODELS_MODEL,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
)
from .ai_observability import record_gateway_provider_result

_TIMEOUT_SECONDS = 18
_BUDGET_LOCK = threading.Lock()
_SUPPORTED_PROVIDERS = {"gemini", "github_models", "ollama"}


@dataclass
class AIGatewayResult:
    text: str
    provider: str
    model: str
    cached: bool
    reason_codes: list[str]
    cost_estimate_usd: float = 0.0


def _normalize_provider_order(preferred: Optional[str] = None) -> list[str]:
    ordered: list[str] = []
    if preferred:
        normalized = str(preferred).strip().lower()
        if normalized in _SUPPORTED_PROVIDERS:
            ordered.append(normalized)
    for provider in AI_PROVIDER_ORDER:
        if provider in _SUPPORTED_PROVIDERS and provider not in ordered:
            ordered.append(provider)
    if not ordered:
        ordered = ["ollama", "github_models", "gemini"]
    return ordered


def _cache_key(namespace: str, payload: str) -> str:
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"ai:gateway:{namespace}:{digest}"


def _estimate_cost_usd(text: str, provider: str) -> float:
    # Coarse heuristic to enforce budget guard in low-budget environments.
    approx_tokens = max(1, len(text) // 4)
    if provider == "gemini":
        rate_per_1k = 0.0005
    elif provider == "github_models":
        rate_per_1k = 0.0008
    else:
        rate_per_1k = 0.0
    return (approx_tokens / 1000.0) * rate_per_1k


def _current_budget_key() -> str:
    now = datetime.utcnow()
    return f"ai:budget:{now.year:04d}-{now.month:02d}"


def _get_spent_usd() -> float:
    raw = cache_client.get(_current_budget_key())
    try:
        return float(raw) if raw is not None else 0.0
    except Exception:
        return 0.0


def _add_spent_usd(amount: float) -> None:
    if amount <= 0:
        return
    with _BUDGET_LOCK:
        total = _get_spent_usd() + amount
        cache_client.set(_current_budget_key(), str(total), ttl=60 * 60 * 24 * 40)


def _budget_allows(estimated_cost: float) -> bool:
    if AI_BUDGET_MONTHLY_USD <= 0:
        return True
    return (_get_spent_usd() + max(0.0, estimated_cost)) <= AI_BUDGET_MONTHLY_USD


def _sanitize_text(payload: object) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload.strip()
    return str(payload).strip()


def _call_gemini(prompt: str, model: Optional[str] = None) -> Optional[str]:
    if not GEMINI_API_KEY:
        return None
    model_name = model or GEMINI_MODEL
    url = f"{GEMINI_API_BASE_URL.rstrip('/')}/models/{model_name}:generateContent"
    params = {"key": GEMINI_API_KEY}
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2},
    }
    resp = requests.post(url, params=params, json=body, timeout=_TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()
    candidates = data.get("candidates") or []
    if not candidates:
        return None
    parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
    texts = [_sanitize_text(part.get("text")) for part in parts if isinstance(part, dict)]
    final_text = "\n".join([text for text in texts if text]).strip()
    return final_text or None


def _call_github_models(prompt: str, model: Optional[str] = None) -> Optional[str]:
    if not GITHUB_MODELS_API_KEY:
        return None
    model_name = model or GITHUB_MODELS_MODEL
    url = f"{GITHUB_MODELS_BASE_URL.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {GITHUB_MODELS_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    }
    resp = requests.post(url, headers=headers, json=body, timeout=_TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()
    choices = data.get("choices") or []
    if not choices:
        return None
    message = (choices[0] or {}).get("message") or {}
    return _sanitize_text(message.get("content")) or None


def _call_ollama(prompt: str, model: Optional[str] = None) -> Optional[str]:
    model_name = model or OLLAMA_MODEL
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/generate"
    body = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2},
    }
    resp = requests.post(url, json=body, timeout=_TIMEOUT_SECONDS)
    resp.raise_for_status()
    data = resp.json()
    return _sanitize_text(data.get("response")) or None


def _provider_model(provider: str, preferred_model: Optional[str]) -> str:
    if preferred_model:
        return preferred_model
    if provider == "gemini":
        return GEMINI_MODEL
    if provider == "github_models":
        return GITHUB_MODELS_MODEL
    return OLLAMA_MODEL


def _call_provider(provider: str, prompt: str, preferred_model: Optional[str]) -> Optional[str]:
    model_name = _provider_model(provider, preferred_model)
    if provider == "gemini":
        return _call_gemini(prompt, model_name)
    if provider == "github_models":
        return _call_github_models(prompt, model_name)
    if provider == "ollama":
        return _call_ollama(prompt, model_name)
    return None


def _fallback_response(prompt: str) -> str:
    lines = [line.strip() for line in prompt.splitlines() if line.strip()]
    topic = lines[-1] if lines else "request"
    if len(topic) > 200:
        topic = topic[:200].strip() + "..."
    return (
        "AI provider unavailable right now. Suggested manual triage:\n"
        "1) Reproduce issue with exact steps.\n"
        "2) Collect logs/screenshot tied to timestamp.\n"
        "3) Apply known workaround from runbook.\n"
        f"4) Escalate with summary: {topic}"
    )


def generate_text(
    *,
    prompt: str,
    system_prompt: Optional[str] = None,
    preferred_provider: Optional[str] = None,
    preferred_model: Optional[str] = None,
    cache_namespace: str = "default",
) -> AIGatewayResult:
    input_text = prompt.strip()
    if system_prompt:
        merged_prompt = f"{system_prompt.strip()}\n\n{input_text}"
    else:
        merged_prompt = input_text

    if not merged_prompt:
        return AIGatewayResult(
            text="",
            provider="none",
            model="none",
            cached=False,
            reason_codes=["empty_prompt"],
            cost_estimate_usd=0.0,
        )

    cache_key = _cache_key(cache_namespace, merged_prompt)
    cached = cache_client.get_json(cache_key)
    if isinstance(cached, dict) and cached.get("text"):
        record_gateway_provider_result(
            provider=str(cached.get("provider") or "cache"),
            model=str(cached.get("model") or "cache"),
            success=True,
            cached=True,
            failover=False,
            cost_estimate_usd=float(cached.get("cost_estimate_usd") or 0.0),
            reason="cache_hit",
        )
        return AIGatewayResult(
            text=str(cached.get("text")),
            provider=str(cached.get("provider") or "cache"),
            model=str(cached.get("model") or "cache"),
            cached=True,
            reason_codes=["cache_hit"],
            cost_estimate_usd=float(cached.get("cost_estimate_usd") or 0.0),
        )

    if AI_GATEWAY_MAX_REQUESTS_PER_MINUTE > 0:
        allowed = cache_client.check_rate_limit(
            "ai_gateway",
            limit=AI_GATEWAY_MAX_REQUESTS_PER_MINUTE,
            window_seconds=60,
        )
        if not allowed:
            record_gateway_provider_result(
                provider="gateway",
                model="none",
                success=False,
                cached=False,
                failover=False,
                cost_estimate_usd=0.0,
                reason="rate_limited",
            )
            return AIGatewayResult(
                text=_fallback_response(merged_prompt),
                provider="fallback",
                model="none",
                cached=False,
                reason_codes=["rate_limited", "fallback_used"],
                cost_estimate_usd=0.0,
            )

    providers = _normalize_provider_order(preferred_provider)
    errors: list[str] = []
    for attempt_index, provider in enumerate(providers):
        model_name = _provider_model(provider, preferred_model)
        estimated_cost = _estimate_cost_usd(merged_prompt, provider)
        failover = attempt_index > 0
        if not _budget_allows(estimated_cost):
            errors.append(f"{provider}:budget_exceeded")
            record_gateway_provider_result(
                provider=provider,
                model=model_name,
                success=False,
                cached=False,
                failover=failover,
                cost_estimate_usd=0.0,
                reason="budget_exceeded",
            )
            continue
        try:
            text = _call_provider(provider, merged_prompt, preferred_model)
            if not text:
                errors.append(f"{provider}:empty")
                record_gateway_provider_result(
                    provider=provider,
                    model=model_name,
                    success=False,
                    cached=False,
                    failover=failover,
                    cost_estimate_usd=0.0,
                    reason="empty_response",
                )
                continue
            result = {
                "text": text,
                "provider": provider,
                "model": model_name,
                "cost_estimate_usd": estimated_cost,
            }
            cache_client.set_json(cache_key, result, ttl=AI_GATEWAY_CACHE_TTL_SECONDS)
            _add_spent_usd(estimated_cost)
            record_gateway_provider_result(
                provider=provider,
                model=model_name,
                success=True,
                cached=False,
                failover=failover,
                cost_estimate_usd=estimated_cost,
                reason="provider_success",
            )
            return AIGatewayResult(
                text=text,
                provider=provider,
                model=model_name,
                cached=False,
                reason_codes=["provider_success"],
                cost_estimate_usd=estimated_cost,
            )
        except Exception as exc:
            errors.append(f"{provider}:{type(exc).__name__}")
            record_gateway_provider_result(
                provider=provider,
                model=model_name,
                success=False,
                cached=False,
                failover=failover,
                cost_estimate_usd=0.0,
                reason=type(exc).__name__,
            )

    fallback_text = _fallback_response(merged_prompt)
    reason_codes = ["all_providers_failed", "fallback_used"]
    if errors:
        reason_codes.append(f"errors={','.join(errors[:3])}")
    record_gateway_provider_result(
        provider="fallback",
        model="none",
        success=True,
        cached=False,
        failover=True,
        cost_estimate_usd=0.0,
        reason="all_providers_failed",
    )
    return AIGatewayResult(
        text=fallback_text,
        provider="fallback",
        model="none",
        cached=False,
        reason_codes=reason_codes,
        cost_estimate_usd=0.0,
    )


def hash_embedding(text: str, dimension: int = 128) -> list[float]:
    tokens = [token for token in _sanitize_text(text).lower().split() if token]
    if dimension <= 0:
        dimension = 128
    if not tokens:
        return [0.0] * dimension

    vector = [0.0] * dimension
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:4], "big") % dimension
        sign = -1.0 if (digest[4] & 1) else 1.0
        magnitude = 1.0 + (digest[5] / 255.0)
        vector[bucket] += sign * magnitude

    norm = sum(value * value for value in vector) ** 0.5
    if norm <= 1e-9:
        return [0.0] * dimension
    return [value / norm for value in vector]


def cosine_similarity(a: Iterable[float], b: Iterable[float]) -> float:
    left = list(a)
    right = list(b)
    if not left or not right:
        return 0.0
    size = min(len(left), len(right))
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for i in range(size):
        lv = float(left[i] or 0.0)
        rv = float(right[i] or 0.0)
        dot += lv * rv
        left_norm += lv * lv
        right_norm += rv * rv
    if left_norm <= 1e-9 or right_norm <= 1e-9:
        return 0.0
    return dot / ((left_norm ** 0.5) * (right_norm ** 0.5))
