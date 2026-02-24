from __future__ import annotations

import os
import re
from typing import Optional

from ..core.cache import cache_client
from ..core.config import STEAM_CATALOG_CACHE_TTL_SECONDS
from ..services.native_core import get_native_core
from .steam_catalog import get_catalog_page, get_hot_appids, search_store

MAX_CANDIDATES = 200
MAX_NUMERIC_CANDIDATES = 60
MIN_SCORE = 25.0
LOCAL_FALLBACK_BATCH_SIZE = 48
LOCAL_FALLBACK_MAX_SCAN = 1200
LOCAL_FALLBACK_TARGET_MATCHES = 40
SEARCH_VARIANT_LIMIT = max(1, int(os.getenv("STEAM_SEARCH_VARIANT_LIMIT", "1")))
STEAM_SEARCH_ENABLE_LOCAL_FALLBACK = os.getenv(
    "STEAM_SEARCH_ENABLE_LOCAL_FALLBACK",
    "false",
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)

_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
_ACRONYM_STOPWORDS = {"the", "of", "and", "for", "to", "a", "an"}
_ROMAN_DIGITS = {
    "i": 1,
    "v": 5,
    "x": 10,
    "l": 50,
    "c": 100,
    "d": 500,
    "m": 1000,
}


def normalize_text(value: str) -> str:
    lowered = value.strip().lower()
    cleaned = _NON_ALNUM.sub(" ", lowered)
    return " ".join(cleaned.split())


def compact_text(value: str) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _roman_to_int(token: str) -> Optional[int]:
    raw = str(token or "").strip().lower()
    if not raw or not re.fullmatch(r"[ivxlcdm]{1,6}", raw):
        return None
    total = 0
    prev = 0
    for char in reversed(raw):
        value = _ROMAN_DIGITS.get(char)
        if value is None:
            return None
        if value < prev:
            total -= value
        else:
            total += value
            prev = value
    if total <= 0 or total > 3999:
        return None
    return total


def build_acronym_variants(value: str) -> set[str]:
    normalized = normalize_text(value)
    if not normalized:
        return set()

    tokens = [token for token in normalized.split(" ") if token]
    if not tokens:
        return set()

    alpha_tokens = [token for token in tokens if any(ch.isalpha() for ch in token)]
    filtered_alpha = [token for token in alpha_tokens if token not in _ACRONYM_STOPWORDS]
    acronym_tokens = filtered_alpha or alpha_tokens

    initials = "".join(token[0] for token in acronym_tokens if token)
    variants: set[str] = set()
    if len(initials) >= 2:
        variants.add(initials)

    numeric_tokens = [token for token in tokens if token.isdigit()]
    roman_numbers = [str(value) for token in tokens if (value := _roman_to_int(token)) is not None]

    for suffix in (numeric_tokens + roman_numbers):
        if initials and suffix:
            variants.add(f"{initials}{suffix}")

    compact = compact_text(normalized)
    if compact:
        variants.add(compact)

    return variants


def build_search_variants(query: str) -> list[str]:
    trimmed = query.strip()
    if not trimmed:
        return []
    variants = [trimmed]
    normalized = normalize_text(trimmed)
    if normalized and normalized not in variants:
        variants.append(normalized)
    if ":" in trimmed:
        head = trimmed.split(":", 1)[0].strip()
        if head and head not in variants:
            variants.append(head)
        head_norm = normalize_text(head)
        if head_norm and head_norm not in variants:
            variants.append(head_norm)
    if "-" in trimmed:
        head = trimmed.split("-", 1)[0].strip()
        if head and head not in variants:
            variants.append(head)
    return variants[:4]


def _native_score(query: str, candidate: str) -> float:
    core = get_native_core()
    if not core:
        return 0.0
    try:
        return float(core.score_search(query, candidate))
    except Exception:
        return 0.0


def _token_score(query: str, candidate: str) -> float:
    tokens = query.split()
    if not tokens:
        return 0.0
    candidate_tokens = candidate.split()
    if not candidate_tokens:
        return 0.0
    matched = 0
    for token in tokens:
        if any(ct.startswith(token) or token in ct for ct in candidate_tokens):
            matched += 1
    ratio = matched / len(tokens)
    return 60.0 + ratio * 20.0


def score_candidate(query: str, item: dict, hot_rank: dict[str, int]) -> float:
    name = str(item.get("name") or "").strip()
    app_id = str(item.get("app_id") or "")
    if not name and not app_id:
        return 0.0
    query_norm = normalize_text(query)
    query_compact = compact_text(query)
    name_norm = normalize_text(name)
    score = 0.0

    if query_norm and name_norm:
        if name_norm == query_norm:
            score = 100.0
        elif name_norm.startswith(query_norm):
            score = 94.0
        elif query_norm in name_norm:
            score = 88.0
        else:
            score = max(score, _token_score(query_norm, name_norm))
        score = max(score, _native_score(query_norm, name_norm))

    if query_compact and name:
        acronym_variants = build_acronym_variants(name)
        if query_compact in acronym_variants:
            score = max(score, 93.0)
        elif len(query_compact) >= 2 and any(
            variant.startswith(query_compact) for variant in acronym_variants if variant
        ):
            score = max(score, 86.0)

    if query_compact.isdigit() and app_id:
        if app_id == query_compact:
            score = max(score, 100.0)
        elif app_id.startswith(query_compact):
            score = max(score, 90.0)
        elif query_compact in app_id:
            score = max(score, 75.0)

    if app_id in hot_rank:
        score += max(0.0, 6.0 - hot_rank[app_id] * 0.05)

    return min(score, 100.0)


def _collect_numeric_candidates(query: str, allowed: list[str]) -> list[str]:
    if not query.isdigit():
        return []
    matches = [appid for appid in allowed if appid.startswith(query)]
    return matches[:MAX_NUMERIC_CANDIDATES]


def _local_fallback_candidates(query: str, allowed_appids: list[str]) -> list[dict]:
    """
    Fallback search path for environments where Steam storesearch endpoint is
    blocked/unreachable. We scan local catalog summaries in bounded batches.
    """
    if not allowed_appids:
        return []

    max_scan = min(len(allowed_appids), LOCAL_FALLBACK_MAX_SCAN)
    candidate_ids = allowed_appids[:max_scan]
    candidates: list[dict] = []

    for start in range(0, len(candidate_ids), LOCAL_FALLBACK_BATCH_SIZE):
        batch = candidate_ids[start : start + LOCAL_FALLBACK_BATCH_SIZE]
        if not batch:
            continue
        summaries = get_catalog_page(batch)
        if not summaries:
            continue
        for item in summaries:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            if score_candidate(query, item, {}) >= MIN_SCORE:
                candidates.append(item)

        if len(candidates) >= LOCAL_FALLBACK_TARGET_MATCHES:
            break

    return candidates


def search_catalog(
    query: str,
    allowed_appids: list[str],
    limit: int,
    offset: int,
    sort: Optional[str] = None,
) -> dict:
    trimmed = query.strip()
    if not trimmed:
        return {"total": 0, "items": []}

    sort_key = (sort or "relevance").lower()
    normalized = normalize_text(trimmed) or trimmed.lower()
    cache_key = f"steam:search:{normalized}:{sort_key}"
    cached = cache_client.get_json(cache_key)
    if cached and isinstance(cached, dict):
        return cached

    allowed_set = set(allowed_appids)
    candidates: dict[str, dict] = {}

    numeric_candidates = _collect_numeric_candidates(trimmed, allowed_appids)
    for appid in numeric_candidates:
        candidates[appid] = {"app_id": appid, "name": appid}

    for variant in build_search_variants(trimmed)[:SEARCH_VARIANT_LIMIT]:
        results = search_store(variant)
        for item in results:
            app_id = str(item.get("app_id") or "")
            if app_id and app_id in allowed_set:
                candidates[app_id] = item
        if len(candidates) >= MAX_CANDIDATES:
            break

    # Fallback: if upstream store search has no candidates (geo/network blocked),
    # do a bounded local scan of known appids.
    if not candidates and STEAM_SEARCH_ENABLE_LOCAL_FALLBACK:
        for item in _local_fallback_candidates(trimmed, allowed_appids):
            app_id = str(item.get("app_id") or "")
            if app_id and app_id in allowed_set:
                candidates[app_id] = item
            if len(candidates) >= MAX_CANDIDATES:
                break

    candidate_ids = list(candidates.keys())
    if candidate_ids:
        details = get_catalog_page(candidate_ids)
        for detail in details:
            app_id = str(detail.get("app_id") or "")
            if app_id:
                candidates[app_id] = detail

    hot_ids = get_hot_appids()
    hot_rank = {appid: index for index, appid in enumerate(hot_ids)}
    scored = []
    for app_id, item in candidates.items():
        score = score_candidate(trimmed, item, hot_rank)
        if score < MIN_SCORE:
            continue
        scored.append((score, hot_rank.get(app_id, 9999), item))

    if sort_key == "popular":
        scored.sort(key=lambda entry: (entry[1], -entry[0], entry[2].get("name") or ""))
    else:
        scored.sort(key=lambda entry: (-entry[0], entry[1], entry[2].get("name") or ""))
    items = [entry[2] for entry in scored]
    total = len(items)
    paged = items[offset : offset + limit]

    payload = {"total": total, "items": paged}
    cache_client.set_json(cache_key, payload, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return payload


def get_popular_catalog(limit: int, offset: int) -> dict:
    hot_ids = get_hot_appids()
    total = len(hot_ids)
    if not hot_ids:
        return {"total": 0, "items": []}
    page_ids = hot_ids[offset : offset + limit]
    items = get_catalog_page(page_ids)
    return {"total": total, "items": items}
