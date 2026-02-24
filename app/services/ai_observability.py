from __future__ import annotations

import re
import threading
import time
from collections import deque
from typing import Any

_LOCK = threading.Lock()
_STARTED_AT = time.time()
_LATENCY_WINDOW = 600
_MAX_ENDPOINT_KEYS = 256
_MAX_PROVIDER_KEYS = 128
_MAX_QUALITY_KEYS = 256
_UUID_SEGMENT_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_NUMERIC_SEGMENT_RE = re.compile(r"^\d+$")
_HEXISH_SEGMENT_RE = re.compile(r"^[0-9a-f]{16,}$", re.IGNORECASE)

_endpoint_metrics: dict[str, dict[str, Any]] = {}
_provider_metrics: dict[str, dict[str, Any]] = {}
_quality_counters: dict[str, float] = {}


def _normalize_path(path: str) -> str:
    normalized = str(path or "").strip()
    if not normalized.startswith("/"):
        normalized = f"/{normalized}" if normalized else "/"
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return "/"
    sanitized: list[str] = []
    for part in parts:
        lower = part.lower()
        if (
            _NUMERIC_SEGMENT_RE.match(lower)
            or _UUID_SEGMENT_RE.match(lower)
            or _HEXISH_SEGMENT_RE.match(lower)
        ):
            sanitized.append(":id")
            continue
        sanitized.append(part)
    return "/" + "/".join(sanitized)


def _endpoint_key(path: str, query_params: dict[str, Any] | None = None) -> str:
    normalized = _normalize_path(path)
    params = query_params or {}
    if normalized == "/steam/catalog":
        mode = str(params.get("search_mode") or params.get("searchMode") or "auto").strip().lower()
        if mode not in {"lexical", "hybrid", "semantic", "auto"}:
            mode = "other"
        return f"/steam/catalog:{mode or 'auto'}"
    return normalized


def should_track_request(path: str) -> bool:
    normalized = str(path or "").strip()
    if not normalized:
        return False
    if normalized.startswith("/ai/"):
        return True
    if normalized.startswith("/privacy/"):
        return True
    if normalized.startswith("/discovery/recommendations/v2"):
        return True
    if normalized.startswith("/steam/catalog"):
        return True
    return False


def _get_endpoint_row(key: str) -> dict[str, Any]:
    if key not in _endpoint_metrics and len(_endpoint_metrics) >= _MAX_ENDPOINT_KEYS:
        key = "__other__"
    row = _endpoint_metrics.get(key)
    if row is None:
        row = {
            "requests": 0,
            "success": 0,
            "errors": 0,
            "latency_ms_sum": 0.0,
            "latency_ms_max": 0.0,
            "latency_ms_window": deque(maxlen=_LATENCY_WINDOW),
            "total_cost_usd": 0.0,
            "failover_count": 0,
            "last_status": 0,
            "last_seen_at": None,
        }
        _endpoint_metrics[key] = row
    return row


def _get_provider_row(provider: str) -> dict[str, Any]:
    key = str(provider or "unknown").strip().lower() or "unknown"
    if key not in _provider_metrics and len(_provider_metrics) >= _MAX_PROVIDER_KEYS:
        key = "other"
    row = _provider_metrics.get(key)
    if row is None:
        row = {
            "requests": 0,
            "success": 0,
            "errors": 0,
            "cached_hits": 0,
            "failovers": 0,
            "total_cost_usd": 0.0,
            "last_model": None,
            "last_reason": None,
            "last_seen_at": None,
        }
        _provider_metrics[key] = row
    return row


def record_http_request(
    *,
    path: str,
    method: str,
    status_code: int,
    latency_ms: float,
    query_params: dict[str, Any] | None = None,
) -> None:
    key = _endpoint_key(path, query_params)
    now = time.time()
    with _LOCK:
        row = _get_endpoint_row(key)
        row["requests"] += 1
        row["latency_ms_sum"] += max(0.0, float(latency_ms or 0.0))
        row["latency_ms_max"] = max(float(row["latency_ms_max"] or 0.0), max(0.0, float(latency_ms or 0.0)))
        row["latency_ms_window"].append(max(0.0, float(latency_ms or 0.0)))
        row["last_status"] = int(status_code or 0)
        row["last_seen_at"] = now
        if 200 <= int(status_code or 0) < 400:
            row["success"] += 1
        else:
            row["errors"] += 1


def record_gateway_provider_result(
    *,
    provider: str,
    model: str,
    success: bool,
    cached: bool,
    failover: bool,
    cost_estimate_usd: float = 0.0,
    reason: str | None = None,
) -> None:
    now = time.time()
    with _LOCK:
        provider_row = _get_provider_row(provider)
        provider_row["requests"] += 1
        provider_row["total_cost_usd"] += max(0.0, float(cost_estimate_usd or 0.0))
        provider_row["last_model"] = model
        provider_row["last_reason"] = reason
        provider_row["last_seen_at"] = now
        if cached:
            provider_row["cached_hits"] += 1
        if failover:
            provider_row["failovers"] += 1
        if success:
            provider_row["success"] += 1
        else:
            provider_row["errors"] += 1

        # Fold provider cost/failover into aggregate AI endpoint row.
        endpoint_row = _get_endpoint_row("/ai/gateway")
        endpoint_row["total_cost_usd"] += max(0.0, float(cost_estimate_usd or 0.0))
        if failover:
            endpoint_row["failover_count"] += 1
        endpoint_row["last_seen_at"] = now


def record_quality_event(name: str, value: float = 1.0) -> None:
    key = str(name or "").strip().lower()
    if not key:
        return
    with _LOCK:
        if key not in _quality_counters and len(_quality_counters) >= _MAX_QUALITY_KEYS:
            key = "other"
        _quality_counters[key] = float(_quality_counters.get(key, 0.0)) + float(value or 0.0)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(round((max(0.0, min(100.0, pct)) / 100.0) * (len(ordered) - 1)))
    return float(ordered[index])


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def metrics_snapshot() -> dict[str, Any]:
    with _LOCK:
        endpoint_rows = {}
        for key, row in _endpoint_metrics.items():
            requests = float(row["requests"] or 0)
            latency_values = list(row["latency_ms_window"])
            endpoint_rows[key] = {
                "requests": int(row["requests"] or 0),
                "success": int(row["success"] or 0),
                "errors": int(row["errors"] or 0),
                "error_rate": round(_safe_ratio(float(row["errors"] or 0), requests), 6),
                "latency_ms_avg": round(_safe_ratio(float(row["latency_ms_sum"] or 0.0), requests), 3),
                "latency_ms_p95": round(_percentile(latency_values, 95.0), 3),
                "latency_ms_max": round(float(row["latency_ms_max"] or 0.0), 3),
                "total_cost_usd": round(float(row["total_cost_usd"] or 0.0), 6),
                "failover_count": int(row["failover_count"] or 0),
                "last_status": int(row["last_status"] or 0),
                "last_seen_at": row["last_seen_at"],
            }

        provider_rows = {}
        for key, row in _provider_metrics.items():
            requests = float(row["requests"] or 0)
            provider_rows[key] = {
                "requests": int(row["requests"] or 0),
                "success": int(row["success"] or 0),
                "errors": int(row["errors"] or 0),
                "error_rate": round(_safe_ratio(float(row["errors"] or 0), requests), 6),
                "cached_hits": int(row["cached_hits"] or 0),
                "cache_hit_rate": round(_safe_ratio(float(row["cached_hits"] or 0), requests), 6),
                "failovers": int(row["failovers"] or 0),
                "failover_rate": round(_safe_ratio(float(row["failovers"] or 0), requests), 6),
                "total_cost_usd": round(float(row["total_cost_usd"] or 0.0), 6),
                "last_model": row["last_model"],
                "last_reason": row["last_reason"],
                "last_seen_at": row["last_seen_at"],
            }

        quality = dict(_quality_counters)

    search_submit = quality.get("search.submit", 0.0)
    search_click = (
        quality.get("search.click", 0.0)
        + quality.get("search.open", 0.0)
        + quality.get("search.detail", 0.0)
    )
    reco_impressions = quality.get("reco.impression", 0.0)
    reco_positive = (
        quality.get("reco.feedback.click", 0.0)
        + quality.get("reco.feedback.play", 0.0)
        + quality.get("reco.feedback.install", 0.0)
        + quality.get("reco.feedback.open", 0.0)
        + quality.get("reco.feedback.liked", 0.0)
    )
    support_suggestions = quality.get("support.suggestion", 0.0)
    support_cached = quality.get("support.cached", 0.0)
    anti_cheat_cases = quality.get("anti_cheat.case", 0.0)
    anti_cheat_reasoned = quality.get("anti_cheat.case_with_reasons", 0.0)

    return {
        "started_at": _STARTED_AT,
        "uptime_seconds": int(max(0, time.time() - _STARTED_AT)),
        "endpoints": endpoint_rows,
        "providers": provider_rows,
        "quality_kpis": {
            "search_submit_count": int(search_submit),
            "search_click_like_count": int(search_click),
            "search_ctr": round(_safe_ratio(search_click, search_submit), 6),
            "reco_impression_count": int(reco_impressions),
            "reco_positive_feedback_count": int(reco_positive),
            "reco_ctr": round(_safe_ratio(reco_positive, reco_impressions), 6),
            "support_suggestion_count": int(support_suggestions),
            "support_cache_hit_rate": round(_safe_ratio(support_cached, support_suggestions), 6),
            "anti_cheat_case_count": int(anti_cheat_cases),
            "anti_cheat_reason_coverage": round(_safe_ratio(anti_cheat_reasoned, anti_cheat_cases), 6),
        },
        "quality_counters": quality,
    }
