from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
import logging

from ..core.config import (
    DOWNLOAD_BIG_GAME_THRESHOLD_BYTES,
    DOWNLOAD_SOURCE_POLICY_ENABLED,
    DOWNLOAD_SOURCE_POLICY_SCOPE,
)
from .huggingface import huggingface_fetcher

logger = logging.getLogger("download_source_policy")

_VIP_ROLES = {"vip", "admin"}
_VALID_SCOPES = {"vip_only", "all"}


@dataclass(frozen=True)
class SourcePolicyDecision:
    enabled: bool
    applied: bool
    prefer_hf_primary: bool
    is_vip: bool
    big_game: bool
    method: str
    game_size_bytes: Optional[int]
    threshold_bytes: int
    scope: str
    reason_codes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "applied": self.applied,
            "prefer_hf_primary": self.prefer_hf_primary,
            "is_vip": self.is_vip,
            "big_game": self.big_game,
            "method": self.method,
            "game_size_bytes": self.game_size_bytes,
            "threshold_bytes": self.threshold_bytes,
            "scope": self.scope,
            "reason_codes": self.reason_codes,
        }


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed < 0:
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _is_membership_active(user: Any) -> bool:
    expires_at = getattr(user, "membership_expires_at", None)
    if not expires_at:
        return False
    try:
        if isinstance(expires_at, str):
            raw = expires_at.strip()
            if raw.endswith("Z"):
                raw = f"{raw[:-1]}+00:00"
            expires_dt = datetime.fromisoformat(raw)
        elif isinstance(expires_at, datetime):
            expires_dt = expires_at
        else:
            return False
    except ValueError:
        return False

    if expires_dt.tzinfo is None:
        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    return expires_dt >= datetime.now(timezone.utc)


def is_vip_identity(user: Any) -> bool:
    if not user:
        return False

    role = str(getattr(user, "role", "") or "").strip().lower()
    if role in _VIP_ROLES:
        return True

    tier = str(getattr(user, "membership_tier", "") or "").strip().lower()
    if tier in {"vip", "supporter_plus", "supporter"} and _is_membership_active(user):
        return True

    return False


def decide_download_source_policy(
    *,
    game_size_bytes: Optional[int],
    is_vip: bool,
    method: Optional[str] = None,
    scope: Optional[str] = None,
    enabled: Optional[bool] = None,
    log_decision: bool = True,
) -> SourcePolicyDecision:
    normalized_scope = (scope or DOWNLOAD_SOURCE_POLICY_SCOPE or "vip_only").strip().lower()
    if normalized_scope not in _VALID_SCOPES:
        normalized_scope = "vip_only"

    policy_enabled = DOWNLOAD_SOURCE_POLICY_ENABLED if enabled is None else bool(enabled)
    threshold_bytes = max(1, int(DOWNLOAD_BIG_GAME_THRESHOLD_BYTES or 0))
    normalized_size = _safe_int(game_size_bytes)
    big_game = normalized_size is not None and normalized_size >= threshold_bytes
    normalized_method = str(method or "").strip().lower() or "auto"

    reasons: list[str] = []
    if policy_enabled:
        reasons.append("policy_enabled")
    else:
        reasons.append("policy_disabled")
    reasons.append(f"scope_{normalized_scope}")
    reasons.append("vip_identity" if is_vip else "non_vip_identity")
    reasons.append("big_game" if big_game else "small_game_or_unknown")
    reasons.append(f"method_{normalized_method}")

    apply_policy = False
    method_allows_hf = normalized_method not in {"cdn", "cdn_direct"}
    if policy_enabled and big_game and method_allows_hf:
        if normalized_scope == "all":
            apply_policy = True
        elif normalized_scope == "vip_only" and is_vip:
            apply_policy = True

    if apply_policy:
        reasons.append("hf_first_applied")
    elif not method_allows_hf:
        reasons.append("method_forces_cdn")
    else:
        reasons.append("default_origin_order")

    decision = SourcePolicyDecision(
        enabled=policy_enabled,
        applied=apply_policy,
        prefer_hf_primary=apply_policy,
        is_vip=is_vip,
        big_game=big_game,
        method=normalized_method,
        game_size_bytes=normalized_size,
        threshold_bytes=threshold_bytes,
        scope=normalized_scope,
        reason_codes=reasons,
    )

    if log_decision:
        logger.info(
            "download_source_policy enabled=%s applied=%s scope=%s vip=%s big_game=%s method=%s size=%s threshold=%s reasons=%s",
            decision.enabled,
            decision.applied,
            decision.scope,
            decision.is_vip,
            decision.big_game,
            decision.method,
            decision.game_size_bytes,
            decision.threshold_bytes,
            ",".join(decision.reason_codes),
        )
    return decision


def _dedupe_urls(urls: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def apply_manifest_source_policy(
    manifest: dict[str, Any],
    *,
    decision: SourcePolicyDecision,
) -> tuple[dict[str, Any], int]:
    payload = deepcopy(manifest)
    if not decision.prefer_hf_primary:
        payload["source_policy"] = {
            **decision.to_dict(),
            "reordered_chunks": 0,
        }
        return payload, 0

    files = payload.get("files")
    if not isinstance(files, list):
        payload["source_policy"] = {
            **decision.to_dict(),
            "reordered_chunks": 0,
        }
        return payload, 0

    reordered = 0
    for file_entry in files:
        if not isinstance(file_entry, dict):
            continue
        source_path = str(file_entry.get("source_path") or "").strip()
        if not source_path:
            continue

        hf_url = huggingface_fetcher.resolve_file_url(source_path)
        if not hf_url:
            continue

        chunks = file_entry.get("chunks")
        if not isinstance(chunks, list):
            continue

        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            current_primary = str(chunk.get("url") or "").strip()
            current_fallbacks_raw = chunk.get("fallback_urls") or []
            current_fallbacks = [
                str(item).strip() for item in current_fallbacks_raw if str(item or "").strip()
            ]
            new_fallbacks = _dedupe_urls([current_primary, *current_fallbacks])
            chunk["url"] = hf_url
            chunk["fallback_urls"] = new_fallbacks
            reordered += 1

    payload["source_policy"] = {
        **decision.to_dict(),
        "reordered_chunks": reordered,
    }
    return payload, reordered
