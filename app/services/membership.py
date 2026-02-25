from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

_VIP_ROLES = {"vip", "admin"}
_MEMBERSHIP_TIERS = {"vip", "supporter_plus", "supporter"}


def normalize_membership_tier(value: Any) -> str:
    return str(value or "").strip().lower()


def parse_membership_expiry(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        if isinstance(value, str):
            raw = value.strip()
            if raw.endswith("Z"):
                raw = f"{raw[:-1]}+00:00"
            parsed = datetime.fromisoformat(raw)
        elif isinstance(value, datetime):
            parsed = value
        else:
            return None
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def is_membership_active(expires_at: Any) -> bool:
    parsed = parse_membership_expiry(expires_at)
    if not parsed:
        return False
    return parsed >= datetime.now(timezone.utc)


def resolve_effective_membership_tier(user: Any) -> Optional[str]:
    if not user:
        return None

    role = normalize_membership_tier(getattr(user, "role", None))
    if role in _VIP_ROLES:
        return "vip"

    tier = normalize_membership_tier(getattr(user, "membership_tier", None))
    if tier in _MEMBERSHIP_TIERS and is_membership_active(
        getattr(user, "membership_expires_at", None)
    ):
        return tier

    return None
