from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from ..core.config import (
    ADMIN_EMAILS,
    ADMIN_OAUTH_IDS,
    ADMIN_USER_IDS,
    ADMIN_USERNAMES,
)
from ..models import User


def _normalize(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def is_admin_identity(
    user: User,
    provider: Optional[str] = None,
    provider_user_id: Optional[str] = None,
) -> bool:
    if user.role == "admin":
        return True

    user_id = _normalize(user.id)
    if user_id and user_id in ADMIN_USER_IDS:
        return True

    email = _normalize(user.email)
    if email and email in ADMIN_EMAILS:
        return True

    username = _normalize(user.username)
    if username and username in ADMIN_USERNAMES:
        return True

    if provider and provider_user_id:
        oauth_key = f"{provider}:{provider_user_id}".lower()
        if oauth_key in ADMIN_OAUTH_IDS:
            return True

    return False


def ensure_admin_role(
    db: Session,
    user: User,
    provider: Optional[str] = None,
    provider_user_id: Optional[str] = None,
) -> bool:
    if not is_admin_identity(user, provider, provider_user_id):
        return False
    if user.role != "admin":
        user.role = "admin"
        db.commit()
        db.refresh(user)
    return True
