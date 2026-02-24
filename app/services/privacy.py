from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy.orm import Session

from ..core.config import AI_PRIVACY_DEFAULT_DENY
from ..models import (
    AntiCheatCase,
    AntiCheatSignal,
    PrivacyConsent,
    PrivacyDeletionRequest,
    RecommendationFeedback,
    RecommendationImpression,
    SearchInteraction,
    SupportSession,
    SupportSuggestion,
    TelemetryEvent,
    UserBehaviorEvent,
)

PRIVACY_CATEGORIES = {
    "telemetry",
    "search",
    "recommendation",
    "support",
    "anti_cheat",
}


def normalize_privacy_category(value: str) -> str:
    cleaned = str(value or "").strip().lower()
    if cleaned in {"anticheat", "anti-cheat"}:
        return "anti_cheat"
    if cleaned in {"reco", "recommendations"}:
        return "recommendation"
    return cleaned


def normalize_privacy_categories(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        category = normalize_privacy_category(value)
        if not category or category not in PRIVACY_CATEGORIES:
            continue
        if category not in normalized:
            normalized.append(category)
    return normalized


def get_user_consent_map(db: Session, user_id: str) -> dict[str, bool]:
    rows = (
        db.query(PrivacyConsent)
        .filter(PrivacyConsent.user_id == user_id)
        .all()
    )
    consent_map: dict[str, bool] = {}
    for row in rows:
        category = normalize_privacy_category(row.category)
        if category:
            consent_map[category] = bool(row.granted)
    return consent_map


def is_consent_granted(db: Session, user_id: str | None, category: str) -> bool:
    normalized = normalize_privacy_category(category)
    if not user_id:
        return not AI_PRIVACY_DEFAULT_DENY
    if normalized not in PRIVACY_CATEGORIES:
        return False
    row = (
        db.query(PrivacyConsent)
        .filter(
            PrivacyConsent.user_id == user_id,
            PrivacyConsent.category == normalized,
        )
        .first()
    )
    if row is None:
        return not AI_PRIVACY_DEFAULT_DENY
    return bool(row.granted)


def upsert_consent(
    db: Session,
    *,
    user_id: str,
    category: str,
    granted: bool,
    source: str = "settings",
    payload: dict | None = None,
) -> PrivacyConsent:
    normalized = normalize_privacy_category(category)
    if normalized not in PRIVACY_CATEGORIES:
        raise ValueError(f"Unsupported privacy category: {category}")

    row = (
        db.query(PrivacyConsent)
        .filter(
            PrivacyConsent.user_id == user_id,
            PrivacyConsent.category == normalized,
        )
        .first()
    )
    if row is None:
        row = PrivacyConsent(
            user_id=user_id,
            category=normalized,
            granted=bool(granted),
            source=source or "settings",
            payload=payload or {},
        )
        db.add(row)
    else:
        row.granted = bool(granted)
        row.source = source or row.source or "settings"
        row.payload = payload or {}
        row.updated_at = datetime.utcnow()
    db.flush()
    return row


def export_user_ai_data(db: Session, user_id: str) -> dict:
    consents = (
        db.query(PrivacyConsent)
        .filter(PrivacyConsent.user_id == user_id)
        .order_by(PrivacyConsent.category.asc())
        .all()
    )
    telemetry = (
        db.query(TelemetryEvent)
        .filter(TelemetryEvent.user_id == user_id)
        .order_by(TelemetryEvent.created_at.desc())
        .all()
    )
    behavior_events = (
        db.query(UserBehaviorEvent)
        .filter(UserBehaviorEvent.user_id == user_id)
        .order_by(UserBehaviorEvent.created_at.desc())
        .all()
    )
    search_interactions = (
        db.query(SearchInteraction)
        .filter(SearchInteraction.user_id == user_id)
        .order_by(SearchInteraction.created_at.desc())
        .all()
    )
    impressions = (
        db.query(RecommendationImpression)
        .filter(RecommendationImpression.user_id == user_id)
        .order_by(RecommendationImpression.created_at.desc())
        .all()
    )
    feedback = (
        db.query(RecommendationFeedback)
        .filter(RecommendationFeedback.user_id == user_id)
        .order_by(RecommendationFeedback.created_at.desc())
        .all()
    )
    anti_cheat_signals = (
        db.query(AntiCheatSignal)
        .filter(AntiCheatSignal.user_id == user_id)
        .order_by(AntiCheatSignal.created_at.desc())
        .all()
    )
    anti_cheat_cases = (
        db.query(AntiCheatCase)
        .filter(AntiCheatCase.user_id == user_id)
        .order_by(AntiCheatCase.updated_at.desc())
        .all()
    )
    support_sessions = (
        db.query(SupportSession)
        .filter(SupportSession.user_id == user_id)
        .order_by(SupportSession.updated_at.desc())
        .all()
    )
    support_suggestions = (
        db.query(SupportSuggestion)
        .filter(SupportSuggestion.user_id == user_id)
        .order_by(SupportSuggestion.created_at.desc())
        .all()
    )

    def _to_dict(row) -> dict:
        if row is None:
            return {}
        data = {}
        for key in row.__table__.columns.keys():
            data[key] = getattr(row, key)
        return data

    return {
        "user_id": user_id,
        "consents": consents,
        "telemetry_events": telemetry,
        "behavior_events": [_to_dict(item) for item in behavior_events],
        "search_interactions": [_to_dict(item) for item in search_interactions],
        "recommendation_impressions": [_to_dict(item) for item in impressions],
        "recommendation_feedback": [_to_dict(item) for item in feedback],
        "anti_cheat_signals": [_to_dict(item) for item in anti_cheat_signals],
        "anti_cheat_cases": [_to_dict(item) for item in anti_cheat_cases],
        "support_sessions": [_to_dict(item) for item in support_sessions],
        "support_suggestions": [_to_dict(item) for item in support_suggestions],
    }


def request_delete_user_ai_data(db: Session, user_id: str, scope: list[str] | None) -> PrivacyDeletionRequest:
    categories = normalize_privacy_categories(scope or []) or sorted(PRIVACY_CATEGORIES)
    row = PrivacyDeletionRequest(
        user_id=user_id,
        scope=categories,
        status="processing",
        requested_at=datetime.utcnow(),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()
    return row


def delete_user_ai_data(db: Session, user_id: str, scope: list[str] | None = None) -> tuple[PrivacyDeletionRequest, dict]:
    row = request_delete_user_ai_data(db, user_id, scope)
    categories = set(row.scope or [])
    deleted_counts: dict[str, int] = {}

    def _delete(model, key: str) -> None:
        deleted = db.query(model).filter(model.user_id == user_id).delete(synchronize_session=False)
        deleted_counts[key] = int(deleted or 0)

    if "telemetry" in categories:
        _delete(TelemetryEvent, "telemetry_events")
        _delete(UserBehaviorEvent, "behavior_events")
    if "search" in categories:
        _delete(SearchInteraction, "search_interactions")
    if "recommendation" in categories:
        _delete(RecommendationFeedback, "recommendation_feedback")
        _delete(RecommendationImpression, "recommendation_impressions")
    if "anti_cheat" in categories:
        _delete(AntiCheatSignal, "anti_cheat_signals")
        _delete(AntiCheatCase, "anti_cheat_cases")
    if "support" in categories:
        _delete(SupportSuggestion, "support_suggestions")
        _delete(SupportSession, "support_sessions")

    row.status = "completed"
    row.processed_at = datetime.utcnow()
    row.result_payload = deleted_counts
    row.updated_at = datetime.utcnow()
    db.flush()
    return row, deleted_counts
