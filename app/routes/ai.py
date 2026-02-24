from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..core.config import (
    AI_SEARCH_EVENTS_MAX_BATCH,
    AI_FEATURE_ANTI_CHEAT_RISK,
    AI_FEATURE_SUPPORT_COPILOT,
)
from ..db import get_db
from ..models import SearchInteraction, User, UserBehaviorEvent
from ..schemas import (
    AISearchEventIn,
    AISearchEventOut,
    AntiCheatCaseOut,
    AntiCheatSignalIn,
    AntiCheatSignalOut,
    RecommendationFeedbackIn,
    RecommendationImpressionIn,
    RecommendationTrackingOut,
    SupportSuggestIn,
    SupportSuggestOut,
)
from ..services.ai_recommendations import (
    create_recommendation_feedback,
    create_recommendation_impression,
)
from ..services.ai_observability import metrics_snapshot, record_quality_event
from ..services.anti_cheat import ingest_signal, list_cases
from ..services.privacy import is_consent_granted
from ..services.support_copilot import suggest_response
from .deps import get_current_user_optional, require_admin_access

router = APIRouter()

_ALLOWED_SEARCH_ACTIONS = {
    "submit",
    "click",
    "open",
    "detail",
    "view",
    "type",
}

_ALLOWED_FEEDBACK_TYPES = {
    "click",
    "play",
    "install",
    "open",
    "liked",
    "dislike",
    "dismiss",
    "skip",
}


def _normalize_search_action(value: str | None) -> str:
    normalized = str(value or "submit").strip().lower() or "submit"
    if normalized in _ALLOWED_SEARCH_ACTIONS:
        return normalized
    return "other"


def _normalize_feedback_type(value: str | None) -> str:
    normalized = str(value or "unknown").strip().lower() or "unknown"
    if normalized in _ALLOWED_FEEDBACK_TYPES:
        return normalized
    return "other"


@router.post("/search/events", response_model=AISearchEventOut)
def ingest_search_events(
    events: list[AISearchEventIn],
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional),
):
    max_batch = max(1, int(AI_SEARCH_EVENTS_MAX_BATCH or 1))
    if len(events) > max_batch:
        record_quality_event("search.batch_rejected")
        raise HTTPException(
            status_code=413,
            detail=f"Too many events in one batch. Max {max_batch}",
        )
    user_id = current_user.id if current_user else None
    if not is_consent_granted(db, user_id, "search"):
        return AISearchEventOut(stored=0, skipped=len(events), consent_required=True)

    stored_rows: list[SearchInteraction] = []
    behavior_rows: list[UserBehaviorEvent] = []
    for event in events:
        action = _normalize_search_action(event.action)
        record_quality_event(f"search.{action}")
        if int(event.dwell_ms or 0) > 0:
            record_quality_event("search.dwell_ms_total", float(event.dwell_ms or 0))
        interaction = SearchInteraction(
            user_id=user_id,
            query=event.query,
            action=action,
            app_id=event.app_id,
            dwell_ms=event.dwell_ms,
            payload=event.payload or {},
        )
        stored_rows.append(interaction)
        behavior_rows.append(
            UserBehaviorEvent(
                user_id=user_id,
                event_type=f"search_{action}",
                source="ai.search.events",
                payload={
                    "query": event.query,
                    "app_id": event.app_id,
                    "dwell_ms": event.dwell_ms,
                    **(event.payload or {}),
                },
            )
        )
    db.add_all(stored_rows)
    db.add_all(behavior_rows)
    db.commit()
    return AISearchEventOut(stored=len(stored_rows), skipped=0, consent_required=False)


@router.post("/recommendations/impression", response_model=RecommendationTrackingOut)
def recommendation_impression(
    payload: RecommendationImpressionIn,
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional),
):
    user_id = current_user.id if current_user else None
    if not is_consent_granted(db, user_id, "recommendation"):
        raise HTTPException(status_code=403, detail="Recommendation consent not granted")
    row = create_recommendation_impression(
        db,
        user_id=user_id,
        game_id=payload.game_id,
        app_id=payload.app_id,
        recommendation_id=payload.recommendation_id,
        rank_position=payload.rank_position,
        algorithm_version=payload.algorithm_version,
        context=payload.context,
        payload=payload.payload,
    )
    db.commit()
    db.refresh(row)
    record_quality_event("reco.impression")
    return RecommendationTrackingOut(id=row.id, created_at=row.created_at)


@router.post("/recommendations/feedback", response_model=RecommendationTrackingOut)
def recommendation_feedback(
    payload: RecommendationFeedbackIn,
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional),
):
    user_id = current_user.id if current_user else None
    if not is_consent_granted(db, user_id, "recommendation"):
        raise HTTPException(status_code=403, detail="Recommendation consent not granted")
    row = create_recommendation_feedback(
        db,
        user_id=user_id,
        impression_id=payload.impression_id,
        game_id=payload.game_id,
        app_id=payload.app_id,
        feedback_type=payload.feedback_type,
        value=payload.value,
        payload=payload.payload,
    )
    db.commit()
    db.refresh(row)
    feedback_type = _normalize_feedback_type(payload.feedback_type)
    record_quality_event(f"reco.feedback.{feedback_type}")
    return RecommendationTrackingOut(id=row.id, created_at=row.created_at)


@router.post("/support/suggest", response_model=SupportSuggestOut)
def support_suggest(
    payload: SupportSuggestIn,
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional),
):
    if not AI_FEATURE_SUPPORT_COPILOT:
        raise HTTPException(status_code=503, detail="Support copilot disabled")
    user_id = current_user.id if current_user else None
    if not is_consent_granted(db, user_id, "support"):
        raise HTTPException(status_code=403, detail="Support consent not granted")

    session, suggestion, reason_codes = suggest_response(
        db,
        user_id=user_id,
        session_id=payload.session_id,
        topic=payload.topic,
        message=payload.message,
        context=payload.context or {},
        preferred_provider=payload.preferred_provider,
        preferred_model=payload.preferred_model,
    )
    db.commit()
    db.refresh(suggestion)
    record_quality_event("support.suggestion")
    if bool(suggestion.cached):
        record_quality_event("support.cached")
    confidence = float(suggestion.confidence or 0.0)
    return SupportSuggestOut(
        session_id=session.id,
        suggestion_id=suggestion.id,
        provider=suggestion.provider,
        model=suggestion.model,
        cached=bool(suggestion.cached),
        confidence=confidence,
        suggestion=suggestion.suggestion_text,
        reason_codes=reason_codes,
    )


@router.post("/anti-cheat/signals", response_model=AntiCheatSignalOut)
def anti_cheat_signals(
    payload: AntiCheatSignalIn,
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional),
):
    if not AI_FEATURE_ANTI_CHEAT_RISK:
        raise HTTPException(status_code=503, detail="Anti-cheat risk module disabled")
    user_id = current_user.id if current_user else None
    if not is_consent_granted(db, user_id, "anti_cheat"):
        raise HTTPException(status_code=403, detail="Anti-cheat consent not granted")

    signal, case = ingest_signal(
        db,
        user_id=user_id,
        device_id=payload.device_id,
        signal_type=payload.signal_type,
        severity=payload.severity,
        observed_at=payload.observed_at,
        payload=payload.payload or {},
    )
    db.commit()
    db.refresh(signal)
    db.refresh(case)
    record_quality_event("anti_cheat.case")
    if list(case.reason_codes or []):
        record_quality_event("anti_cheat.case_with_reasons")
    return AntiCheatSignalOut(
        signal_id=signal.id,
        case_id=case.id,
        risk_score=float(case.risk_score or 0.0),
        risk_level=case.risk_level or "low",
        recommended_action=case.recommended_action or "monitor",
        reason_codes=list(case.reason_codes or []),
    )


@router.get("/metrics")
def ai_metrics(
    _admin: User | None = Depends(require_admin_access),
):
    return metrics_snapshot()


@router.get("/anti-cheat/cases", response_model=list[AntiCheatCaseOut])
def anti_cheat_cases(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: str | None = Query(None),
    user_id: str | None = Query(None),
    device_id: str | None = Query(None),
    db: Session = Depends(get_db),
    _admin: User | None = Depends(require_admin_access),
):
    _, rows = list_cases(
        db,
        limit=limit,
        offset=offset,
        status=status,
        user_id=user_id,
        device_id=device_id,
    )
    return rows
