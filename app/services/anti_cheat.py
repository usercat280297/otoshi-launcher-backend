from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from ..models import AntiCheatCase, AntiCheatSignal

_SIGNAL_WEIGHTS = {
    "integrity_mismatch": 2.6,
    "suspicious_process_tree": 2.2,
    "abnormal_runtime_pattern": 1.8,
    "tamper_attempt": 3.0,
    "repeated_tamper": 3.2,
    "driver_conflict": 1.2,
}


def _normalize_signal_type(signal_type: str) -> str:
    normalized = str(signal_type or "").strip().lower().replace(" ", "_").replace("-", "_")
    return normalized or "unknown"


def _risk_level(score: float) -> str:
    if score >= 75:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def _recommended_action(risk_level: str) -> str:
    if risk_level == "high":
        return "manual_review"
    if risk_level == "medium":
        return "monitor"
    return "allow"


def _reason_codes(
    *,
    signal_type: str,
    severity: int,
    recent_count: int,
    risk_level: str,
) -> list[str]:
    reasons: list[str] = [f"signal:{signal_type}", f"severity:{severity}"]
    if recent_count >= 3:
        reasons.append("repeat_pattern")
    if risk_level == "high":
        reasons.append("escalated_review")
    return reasons


def ingest_signal(
    db: Session,
    *,
    user_id: Optional[str],
    device_id: str,
    signal_type: str,
    severity: int,
    observed_at: Optional[datetime],
    payload: dict,
) -> tuple[AntiCheatSignal, AntiCheatCase]:
    normalized_type = _normalize_signal_type(signal_type)
    now = observed_at or datetime.utcnow()
    severity_value = max(1, min(10, int(severity or 1)))
    signal = AntiCheatSignal(
        user_id=user_id,
        device_id=device_id,
        signal_type=normalized_type,
        severity=severity_value,
        observed_at=now,
        payload=payload or {},
    )
    db.add(signal)
    db.flush()

    case_query = db.query(AntiCheatCase).filter(
        AntiCheatCase.device_id == device_id,
        AntiCheatCase.status == "open",
    )
    if user_id:
        case_query = case_query.filter(AntiCheatCase.user_id == user_id)
    case = case_query.order_by(AntiCheatCase.updated_at.desc()).first()
    if case is None:
        case = AntiCheatCase(
            user_id=user_id,
            device_id=device_id,
            status="open",
            risk_score=0.0,
            risk_level="low",
            reason_codes=[],
            recommended_action="allow",
            latest_signal_at=now,
            payload={},
        )
        db.add(case)
        db.flush()

    recent_window = datetime.utcnow() - timedelta(hours=24)
    recent_count = (
        db.query(AntiCheatSignal)
        .filter(
            AntiCheatSignal.device_id == device_id,
            AntiCheatSignal.signal_type == normalized_type,
            AntiCheatSignal.observed_at >= recent_window,
        )
        .count()
    )

    weight = _SIGNAL_WEIGHTS.get(normalized_type, 1.5)
    increment = (severity_value * weight) + max(0, recent_count - 1) * 2.0
    decayed = float(case.risk_score or 0.0) * 0.65
    risk_score = max(0.0, min(100.0, decayed + increment))
    risk_level = _risk_level(risk_score)
    action = _recommended_action(risk_level)
    reason_codes = _reason_codes(
        signal_type=normalized_type,
        severity=severity_value,
        recent_count=recent_count,
        risk_level=risk_level,
    )

    case.risk_score = risk_score
    case.risk_level = risk_level
    case.recommended_action = action
    case.reason_codes = reason_codes
    case.latest_signal_at = now
    case.updated_at = datetime.utcnow()
    merged_payload = dict(case.payload or {})
    merged_payload["last_signal_type"] = normalized_type
    merged_payload["last_signal_id"] = signal.id
    merged_payload["recent_signal_count_24h"] = recent_count
    case.payload = merged_payload
    db.flush()
    return signal, case


def list_cases(
    db: Session,
    *,
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    device_id: Optional[str] = None,
) -> tuple[int, list[AntiCheatCase]]:
    query = db.query(AntiCheatCase)
    if status:
        query = query.filter(AntiCheatCase.status == status)
    if user_id:
        query = query.filter(AntiCheatCase.user_id == user_id)
    if device_id:
        query = query.filter(AntiCheatCase.device_id == device_id)

    total = query.count()
    rows = (
        query.order_by(AntiCheatCase.updated_at.desc())
        .offset(max(0, int(offset)))
        .limit(max(1, min(200, int(limit))))
        .all()
    )
    return total, rows
