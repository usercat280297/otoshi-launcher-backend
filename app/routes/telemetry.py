from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import TelemetryEvent, User, UserBehaviorEvent
from ..schemas import TelemetryEventIn, TelemetryEventOut
from ..services.privacy import is_consent_granted
from .deps import get_current_user_optional

router = APIRouter()


@router.post("/events", response_model=List[TelemetryEventOut])
def ingest_events(
    events: List[TelemetryEventIn],
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional)
):
    user_id = current_user.id if current_user else None
    if not is_consent_granted(db, user_id, "telemetry"):
        return []

    stored: List[TelemetryEvent] = []
    behavior_rows: List[UserBehaviorEvent] = []
    for event in events:
        stored.append(
            TelemetryEvent(
                user_id=user_id,
                event_name=event.name,
                payload=event.payload
            )
        )
        behavior_rows.append(
            UserBehaviorEvent(
                user_id=user_id,
                event_type=f"telemetry.{event.name}",
                source="telemetry.route",
                payload=event.payload or {},
            )
        )
    db.add_all(stored)
    db.add_all(behavior_rows)
    db.commit()
    for item in stored:
        db.refresh(item)
    return stored
