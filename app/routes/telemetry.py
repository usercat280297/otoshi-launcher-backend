from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import TelemetryEvent, User
from ..schemas import TelemetryEventIn, TelemetryEventOut
from .deps import get_current_user_optional

router = APIRouter()


@router.post("/events", response_model=List[TelemetryEventOut])
def ingest_events(
    events: List[TelemetryEventIn],
    db: Session = Depends(get_db),
    current_user: User | None = Depends(get_current_user_optional)
):
    stored: List[TelemetryEvent] = []
    for event in events:
        stored.append(
            TelemetryEvent(
                user_id=current_user.id if current_user else None,
                event_name=event.name,
                payload=event.payload
            )
        )
    db.add_all(stored)
    db.commit()
    for item in stored:
        db.refresh(item)
    return stored
