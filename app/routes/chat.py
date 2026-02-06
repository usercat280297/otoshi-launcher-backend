from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import ChatMessage, User
from ..schemas import ChatMessageIn, ChatMessageOut
from .deps import get_current_user

router = APIRouter()


@router.post("/send", response_model=ChatMessageOut)
def send_message(
    payload: ChatMessageIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if payload.recipient_id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot message yourself")

    message = ChatMessage(
        sender_id=current_user.id,
        recipient_id=payload.recipient_id,
        body=payload.body,
    )
    db.add(message)
    db.commit()
    db.refresh(message)
    return message


@router.get("/{recipient_id}", response_model=list[ChatMessageOut])
def list_messages(
    recipient_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(ChatMessage)
        .filter(
            ((ChatMessage.sender_id == current_user.id) & (ChatMessage.recipient_id == recipient_id))
            | ((ChatMessage.sender_id == recipient_id) & (ChatMessage.recipient_id == current_user.id))
        )
        .order_by(ChatMessage.created_at.desc())
        .limit(50)
        .all()
    )
