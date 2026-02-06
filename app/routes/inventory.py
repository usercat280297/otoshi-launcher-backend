from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import (
    BadgeDefinition,
    InventoryItem,
    TradeOffer,
    TradingCardDefinition,
    User,
)
from ..schemas import (
    BadgeOut,
    InventoryGrantIn,
    InventoryItemOut,
    TradeOfferIn,
    TradeOfferOut,
    TradingCardOut,
)
from .deps import get_current_user

router = APIRouter()


@router.get("/", response_model=List[InventoryItemOut])
def list_inventory(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(InventoryItem)
        .filter(InventoryItem.user_id == current_user.id)
        .order_by(InventoryItem.created_at.desc())
        .all()
    )


@router.post("/grant", response_model=InventoryItemOut)
def grant_item(
    payload: InventoryGrantIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    item = InventoryItem(
        user_id=current_user.id,
        game_id=payload.game_id,
        item_type=payload.item_type,
        name=payload.name,
        rarity=payload.rarity or "common",
        quantity=max(1, payload.quantity),
        item_metadata=payload.item_metadata,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.get("/cards/{game_id}", response_model=List[TradingCardOut])
def list_cards(game_id: str, db: Session = Depends(get_db)):
    return (
        db.query(TradingCardDefinition)
        .filter(TradingCardDefinition.game_id == game_id)
        .all()
    )


@router.get("/badges/{game_id}", response_model=List[BadgeOut])
def list_badges(game_id: str, db: Session = Depends(get_db)):
    return (
        db.query(BadgeDefinition)
        .filter(BadgeDefinition.game_id == game_id)
        .all()
    )


@router.post("/cards/drop/{game_id}", response_model=InventoryItemOut)
def card_drop(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    cards = (
        db.query(TradingCardDefinition)
        .filter(TradingCardDefinition.game_id == game_id)
        .all()
    )
    if not cards:
        cards = [
            TradingCardDefinition(game_id=game_id, card_name="Collector", rarity="common"),
            TradingCardDefinition(game_id=game_id, card_name="Explorer", rarity="uncommon"),
            TradingCardDefinition(game_id=game_id, card_name="Legend", rarity="rare"),
        ]
        db.add_all(cards)
        db.commit()

    card = random.choice(cards)
    item = InventoryItem(
        user_id=current_user.id,
        game_id=game_id,
        item_type="card",
        name=card.card_name,
        rarity=card.rarity,
        item_metadata={"card_id": card.id},
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.post("/badges/craft/{game_id}", response_model=InventoryItemOut)
def craft_badge(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    cards = (
        db.query(InventoryItem)
        .filter(
            InventoryItem.user_id == current_user.id,
            InventoryItem.game_id == game_id,
            InventoryItem.item_type == "card",
        )
        .order_by(InventoryItem.created_at.asc())
        .all()
    )
    if len(cards) < 3:
        raise HTTPException(status_code=400, detail="Not enough cards to craft a badge")

    for card in cards[:3]:
        db.delete(card)

    badge = InventoryItem(
        user_id=current_user.id,
        game_id=game_id,
        item_type="badge",
        name="Starter Badge",
        rarity="uncommon",
        item_metadata={"crafted_from": [card.id for card in cards[:3]]},
    )
    db.add(badge)
    db.commit()
    db.refresh(badge)
    return badge


@router.get("/trades", response_model=List[TradeOfferOut])
def list_trades(
    status: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = db.query(TradeOffer).filter(
        (TradeOffer.from_user_id == current_user.id)
        | (TradeOffer.to_user_id == current_user.id)
    )
    if status:
        query = query.filter(TradeOffer.status == status)
    return query.order_by(TradeOffer.created_at.desc()).all()


@router.post("/trades", response_model=TradeOfferOut)
def create_trade(
    payload: TradeOfferIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if payload.to_user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot trade with yourself")

    to_user = db.query(User).filter(User.id == payload.to_user_id).first()
    if not to_user:
        raise HTTPException(status_code=404, detail="Target user not found")

    offered_items = (
        db.query(InventoryItem)
        .filter(
            InventoryItem.user_id == current_user.id,
            InventoryItem.id.in_(payload.offered_item_ids),
        )
        .all()
    )
    if len(offered_items) != len(payload.offered_item_ids):
        raise HTTPException(status_code=400, detail="Invalid offered items")

    requested_items = (
        db.query(InventoryItem)
        .filter(
            InventoryItem.user_id == payload.to_user_id,
            InventoryItem.id.in_(payload.requested_item_ids),
        )
        .all()
    )
    if len(requested_items) != len(payload.requested_item_ids):
        raise HTTPException(status_code=400, detail="Invalid requested items")

    offer = TradeOffer(
        from_user_id=current_user.id,
        to_user_id=payload.to_user_id,
        offered_item_ids=payload.offered_item_ids,
        requested_item_ids=payload.requested_item_ids,
        status="pending",
        expires_at=datetime.utcnow() + timedelta(days=7),
    )
    db.add(offer)
    db.commit()
    db.refresh(offer)
    return offer


@router.post("/trades/{trade_id}/accept", response_model=TradeOfferOut)
def accept_trade(
    trade_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    offer = db.query(TradeOffer).filter(TradeOffer.id == trade_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Trade offer not found")
    if offer.to_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to accept")
    if offer.status != "pending":
        raise HTTPException(status_code=400, detail="Trade offer not pending")

    offered_items = (
        db.query(InventoryItem)
        .filter(InventoryItem.id.in_(offer.offered_item_ids))
        .all()
    )
    requested_items = (
        db.query(InventoryItem)
        .filter(InventoryItem.id.in_(offer.requested_item_ids))
        .all()
    )
    if len(offered_items) != len(offer.offered_item_ids) or len(requested_items) != len(offer.requested_item_ids):
        raise HTTPException(status_code=400, detail="Trade items missing")

    for item in offered_items:
        item.user_id = offer.to_user_id
    for item in requested_items:
        item.user_id = offer.from_user_id

    offer.status = "accepted"
    db.commit()
    db.refresh(offer)
    return offer


@router.post("/trades/{trade_id}/decline", response_model=TradeOfferOut)
def decline_trade(
    trade_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    offer = db.query(TradeOffer).filter(TradeOffer.id == trade_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Trade offer not found")
    if offer.to_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to decline")
    offer.status = "declined"
    db.commit()
    db.refresh(offer)
    return offer


@router.post("/trades/{trade_id}/cancel", response_model=TradeOfferOut)
def cancel_trade(
    trade_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    offer = db.query(TradeOffer).filter(TradeOffer.id == trade_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Trade offer not found")
    if offer.from_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to cancel")
    offer.status = "cancelled"
    db.commit()
    db.refresh(offer)
    return offer
