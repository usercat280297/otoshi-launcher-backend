from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, LibraryEntry, PaymentTransaction, SupportDonation, User
from ..schemas import DonationCreateIn, DonationOut, PaymentIntentIn, PaymentOut
from ..services.stripe_service import stripe_service
from ..services.vnpay import vnpay_service
from ..core.config import VNPAY_RETURN_URL
from .deps import get_current_user

router = APIRouter()
_DONATION_TIER_RULES: list[tuple[str, float, int]] = [
    ("vip", 100.0, 365),
    ("supporter_plus", 30.0, 180),
    ("supporter", 5.0, 90),
]


def _resolve_tier_from_total(total_amount: float) -> tuple[str, int] | tuple[None, None]:
    for tier, threshold, duration_days in _DONATION_TIER_RULES:
        if total_amount >= threshold:
            return tier, duration_days
    return None, None


@router.get("/history", response_model=list[PaymentOut])
def payment_history(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return (
        db.query(PaymentTransaction)
        .filter(PaymentTransaction.user_id == current_user.id)
        .order_by(PaymentTransaction.created_at.desc())
        .all()
    )


@router.post("/checkout/{game_id}", response_model=PaymentOut)
def checkout_game(
    game_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    amount = float(game.price) * (1 - (game.discount_percent or 0) / 100)
    payment = PaymentTransaction(
        user_id=current_user.id,
        game_id=game.id,
        amount=amount,
        currency="USD",
        status="completed",
        provider="library",
    )
    db.add(payment)

    existing = (
        db.query(LibraryEntry)
        .filter(
            LibraryEntry.user_id == current_user.id,
            LibraryEntry.game_id == game.id,
        )
        .first()
    )
    if not existing:
        db.add(LibraryEntry(user_id=current_user.id, game_id=game.id))

    db.commit()
    db.refresh(payment)
    return payment


@router.post("/donate", response_model=DonationOut)
def donate(
    payload: DonationCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    currency = str(payload.currency or "USD").strip().upper()[:10] or "USD"
    provider = str(payload.provider or "donation").strip()[:40] or "donation"
    donation = SupportDonation(
        user_id=current_user.id,
        amount=float(payload.amount),
        currency=currency,
        provider=provider,
        note=payload.note,
        created_at=datetime.utcnow(),
    )
    db.add(donation)
    db.flush()

    lifetime_total = (
        db.query(func.coalesce(func.sum(SupportDonation.amount), 0.0))
        .filter(SupportDonation.user_id == current_user.id)
        .scalar()
    )
    tier, duration_days = _resolve_tier_from_total(float(lifetime_total or 0.0))
    tier_expires_at = None
    if tier and duration_days:
        now = datetime.utcnow()
        proposed_expiry = now + timedelta(days=duration_days)
        current_expiry = current_user.membership_expires_at
        if current_expiry and current_expiry > proposed_expiry:
            tier_expires_at = current_expiry
        else:
            tier_expires_at = proposed_expiry
        current_user.membership_tier = tier
        current_user.membership_expires_at = tier_expires_at

    db.commit()
    db.refresh(donation)
    db.refresh(current_user)

    return {
        "id": donation.id,
        "amount": float(donation.amount or 0.0),
        "currency": donation.currency or currency,
        "provider": donation.provider or provider,
        "note": donation.note,
        "created_at": donation.created_at,
        "tier": current_user.membership_tier,
        "tier_expires_at": current_user.membership_expires_at,
    }


@router.post("/stripe/create-intent")
def create_payment_intent(
    payload: PaymentIntentIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not stripe_service.enabled():
        raise HTTPException(status_code=503, detail="Stripe not configured")

    games = db.query(Game).filter(Game.id.in_(payload.items)).all()
    if not games:
        raise HTTPException(status_code=404, detail="Games not found")

    total = sum(
        float(game.price) * (1 - (game.discount_percent or 0) / 100) for game in games
    )
    intent = stripe_service.create_payment_intent(
        amount_cents=int(total * 100),
        currency=payload.currency or "usd",
        metadata={
            "game_ids": ",".join([game.id for game in games]),
            "user_id": current_user.id,
        },
    )
    return {"client_secret": intent.client_secret}


@router.post("/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.body()
    signature = request.headers.get("stripe-signature", "")
    if not signature:
        raise HTTPException(status_code=400, detail="Missing signature")

    try:
        event = stripe_service.verify_webhook(payload, signature)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if event["type"] == "payment_intent.succeeded":
        payment_intent = event["data"]["object"]
        handle_successful_payment(payment_intent, db)

    return {"status": "success"}


def handle_successful_payment(payment_intent: dict, db: Session) -> None:
    metadata = payment_intent.get("metadata", {})
    user_id = metadata.get("user_id")
    if not user_id:
        return
    game_ids = [item for item in metadata.get("game_ids", "").split(",") if item]

    games = db.query(Game).filter(Game.id.in_(game_ids)).all()
    for game in games:
        amount = float(game.price) * (1 - (game.discount_percent or 0) / 100)
        db.add(
            PaymentTransaction(
                user_id=user_id,
                game_id=game.id,
                amount=amount,
                currency="USD",
                status="completed",
                provider="stripe",
            )
        )

        existing = (
            db.query(LibraryEntry)
            .filter(LibraryEntry.user_id == user_id, LibraryEntry.game_id == game.id)
            .first()
        )
        if not existing:
            db.add(LibraryEntry(user_id=user_id, game_id=game.id))

    db.commit()


@router.post("/vnpay/create")
def vnpay_create_payment(
    game_id: str,
    amount_vnd: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not vnpay_service.enabled():
        raise HTTPException(status_code=503, detail="VNPay not configured")

    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    payment = PaymentTransaction(
        user_id=current_user.id,
        game_id=game.id,
        amount=amount_vnd,
        currency="VND",
        status="pending",
        provider="vnpay",
        created_at=datetime.utcnow(),
    )
    db.add(payment)
    db.commit()
    db.refresh(payment)

    url = vnpay_service.create_payment_url(
        order_id=payment.id,
        amount_vnd=amount_vnd,
        return_url=VNPAY_RETURN_URL,
        description=f"Payment for {game.title}",
    )
    return {"payment_url": url, "order_id": payment.id}


@router.get("/vnpay/return")
def vnpay_return(
    request: Request,
    db: Session = Depends(get_db),
):
    params = dict(request.query_params)
    order_id = params.get("vnp_TxnRef")
    if not order_id:
        raise HTTPException(status_code=400, detail="Missing order id")

    if not vnpay_service.verify_payment(params):
        raise HTTPException(status_code=400, detail="Invalid VNPay signature")

    payment = db.query(PaymentTransaction).filter(PaymentTransaction.id == order_id).first()
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    payment.status = "completed"
    db.commit()

    existing = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.user_id == payment.user_id, LibraryEntry.game_id == payment.game_id)
        .first()
    )
    if not existing:
        db.add(LibraryEntry(user_id=payment.user_id, game_id=payment.game_id))
        db.commit()

    return {"status": "success"}
