from typing import Any

import stripe

from ..core.config import STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET


class StripeService:
    def __init__(self) -> None:
        if STRIPE_SECRET_KEY:
            stripe.api_key = STRIPE_SECRET_KEY

    def enabled(self) -> bool:
        return bool(STRIPE_SECRET_KEY)

    def create_payment_intent(self, amount_cents: int, currency: str, metadata: dict) -> Any:
        if not STRIPE_SECRET_KEY:
            raise RuntimeError("Stripe is not configured")
        return stripe.PaymentIntent.create(
            amount=amount_cents,
            currency=currency,
            metadata=metadata,
        )

    def verify_webhook(self, payload: bytes, signature: str) -> Any:
        if not STRIPE_WEBHOOK_SECRET:
            raise RuntimeError("Stripe webhook secret missing")
        return stripe.Webhook.construct_event(payload, signature, STRIPE_WEBHOOK_SECRET)


stripe_service = StripeService()
