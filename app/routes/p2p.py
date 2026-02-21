from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import P2PPeer, User
from ..routes.deps import get_current_user
from ..schemas import (
    P2PPeerHeartbeatIn,
    P2PPeerHeartbeatOut,
    P2PPeerListOut,
    P2PPeerOut,
    P2PPeerRegisterIn,
    P2PPeerRegisterOut,
)

router = APIRouter()

DEFAULT_HEARTBEAT_INTERVAL_S = 20
DEFAULT_ONLINE_TTL_S = 90


def _normalize_addresses(addresses: list[str], limit: int = 24) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in addresses[: max(0, limit * 2)]:
        value = str(raw).strip()
        if not value:
            continue
        if len(value) > 64:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
        if len(out) >= limit:
            break
    return out


@router.post("/peers/register", response_model=P2PPeerRegisterOut)
def register_peer(
    payload: P2PPeerRegisterIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    addresses = _normalize_addresses(payload.addresses)
    now = datetime.utcnow()
    peer = (
        db.query(P2PPeer)
        .filter(P2PPeer.user_id == current_user.id, P2PPeer.device_id == payload.device_id)
        .first()
    )
    if not peer:
        peer = P2PPeer(
            user_id=current_user.id,
            device_id=payload.device_id,
            port=int(payload.port),
            addresses=addresses,
            share_enabled=bool(payload.share_enabled),
            upload_limit_bps=int(payload.upload_limit_bps or 0),
            last_seen_at=now,
        )
        db.add(peer)
    else:
        peer.port = int(payload.port)
        peer.addresses = addresses
        peer.share_enabled = bool(payload.share_enabled)
        peer.upload_limit_bps = int(payload.upload_limit_bps or 0)
        peer.last_seen_at = now

    db.commit()
    db.refresh(peer)
    return P2PPeerRegisterOut(peer_id=peer.id, heartbeat_interval_s=DEFAULT_HEARTBEAT_INTERVAL_S)


@router.post("/peers/heartbeat", response_model=P2PPeerHeartbeatOut)
def peer_heartbeat(
    payload: P2PPeerHeartbeatIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    now = datetime.utcnow()
    peer = (
        db.query(P2PPeer)
        .filter(P2PPeer.id == payload.peer_id, P2PPeer.user_id == current_user.id)
        .first()
    )
    if not peer:
        # Treat as "not registered" and let client re-register.
        return P2PPeerHeartbeatOut(ok=False, heartbeat_interval_s=DEFAULT_HEARTBEAT_INTERVAL_S)

    peer.last_seen_at = now
    if payload.addresses:
        peer.addresses = _normalize_addresses(payload.addresses)
    if payload.port is not None:
        peer.port = int(payload.port)
    if payload.share_enabled is not None:
        peer.share_enabled = bool(payload.share_enabled)
    if payload.upload_limit_bps is not None:
        peer.upload_limit_bps = int(payload.upload_limit_bps)

    db.commit()
    return P2PPeerHeartbeatOut(ok=True, heartbeat_interval_s=DEFAULT_HEARTBEAT_INTERVAL_S)


@router.get("/peers", response_model=P2PPeerListOut)
def list_online_peers(
    game_id: Optional[str] = Query(default=None),
    peer_id: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # game_id is reserved for future filtering and peer availability indexing.
    _ = game_id
    _ = current_user
    cutoff = datetime.utcnow() - timedelta(seconds=DEFAULT_ONLINE_TTL_S)
    query = db.query(P2PPeer).filter(
        P2PPeer.share_enabled.is_(True),
        P2PPeer.last_seen_at >= cutoff,
    )
    if peer_id:
        query = query.filter(P2PPeer.id != peer_id)
    peers = query.order_by(P2PPeer.last_seen_at.desc()).limit(200).all()
    out = [
        P2PPeerOut(
            peer_id=item.id,
            port=int(item.port or 0),
            addresses=list(item.addresses or []),
            upload_limit_bps=int(item.upload_limit_bps or 0),
            last_seen_at=item.last_seen_at,
        )
        for item in peers
        if (item.port or 0) > 0
    ]
    return P2PPeerListOut(peers=out)
