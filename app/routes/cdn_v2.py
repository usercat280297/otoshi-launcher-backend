from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from ..models import User
from ..routes.deps import get_current_user_optional
from ..services.download_source_policy import is_vip_identity
from ..services.origin_router import resolve_origin_url

router = APIRouter(prefix="/v2/cdn", tags=["v2-cdn"])


@router.get("/resolve")
def resolve_cdn_origin_v2(
    path: str = Query(...),
    channel: str = Query(default="stable"),
    signed: bool = Query(default=False),
    ttl_seconds: int = Query(default=600, ge=60, le=3600),
    game_size_bytes: Optional[int] = Query(default=None, ge=0),
    method: Optional[str] = Query(default=None),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    route = resolve_origin_url(
        path,
        channel=channel,
        signed=signed,
        ttl_seconds=ttl_seconds,
        game_size_bytes=game_size_bytes,
        is_vip=is_vip_identity(current_user),
        method=method,
    )
    return route.to_dict()
