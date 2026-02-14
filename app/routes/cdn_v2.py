from __future__ import annotations

from fastapi import APIRouter, Query

from ..services.origin_router import resolve_origin_url

router = APIRouter(prefix="/v2/cdn", tags=["v2-cdn"])


@router.get("/resolve")
def resolve_cdn_origin_v2(
    path: str = Query(...),
    channel: str = Query(default="stable"),
    signed: bool = Query(default=False),
    ttl_seconds: int = Query(default=600, ge=60, le=3600),
):
    route = resolve_origin_url(
        path,
        channel=channel,
        signed=signed,
        ttl_seconds=ttl_seconds,
    )
    return route.to_dict()

