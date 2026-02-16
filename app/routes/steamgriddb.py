from __future__ import annotations

import hashlib
import os
import sys
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

from ..core.config import STEAMGRIDDB_API_KEY
from ..schemas import SteamGridDBAssetOut
from ..services.steam_catalog import get_steam_summary
from ..services.steamgriddb import (
    SteamGridDBError,
    build_title_variants,
    build_steam_fallback_assets,
    fetch_assets,
    get_cached_assets,
    search_game_by_steam_id,
    search_game_by_title,
    save_cached_assets,
)

router = APIRouter()


def _resolve_image_cache_root() -> Path:
    env_cache_root = os.getenv("OTOSHI_CACHE_DIR", "").strip()
    if env_cache_root:
        return Path(env_cache_root)

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        # Portable layout: resources/backend/otoshi-backend.exe -> ../../otoshi/cached
        return (exe_dir / ".." / ".." / "otoshi" / "cached").resolve()

    appdata = os.getenv("APPDATA", "").strip()
    if appdata:
        return Path(appdata) / "otoshi_launcher" / "cached"

    return Path("./storage/cache")


_IMAGE_CACHE_ROOT = _resolve_image_cache_root() / "image_thumbs"
_IMAGE_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
_ALLOWED_IMAGE_HOST_SUFFIXES = (
    "steamstatic.com",
    "steamgriddb.com",
    "steamusercontent.com",
    "steampowered.com",
    "akamaihd.net",
    "unsplash.com",
)

try:
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover - runtime fallback
    Image = None  # type: ignore


def _is_allowed_image_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    return any(host.endswith(suffix) for suffix in _ALLOWED_IMAGE_HOST_SUFFIXES)


def _redirect_source_image(url: str):
    return RedirectResponse(url=url, status_code=307)


def _cache_file_for(url: str, width: int, quality: int) -> Path:
    key = hashlib.sha1(f"{url}|{width}|{quality}".encode("utf-8")).hexdigest()
    return _IMAGE_CACHE_ROOT / f"{key}.webp"


def _build_cache_headers(etag: str) -> Dict[str, str]:
    return {
        "ETag": etag,
        "Cache-Control": "public, max-age=86400, stale-while-revalidate=259200",
    }


def _cache_etag(path: Path) -> str:
    stat = path.stat()
    return f'W/"{stat.st_mtime_ns:x}-{stat.st_size:x}"'


def _quality_for_mode(mode: str, q: Optional[int]) -> int:
    if q is not None:
        return max(20, min(90, int(q)))
    if mode == "fast":
        return 42
    if mode == "high":
        return 68
    return 52


def _resolve_lookup_assets(
    title: str | None,
    steam_app_id: str | None,
):
    if not STEAMGRIDDB_API_KEY:
        raise HTTPException(status_code=400, detail="SteamGridDB not configured")
    if not title and not steam_app_id:
        raise HTTPException(status_code=400, detail="Missing title or steam_app_id")

    if steam_app_id:
        cached = get_cached_assets(steam_app_id)
        if cached:
            return cached

    game = None
    search_title = title
    if not search_title and steam_app_id:
        summary = get_steam_summary(steam_app_id)
        search_title = summary.get("name") if summary else None

    try:
        if steam_app_id:
            try:
                game = search_game_by_steam_id(steam_app_id)
            except SteamGridDBError:
                game = None
        if not game and search_title:
            for candidate in build_title_variants(search_title):
                try:
                    game = search_game_by_title(candidate)
                except SteamGridDBError:
                    game = None
                if game:
                    break
        if not game or not game.get("id"):
            fallback = build_steam_fallback_assets(steam_app_id or "")
            result = {
                "game_id": 0,
                "name": search_title or (steam_app_id or ""),
                "grid": fallback.get("grid"),
                "hero": fallback.get("hero"),
                "logo": fallback.get("logo"),
                "icon": fallback.get("icon"),
            }
            save_cached_assets(
                steam_app_id or "",
                result["name"],
                None,
                fallback,
                source="steam_fallback",
            )
            return result
        assets = fetch_assets(int(game["id"]))
        fallback = build_steam_fallback_assets(steam_app_id or "")
        merged = {
            "grid": assets.get("grid") or fallback.get("grid"),
            "hero": assets.get("hero") or fallback.get("hero"),
            "logo": assets.get("logo") or fallback.get("logo"),
            "icon": assets.get("icon") or fallback.get("icon"),
        }
        result = {
            "game_id": int(game["id"]),
            "name": game.get("name") or (title or ""),
            "grid": merged.get("grid"),
            "hero": merged.get("hero"),
            "logo": merged.get("logo"),
            "icon": merged.get("icon"),
        }
        source = "steamgriddb" if any(assets.values()) else "steam_fallback"
        save_cached_assets(
            steam_app_id or "",
            result["name"],
            int(game["id"]),
            merged,
            source=source,
        )
        return result
    except SteamGridDBError:
        fallback = build_steam_fallback_assets(steam_app_id or "")
        result = {
            "game_id": 0,
            "name": search_title or (steam_app_id or ""),
            "grid": fallback.get("grid"),
            "hero": fallback.get("hero"),
            "logo": fallback.get("logo"),
            "icon": fallback.get("icon"),
        }
        save_cached_assets(
            steam_app_id or "",
            result["name"],
            None,
            fallback,
            source="steam_fallback",
        )
        return result


class LookupBatchItemIn(BaseModel):
    app_id: Optional[str] = None
    title: Optional[str] = None


class LookupBatchIn(BaseModel):
    items: List[LookupBatchItemIn] = Field(default_factory=list, max_length=200)


@router.get("/thumbnail")
def get_thumbnail(
    request: Request,
    url: str = Query(..., min_length=8),
    w: int = Query(320, ge=64, le=1024),
    q: int | None = Query(None, ge=20, le=90),
    mode: str = Query("adaptive", pattern="^(fast|adaptive|high)$"),
):
    if not _is_allowed_image_url(url):
        raise HTTPException(status_code=400, detail="Unsupported image host")

    quality = _quality_for_mode(mode, q)
    cache_path = _cache_file_for(url, w, quality)
    if cache_path.is_file():
        etag = _cache_etag(cache_path)
        headers = _build_cache_headers(etag)
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=headers)
        return FileResponse(cache_path, media_type="image/webp", headers=headers)

    try:
        response = requests.get(
            url,
            timeout=12,
            headers={"User-Agent": "OtoshiLauncher/1.0 (+image-cache)"},
        )
    except requests.RequestException:
        # Keep image rendering functional even if backend fetching fails.
        return _redirect_source_image(url)

    if response.status_code >= 400:
        return _redirect_source_image(url)

    content = response.content
    if len(content) > 15 * 1024 * 1024:
        return _redirect_source_image(url)

    if Image is None:
        # Pillow unavailable: keep app functional by falling back to source URL.
        return _redirect_source_image(url)

    try:
        with Image.open(BytesIO(content)) as image:
            if image.mode not in ("RGB", "RGBA"):
                image = image.convert("RGB")
            target_height = max(64, int((image.height / max(image.width, 1)) * w))
            image = image.resize((w, target_height))
            image.save(cache_path, format="WEBP", quality=quality, method=6)
    except Exception:
        return _redirect_source_image(url)

    etag = _cache_etag(cache_path)
    return FileResponse(
        cache_path,
        media_type="image/webp",
        headers=_build_cache_headers(etag),
    )


@router.get("/lookup", response_model=SteamGridDBAssetOut)
def lookup_assets(
    title: str | None = Query(None),
    steam_app_id: str | None = Query(None),
):
    return _resolve_lookup_assets(title=title, steam_app_id=steam_app_id)


@router.post("/lookup/batch")
def lookup_assets_batch(payload: LookupBatchIn):
    results: Dict[str, Dict[str, Any] | None] = {}
    for item in payload.items:
        steam_app_id = (item.app_id or "").strip() or None
        title = (item.title or "").strip() or None
        if not steam_app_id and not title:
            continue
        key = steam_app_id or (title or "").lower()
        try:
            results[key] = _resolve_lookup_assets(title=title, steam_app_id=steam_app_id)
        except HTTPException:
            results[key] = None
    return {"items": results}
