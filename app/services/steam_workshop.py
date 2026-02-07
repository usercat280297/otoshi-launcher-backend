from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from ..core.cache import cache_client
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.config import (
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
    STEAM_REQUEST_TIMEOUT_SECONDS,
    WORKSHOP_STEAM_APP_ID,
    WORKSHOP_STEAM_APP_IDS,
    WORKSHOP_STEAM_SOURCE,
    WORKSHOP_STEAM_MAX_APPIDS,
    WORKSHOP_STEAM_PER_GAME,
    WORKSHOP_STEAM_LIMIT,
)
from ..services.steam_catalog import get_lua_appids, get_lua_workshop_appids


def _request(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            url,
            params=params,
            timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "otoshi-launcher/1.0"},
        )
        if response.status_code != 200:
            return None
        return response.json()
    except (requests.RequestException, ValueError):
        return None


def _to_datetime(ts: Optional[int]) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def query_workshop_items(
    app_id: str,
    search: Optional[str] = None,
    limit: int = 24,
) -> List[Dict[str, Any]]:
    if not STEAM_WEB_API_KEY or not app_id:
        return []

    cache_key = f"steam:workshop:{app_id}:{search or ''}:{limit}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached

    url = f"{STEAM_WEB_API_URL.rstrip('/')}/IPublishedFileService/QueryFiles/v1/"
    params: Dict[str, Any] = {
        "key": STEAM_WEB_API_KEY,
        "query_type": 0,
        "page": 1,
        "numperpage": limit,
        "appid": app_id,
    }
    if search:
        params["search_text"] = search

    payload = _request(url, params)
    response = payload.get("response") if payload else None
    files = response.get("publishedfiledetails") if response else None
    if not files or not isinstance(files, list):
        return []

    items: List[Dict[str, Any]] = []
    for entry in files:
        published_id = str(entry.get("publishedfileid") or "")
        if not published_id:
            continue
        tags = []
        raw_tags = entry.get("tags") or []
        if isinstance(raw_tags, list):
            for tag in raw_tags:
                if isinstance(tag, dict) and tag.get("tag"):
                    tags.append(tag.get("tag"))
        created_at = _to_datetime(entry.get("time_created"))
        updated_at = _to_datetime(entry.get("time_updated"))

        items.append(
            {
                "id": f"steam:{published_id}",
                "game_id": str(app_id),
                "creator_id": "steam",
                "title": entry.get("title") or "Workshop item",
                "description": entry.get("description") or None,
                "item_type": entry.get("file_type") or "workshop",
                "visibility": "public",
                "total_downloads": int(entry.get("subscriptions") or 0),
                "total_subscriptions": int(entry.get("subscriptions") or 0),
                "rating_up": int(entry.get("votes_up") or 0),
                "rating_down": int(entry.get("votes_down") or 0),
                "tags": tags,
                "preview_image_url": entry.get("preview_url") or None,
                "created_at": created_at,
                "updated_at": updated_at,
                "source": "steam",
            }
        )

    cache_client.set_json(cache_key, items, ttl=600)
    return items


def _parse_app_ids(raw: str) -> List[str]:
    if not raw:
        return []
    parts = []
    for item in raw.split(","):
        cleaned = item.strip()
        if cleaned:
            parts.append(cleaned)
    return parts


def get_workshop_app_ids(explicit: Optional[List[str]] = None) -> List[str]:
    if explicit:
        return explicit

    if WORKSHOP_STEAM_APP_ID:
        return [WORKSHOP_STEAM_APP_ID]

    configured = _parse_app_ids(WORKSHOP_STEAM_APP_IDS)
    if configured:
        return configured

    if WORKSHOP_STEAM_SOURCE == "lua":
        return get_lua_workshop_appids()

    return []


def query_workshop_multi(
    app_ids: List[str],
    search: Optional[str] = None,
    per_game: Optional[int] = None,
    total_limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not app_ids:
        return []

    max_appids = max(1, WORKSHOP_STEAM_MAX_APPIDS)
    candidate_ids = app_ids[:]
    if len(candidate_ids) > max_appids:
        candidate_ids = random.sample(candidate_ids, max_appids)

    per_game_limit = per_game or WORKSHOP_STEAM_PER_GAME
    hard_limit = total_limit or WORKSHOP_STEAM_LIMIT

    items: List[Dict[str, Any]] = []
    seen: set[str] = set()

    with ThreadPoolExecutor(max_workers=min(6, len(candidate_ids))) as executor:
        futures = {
            executor.submit(query_workshop_items, app_id, search, per_game_limit): app_id
            for app_id in candidate_ids
        }
        for future in as_completed(futures):
            result = future.result() or []
            for entry in result:
                entry_id = entry.get("id")
                if not entry_id or entry_id in seen:
                    continue
                seen.add(entry_id)
                items.append(entry)
                if hard_limit and len(items) >= hard_limit:
                    return items

    return items
