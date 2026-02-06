from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from ..core.cache import cache_client
from ..core.config import STEAM_CATALOG_CACHE_TTL_SECONDS
from ..core.denuvo import DENUVO_APP_ID_SET
from .steam_catalog import get_catalog_page

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _load_json(name: str) -> dict[str, Any]:
    path = DATA_DIR / name
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _normalize_options(raw: Any) -> list[dict[str, Any]]:
    if not raw:
        return []
    if isinstance(raw, str):
        return [{"link": raw}]
    if isinstance(raw, list):
        options = []
        for item in raw:
            if isinstance(item, str):
                options.append({"link": item})
            elif isinstance(item, dict):
                options.append(
                    {
                        "link": item.get("link", ""),
                        "name": item.get("name"),
                        "note": item.get("note"),
                        "version": item.get("version"),
                        "size": item.get("size"),
                        "recommended": bool(item.get("recommended")),
                    }
                )
        return [option for option in options if option.get("link")]
    if isinstance(raw, dict):
        if "link" in raw:
            return [
                {
                    "link": raw.get("link", ""),
                    "name": raw.get("name"),
                    "note": raw.get("note"),
                    "version": raw.get("version"),
                    "size": raw.get("size"),
                    "recommended": bool(raw.get("recommended")),
                }
            ]
    return []


def _build_entries(appids: Iterable[str], mapping: dict[str, Any]) -> list[dict[str, Any]]:
    appids = [str(app_id) for app_id in appids]
    summaries = get_catalog_page(appids)
    summary_map = {item.get("app_id"): item for item in summaries}
    entries = []
    for app_id in appids:
        options = _normalize_options(mapping.get(app_id))
        if not options:
            continue
        steam = summary_map.get(app_id)
        name = steam.get("name") if steam else options[0].get("name")
        entries.append(
            {
                "app_id": app_id,
                "name": name or app_id,
                "steam": steam,
                "options": options,
                "denuvo": str(app_id) in DENUVO_APP_ID_SET,
            }
        )
    return entries


def _paginate(items: list[dict[str, Any]], offset: int, limit: int) -> tuple[int, list[dict[str, Any]]]:
    total = len(items)
    if limit <= 0:
        return total, items[offset:]
    return total, items[offset : offset + limit]


def get_online_fix_catalog(offset: int = 0, limit: int = 100) -> dict[str, Any]:
    cache_key = f"fixes:online:{offset}:{limit}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached
    data = _load_json("online_fix.json")
    appids = sorted(data.keys(), key=lambda value: int(value))
    entries = _build_entries(appids, data)
    total, items = _paginate(entries, offset, limit)
    payload = {"total": total, "offset": offset, "limit": limit, "items": items}
    cache_client.set_json(cache_key, payload, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return payload


def get_bypass_catalog(offset: int = 0, limit: int = 100) -> dict[str, Any]:
    cache_key = f"fixes:bypass:{offset}:{limit}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached
    data = _load_json("bypass.json")
    appids = sorted(data.keys(), key=lambda value: int(value))
    entries = _build_entries(appids, data)
    total, items = _paginate(entries, offset, limit)
    payload = {"total": total, "offset": offset, "limit": limit, "items": items}
    cache_client.set_json(cache_key, payload, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return payload


def get_online_fix_options(app_id: str) -> list[dict[str, Any]]:
    data = _load_json("online_fix.json")
    return _normalize_options(data.get(str(app_id)))


def get_bypass_option(app_id: str) -> dict[str, Any] | None:
    data = _load_json("bypass.json")
    options = _normalize_options(data.get(str(app_id)))
    return options[0] if options else None


def get_bypass_categories() -> list[dict[str, Any]]:
    """Get all bypass categories with their games."""
    cache_key = "fixes:bypass:categories"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached

    cat_data = _load_json("bypass_categories.json")
    if not cat_data:
        return []

    categories = cat_data.get("categories", [])
    games_data = cat_data.get("games", {})

    result = []
    for cat in categories:
        cat_id = cat.get("id", "")
        cat_games = cat.get("games", [])

        # Get Steam summaries for games in this category
        summaries = get_catalog_page(cat_games)
        summary_map = {item.get("app_id"): item for item in summaries}

        games = []
        for app_id in cat_games:
            game_info = games_data.get(app_id, {})
            steam = summary_map.get(app_id)

            # Handle single link or multiple links
            if "links" in game_info:
                # Multiple links format: {"links": [{"link": "...", "name": "..."}, ...]}
                options = [{"link": l.get("link", ""), "name": l.get("name", "")} for l in game_info.get("links", [])]
            elif "link" in game_info:
                # Single link format: {"link": "...", "name": "..."}
                link_value = game_info.get("link", "")
                if isinstance(link_value, list):
                    # link is an array of strings
                    options = [{"link": l, "name": f"Option {i+1}"} for i, l in enumerate(link_value)]
                else:
                    # link is a single string
                    options = [{"link": link_value, "name": game_info.get("name", "")}]
            else:
                options = []

            games.append(
                {
                    "app_id": app_id,
                    "name": game_info.get("name", app_id),
                    "steam": steam,
                    "options": options,
                    "denuvo": str(app_id) in DENUVO_APP_ID_SET,
                }
            )

        result.append(
            {
                "id": cat_id,
                "name": cat.get("name", ""),
                "description": cat.get("description", ""),
                "icon": cat.get("icon", ""),
                "total": len(games),
                "games": games,
            }
        )

    cache_client.set_json(cache_key, result, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return result


def get_bypass_by_category(category_id: str, offset: int = 0, limit: int = 100) -> dict[str, Any]:
    """Get bypass games filtered by category."""
    cache_key = f"fixes:bypass:cat:{category_id}:{offset}:{limit}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached

    cat_data = _load_json("bypass_categories.json")
    if not cat_data:
        return {"total": 0, "offset": offset, "limit": limit, "items": [], "category": None}

    categories = cat_data.get("categories", [])
    games_data = cat_data.get("games", {})

    # Find the category
    target_cat = None
    for cat in categories:
        if cat.get("id") == category_id:
            target_cat = cat
            break

    if not target_cat:
        return {"total": 0, "offset": offset, "limit": limit, "items": [], "category": None}

    cat_games = target_cat.get("games", [])

    # Get Steam summaries for games in this category
    summaries = get_catalog_page(cat_games)
    summary_map = {item.get("app_id"): item for item in summaries}

    items = []
    for app_id in cat_games:
        game_info = games_data.get(app_id, {})
        steam = summary_map.get(app_id)

        # Handle single link or multiple links
        if "links" in game_info:
            # Multiple links format: {"links": [{"link": "...", "name": "..."}, ...]}
            options = [{"link": l.get("link", ""), "name": l.get("name", "")} for l in game_info.get("links", [])]
        elif "link" in game_info:
            # Single link format: {"link": "...", "name": "..."}
            link_value = game_info.get("link", "")
            if isinstance(link_value, list):
                # link is an array of strings
                options = [{"link": l, "name": f"Option {i+1}"} for i, l in enumerate(link_value)]
            else:
                # link is a single string
                options = [{"link": link_value, "name": game_info.get("name", "")}]
        else:
            options = []

        items.append(
            {
                "app_id": app_id,
                "name": game_info.get("name", app_id),
                "steam": steam,
                "options": options,
                "denuvo": str(app_id) in DENUVO_APP_ID_SET,
            }
        )

    total, paginated = _paginate(items, offset, limit)

    payload = {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": paginated,
        "category": {
            "id": target_cat.get("id", ""),
            "name": target_cat.get("name", ""),
            "description": target_cat.get("description", ""),
            "icon": target_cat.get("icon", ""),
        },
    }

    cache_client.set_json(cache_key, payload, ttl=STEAM_CATALOG_CACHE_TTL_SECONDS)
    return payload
