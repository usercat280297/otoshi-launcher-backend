from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

from ..core.cache import cache_client
from ..core.config import STEAM_CATALOG_CACHE_TTL_SECONDS
from ..core.denuvo import DENUVO_APP_ID_SET
from .steam_catalog import get_catalog_page

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
_PLACEHOLDER_STEAM_APP_RE = re.compile(r"^steam app\s+\d+$", re.IGNORECASE)


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
    if isinstance(raw, dict) and "link" in raw:
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


def _normalize_bypass_game_options(game_info: Any) -> list[dict[str, Any]]:
    if not isinstance(game_info, dict):
        return _normalize_options(game_info)

    if "links" in game_info:
        return _normalize_options(game_info.get("links"))

    if "link" in game_info:
        link_value = game_info.get("link")
        if isinstance(link_value, list):
            return _normalize_options(link_value)
        return _normalize_options(
            {
                "link": link_value,
                "name": game_info.get("name"),
                "note": game_info.get("note"),
                "version": game_info.get("version"),
                "size": game_info.get("size"),
                "recommended": game_info.get("recommended"),
            }
        )

    return []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _is_placeholder_title(value: Any, app_id: str | None = None) -> bool:
    text = _clean_text(value)
    if not text:
        return True
    if text.isdigit():
        return True
    if _PLACEHOLDER_STEAM_APP_RE.match(text):
        return True
    app_text = _clean_text(app_id)
    if app_text:
        lowered = text.lower()
        if lowered == app_text.lower() or lowered == f"steam app {app_text}".lower():
            return True
    return False


def _extract_option_names(options: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for option in options:
        name = _clean_text(option.get("name"))
        if name:
            names.append(name)
    return names


def _resolve_entry_name(
    app_id: str,
    steam: dict[str, Any] | None,
    options: list[dict[str, Any]],
    explicit_name: Any = None,
) -> str:
    candidates: list[str] = []
    explicit = _clean_text(explicit_name)
    if explicit:
        candidates.append(explicit)
    candidates.extend(_extract_option_names(options))
    steam_name = _clean_text((steam or {}).get("name"))
    if steam_name:
        candidates.append(steam_name)

    for candidate in candidates:
        if not _is_placeholder_title(candidate, app_id):
            return candidate
    for candidate in candidates:
        if candidate:
            return candidate
    return app_id


def _patch_steam_summary_name(
    steam: dict[str, Any] | None,
    app_id: str,
    preferred_name: str,
) -> dict[str, Any] | None:
    if not isinstance(steam, dict):
        return steam
    payload = dict(steam)
    if _is_placeholder_title(payload.get("name"), app_id):
        payload["name"] = preferred_name
    return payload


def _get_steam_summary(app_id: str) -> dict[str, Any] | None:
    summaries = get_catalog_page([str(app_id)])
    if not summaries:
        return None
    return summaries[0]


def _build_entries(appids: Iterable[str], mapping: dict[str, Any]) -> list[dict[str, Any]]:
    appids = [str(app_id) for app_id in appids]
    summaries = get_catalog_page(appids)
    summary_map = {item.get("app_id"): item for item in summaries}
    entries = []
    for app_id in appids:
        raw_entry = mapping.get(app_id)
        options = _normalize_options(raw_entry)
        if not options:
            continue
        steam = summary_map.get(app_id)
        explicit_name = raw_entry.get("name") if isinstance(raw_entry, dict) else None
        name = _resolve_entry_name(app_id, steam, options, explicit_name=explicit_name)
        steam = _patch_steam_summary_name(steam, app_id, name)
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


def _to_string_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        values: list[str] = []
        for item in raw:
            if isinstance(item, str):
                values.append(item)
        return values
    return []


def _to_step_objects(raw: Any) -> list[dict[str, str]]:
    if not raw:
        return []
    steps: list[dict[str, str]] = []
    if isinstance(raw, list):
        for index, item in enumerate(raw):
            if isinstance(item, dict):
                title = str(item.get("title") or f"Step {index + 1}")
                description = str(item.get("description") or "").strip()
                if description:
                    steps.append({"title": title, "description": description})
            elif isinstance(item, str):
                text = item.strip()
                if text:
                    steps.append({"title": f"Step {index + 1}", "description": text})
    return steps


def _default_guide(kind: str, app_id: str, name: str) -> dict[str, Any]:
    if kind == "bypass":
        steps = [
            {"title": "Step 1", "description": "Close the game and launcher before applying bypass files."},
            {"title": "Step 2", "description": "Extract the selected package to a temporary folder."},
            {"title": "Step 3", "description": "Copy files into the game folder and replace when prompted."},
            {"title": "Step 4", "description": "Launch the game with the required bypass executable if provided."},
        ]
        warnings = [
            "Always keep a backup of original game files before replacing anything.",
            "Some security tools may quarantine patched files.",
        ]
    else:
        steps = [
            {"title": "Step 1", "description": "Close the game before applying online-fix files."},
            {"title": "Step 2", "description": "Extract the selected package to a temporary folder."},
            {"title": "Step 3", "description": "Copy all files to the game directory and replace existing files."},
            {"title": "Step 4", "description": "Launch the game and test multiplayer/online features."},
        ]
        warnings = [
            "Use only the fix version that matches your game version.",
            "Keep a clean backup of original files so you can roll back.",
        ]

    return {
        "title": f"{name} setup guide",
        "summary": "Guide content is currently default template. You can customize it in backend/app/data/fix_guides.json.",
        "steps": steps,
        "warnings": warnings,
        "notes": [f"App ID: {app_id}"],
        "updated_at": None,
    }


def _load_fix_guides() -> dict[str, Any]:
    return _load_json("fix_guides.json")


def _build_guide(kind: str, app_id: str, name: str, option_notes: list[str]) -> dict[str, Any]:
    guides = _load_fix_guides()
    by_kind = guides.get(kind) if isinstance(guides, dict) else None
    custom = by_kind.get(str(app_id)) if isinstance(by_kind, dict) else None

    guide = _default_guide(kind, app_id, name)
    if isinstance(custom, dict):
        title = str(custom.get("title") or guide["title"])
        summary = custom.get("summary")
        summary = str(summary) if isinstance(summary, str) and summary.strip() else guide["summary"]
        steps = _to_step_objects(custom.get("steps"))
        warnings = _to_string_list(custom.get("warnings"))
        notes = _to_string_list(custom.get("notes"))
        updated_at = custom.get("updated_at")
        guide = {
            "title": title,
            "summary": summary,
            "steps": steps or guide["steps"],
            "warnings": warnings or guide["warnings"],
            "notes": notes,
            "updated_at": str(updated_at) if isinstance(updated_at, str) else None,
        }

    merged_notes = list(guide.get("notes") or [])
    for note in option_notes:
        if note and note not in merged_notes:
            merged_notes.append(note)
    guide["notes"] = merged_notes
    return guide


def _find_bypass_game(app_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    cat_data = _load_json("bypass_categories.json")
    if not cat_data:
        return None, None

    app_id = str(app_id)
    categories = cat_data.get("categories", [])
    games_data = cat_data.get("games", {})
    game_info = games_data.get(app_id)

    category_meta = None
    if isinstance(categories, list):
        for cat in categories:
            if not isinstance(cat, dict):
                continue
            if app_id in (cat.get("games") or []):
                category_meta = {
                    "id": cat.get("id", ""),
                    "name": cat.get("name", ""),
                    "description": cat.get("description", ""),
                    "icon": cat.get("icon", ""),
                }
                break

    return game_info if isinstance(game_info, dict) else None, category_meta


def get_online_fix_catalog(offset: int = 0, limit: int = 100) -> dict[str, Any]:
    cache_key = f"fixes:online:v2:{offset}:{limit}"
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
    cache_key = f"fixes:bypass:v2:{offset}:{limit}"
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
    game_info, _ = _find_bypass_game(str(app_id))
    if game_info:
        options = _normalize_bypass_game_options(game_info)
        return options[0] if options else None

    data = _load_json("bypass.json")
    options = _normalize_options(data.get(str(app_id)))
    return options[0] if options else None


def get_bypass_categories() -> list[dict[str, Any]]:
    """Get all bypass categories with their games."""
    cache_key = "fixes:bypass:categories:v2"
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

        summaries = get_catalog_page(cat_games)
        summary_map = {item.get("app_id"): item for item in summaries}

        games = []
        for app_id in cat_games:
            app_id = str(app_id)
            game_info = games_data.get(app_id, {})
            steam = summary_map.get(app_id)
            options = _normalize_bypass_game_options(game_info)
            name = _resolve_entry_name(
                app_id,
                steam,
                options,
                explicit_name=game_info.get("name") if isinstance(game_info, dict) else None,
            )
            steam = _patch_steam_summary_name(steam, app_id, name)

            games.append(
                {
                    "app_id": app_id,
                    "name": name,
                    "steam": steam,
                    "options": options,
                    "denuvo": app_id in DENUVO_APP_ID_SET,
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
    cache_key = f"fixes:bypass:cat:v2:{category_id}:{offset}:{limit}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached

    cat_data = _load_json("bypass_categories.json")
    if not cat_data:
        return {"total": 0, "offset": offset, "limit": limit, "items": [], "category": None}

    categories = cat_data.get("categories", [])
    games_data = cat_data.get("games", {})

    target_cat = None
    for cat in categories:
        if cat.get("id") == category_id:
            target_cat = cat
            break

    if not target_cat:
        return {"total": 0, "offset": offset, "limit": limit, "items": [], "category": None}

    cat_games = target_cat.get("games", [])
    summaries = get_catalog_page(cat_games)
    summary_map = {item.get("app_id"): item for item in summaries}

    items = []
    for app_id in cat_games:
        app_id = str(app_id)
        game_info = games_data.get(app_id, {})
        steam = summary_map.get(app_id)
        options = _normalize_bypass_game_options(game_info)
        name = _resolve_entry_name(
            app_id,
            steam,
            options,
            explicit_name=game_info.get("name") if isinstance(game_info, dict) else None,
        )
        steam = _patch_steam_summary_name(steam, app_id, name)
        items.append(
            {
                "app_id": app_id,
                "name": name,
                "steam": steam,
                "options": options,
                "denuvo": app_id in DENUVO_APP_ID_SET,
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


def get_fix_entry_detail(kind: str, app_id: str) -> dict[str, Any] | None:
    app_id = str(app_id)
    if kind not in {"online-fix", "bypass"}:
        return None

    category = None
    if kind == "online-fix":
        options = get_online_fix_options(app_id)
        steam = _get_steam_summary(app_id)
        name = _resolve_entry_name(app_id, steam, options, explicit_name=None)
        steam = _patch_steam_summary_name(steam, app_id, name)
    else:
        game_info, category = _find_bypass_game(app_id)
        options = _normalize_bypass_game_options(game_info) if game_info else []
        if not options:
            fallback = _load_json("bypass.json")
            options = _normalize_options(fallback.get(app_id))
        steam = _get_steam_summary(app_id)
        explicit_name = game_info.get("name") if isinstance(game_info, dict) else None
        name = _resolve_entry_name(app_id, steam, options, explicit_name=explicit_name)
        steam = _patch_steam_summary_name(steam, app_id, name)

    if not options:
        return None

    option_notes = [str(opt.get("note")).strip() for opt in options if isinstance(opt.get("note"), str)]
    guide = _build_guide(kind, app_id, str(name), option_notes)

    return {
        "kind": kind,
        "app_id": app_id,
        "name": str(name),
        "steam": steam,
        "options": options,
        "denuvo": app_id in DENUVO_APP_ID_SET,
        "category": category,
        "guide": guide,
    }
