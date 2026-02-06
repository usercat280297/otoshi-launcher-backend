from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

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


@router.get("/lookup", response_model=SteamGridDBAssetOut)
def lookup_assets(
    title: str | None = Query(None),
    steam_app_id: str | None = Query(None),
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
