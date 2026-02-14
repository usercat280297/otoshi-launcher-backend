from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Game, GameGraphicsConfig
from ..schemas import GameGraphicsConfigIn, GameGraphicsConfigOut
from ..services.graphics_registry import (
    get_registry_entry,
    load_launcher_defaults,
    merge_flags,
    normalize_api,
    normalize_flags,
)

router = APIRouter()


def _extract_app_id(game_id: str) -> str | None:
    raw = (game_id or "").strip()
    if raw.startswith("steam-"):
        raw = raw[6:]
    return raw if raw.isdigit() else None


def _default_overlay(defaults: dict) -> bool:
    value = defaults.get("overlayDefault", True)
    return bool(value)


def _renderer_priority(defaults: dict) -> list[str]:
    priority = defaults.get("rendererPriority")
    if isinstance(priority, list) and priority:
        return [str(item) for item in priority if str(item).strip()]
    return ["dx12", "dx11", "vulkan"]


def _renderer_args(defaults: dict) -> dict[str, list[str]]:
    args = defaults.get("rendererArgs") if isinstance(defaults.get("rendererArgs"), dict) else {}
    return {key: normalize_flags(value) for key, value in args.items()}


def _config_to_out(config: GameGraphicsConfig, source: str = "db") -> GameGraphicsConfigOut:
    return GameGraphicsConfigOut(
        id=config.id,
        game_id=config.game_id,
        dx12_flags=normalize_flags(config.dx12_flags),
        dx11_flags=normalize_flags(config.dx11_flags),
        vulkan_flags=normalize_flags(config.vulkan_flags),
        overlay_enabled=bool(config.overlay_enabled),
        recommended_api=normalize_api(config.recommended_api),
        executable=config.executable,
        game_dir=config.game_dir,
        created_at=config.created_at,
        updated_at=config.updated_at,
        source=source,
    )


def _registry_to_out(
    game_id: str, entry: dict, overlay_default: bool, source: str = "registry"
) -> GameGraphicsConfigOut:
    recommended = None
    if isinstance(entry.get("recommendations"), dict):
        recommended = normalize_api(entry.get("recommendations", {}).get("preferred"))
    return GameGraphicsConfigOut(
        id=None,
        game_id=game_id,
        dx12_flags=normalize_flags(entry.get("dx12")),
        dx11_flags=normalize_flags(entry.get("dx11")),
        vulkan_flags=normalize_flags(entry.get("vulkan")),
        overlay_enabled=bool(entry.get("overlayEnabled", overlay_default)),
        recommended_api=recommended,
        executable=entry.get("executable"),
        game_dir=entry.get("gameDir"),
        source=source,
    )


def _default_out(game_id: str, overlay_default: bool) -> GameGraphicsConfigOut:
    return GameGraphicsConfigOut(
        id=None,
        game_id=game_id,
        dx12_flags=[],
        dx11_flags=[],
        vulkan_flags=[],
        overlay_enabled=overlay_default,
        recommended_api=None,
        executable=None,
        game_dir=None,
        source="default",
    )


@router.get("/{game_id}/graphics-config", response_model=GameGraphicsConfigOut)
def get_graphics_config(game_id: str, db: Session = Depends(get_db)):
    defaults = load_launcher_defaults()
    overlay_default = _default_overlay(defaults)

    app_id = _extract_app_id(game_id)
    if app_id:
        entry = get_registry_entry(app_id)
        if entry:
            return _registry_to_out(game_id, entry, overlay_default)
        return _default_out(game_id, overlay_default)

    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    config = db.query(GameGraphicsConfig).filter(GameGraphicsConfig.game_id == game_id).first()
    if config:
        return _config_to_out(config, source="db")
    return _default_out(game_id, overlay_default)


@router.post("/{game_id}/graphics-config", response_model=GameGraphicsConfigOut)
def upsert_graphics_config(
    game_id: str,
    payload: GameGraphicsConfigIn,
    db: Session = Depends(get_db),
):
    app_id = _extract_app_id(game_id)
    if app_id:
        raise HTTPException(
            status_code=400,
            detail="Steam titles are read-only; update graphics_registry.json instead.",
        )

    game = db.query(Game).filter(Game.id == game_id).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    config = db.query(GameGraphicsConfig).filter(GameGraphicsConfig.game_id == game_id).first()
    if not config:
        config = GameGraphicsConfig(game_id=game_id)
        db.add(config)

    recommended = normalize_api(payload.recommended_api)
    if payload.recommended_api and not recommended:
        raise HTTPException(status_code=400, detail="recommended_api must be dx12, dx11, or vulkan")

    config.dx12_flags = normalize_flags(payload.dx12_flags)
    config.dx11_flags = normalize_flags(payload.dx11_flags)
    config.vulkan_flags = normalize_flags(payload.vulkan_flags)
    config.overlay_enabled = bool(payload.overlay_enabled)
    config.recommended_api = recommended
    config.executable = (payload.executable or "").strip() or None
    config.game_dir = (payload.game_dir or "").strip() or None

    db.commit()
    db.refresh(config)
    return _config_to_out(config, source="db")


@router.get("/{game_id}/launch-config")
def get_launch_config(game_id: str, db: Session = Depends(get_db)):
    defaults = load_launcher_defaults()
    overlay_default = _default_overlay(defaults)
    renderer_priority = _renderer_priority(defaults)
    renderer_args = _renderer_args(defaults)

    app_id = _extract_app_id(game_id)
    entry = get_registry_entry(app_id) if app_id else None

    config = None
    if not app_id:
        game = db.query(Game).filter(Game.id == game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        config = (
            db.query(GameGraphicsConfig).filter(GameGraphicsConfig.game_id == game_id).first()
        )

    flags = {}
    for api in ["dx12", "dx11", "vulkan"]:
        base = renderer_args.get(api, [])
        if config:
            override = getattr(config, f"{api}_flags", [])
        elif entry:
            override = entry.get(api)
        else:
            override = []
        flags[api] = merge_flags(base, override)

    recommended_api = normalize_api(config.recommended_api) if config else None
    if not recommended_api and entry and isinstance(entry.get("recommendations"), dict):
        recommended_api = normalize_api(entry.get("recommendations", {}).get("preferred"))
    if not recommended_api:
        recommended_api = renderer_priority[0] if renderer_priority else "dx12"

    overlay_enabled = bool(config.overlay_enabled) if config else overlay_default
    executable = config.executable if config else (entry.get("executable") if entry else None)
    game_dir = config.game_dir if config else (entry.get("gameDir") if entry else None)

    return {
        "game_id": game_id,
        "app_id": app_id,
        "renderer_priority": renderer_priority,
        "recommended_api": recommended_api,
        "overlay_enabled": overlay_enabled,
        "flags": flags,
        "launch_args": flags.get(recommended_api, []),
        "executable": executable,
        "game_dir": game_dir,
        "registry": {
            "name": entry.get("name"),
            "recommendations": entry.get("recommendations"),
        }
        if entry
        else None,
        "source": "db" if config else ("registry" if entry else "default"),
    }
