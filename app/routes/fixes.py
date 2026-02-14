from __future__ import annotations

from typing import Any, List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from ..schemas import FixCatalogOut, FixEntryDetailOut
from ..services.fixes import (
    get_bypass_by_category,
    get_bypass_catalog,
    get_bypass_categories,
    get_fix_entry_detail,
    get_bypass_option,
    get_online_fix_catalog,
    get_online_fix_options,
)

router = APIRouter()


class CrackInstallGuide(BaseModel):
    """Installation guide for a crack/fix."""
    app_id: str
    name: Optional[str] = None
    steps: List[str]
    warnings: List[str]
    notes: Optional[str] = None


class CrackOptionDetail(BaseModel):
    """Detailed information about a crack option."""
    link: str
    name: Optional[str] = None
    note: Optional[str] = None
    version: Optional[str] = None
    size: Optional[int] = None
    recommended: bool = False
    install_guide: Optional[CrackInstallGuide] = None


@router.get("/online-fix", response_model=FixCatalogOut)
def online_fix_catalog(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
):
    return get_online_fix_catalog(offset=offset, limit=limit)


@router.get("/bypass", response_model=FixCatalogOut)
def bypass_catalog(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
):
    return get_bypass_catalog(offset=offset, limit=limit)


@router.get("/bypass/categories")
def bypass_categories_list() -> List[dict[str, Any]]:
    """Get all bypass categories with game counts."""
    return get_bypass_categories()


@router.get("/bypass/category/{category_id}")
def bypass_by_category(
    category_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
) -> dict[str, Any]:
    """Get bypass games filtered by category (ea, ubisoft, rockstar, denuvo)."""
    return get_bypass_by_category(category_id, offset=offset, limit=limit)


@router.get("/detail/{kind}/{app_id}", response_model=FixEntryDetailOut)
def fix_entry_detail(kind: str, app_id: str):
    """Get detail page payload for a specific online-fix/bypass game."""
    if kind not in {"online-fix", "bypass"}:
        raise HTTPException(status_code=404, detail="Fix type not supported")

    detail = get_fix_entry_detail(kind, app_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Fix entry not found")
    return detail


@router.get("/online-fix/{app_id}/options")
def get_online_fix_app_options(app_id: str) -> List[CrackOptionDetail]:
    """Get all online-fix options for a specific app."""
    options = get_online_fix_options(app_id)
    if not options:
        raise HTTPException(status_code=404, detail="No online-fix options found for this app")

    return [
        CrackOptionDetail(
            link=opt.get("link", ""),
            name=opt.get("name"),
            note=opt.get("note"),
            version=opt.get("version"),
            size=opt.get("size"),
            recommended=opt.get("recommended", False),
            install_guide=CrackInstallGuide(
                app_id=app_id,
                name=opt.get("name"),
                steps=[
                    "Close the game if it's running",
                    "Extract the downloaded archive",
                    "Copy all files to the game installation folder",
                    "Replace existing files when prompted",
                    "Launch the game",
                ],
                warnings=[
                    "Make sure to backup original game files",
                    "Disable antivirus temporarily if needed",
                ],
                notes=opt.get("note"),
            ),
        )
        for opt in options
    ]


@router.get("/bypass/{app_id}/option")
def get_bypass_app_option(app_id: str) -> CrackOptionDetail:
    """Get the bypass option for a specific app."""
    option = get_bypass_option(app_id)
    if not option:
        raise HTTPException(status_code=404, detail="No bypass option found for this app")

    return CrackOptionDetail(
        link=option.get("link", ""),
        name=option.get("name"),
        note=option.get("note"),
        version=option.get("version"),
        size=option.get("size"),
        recommended=option.get("recommended", False),
        install_guide=CrackInstallGuide(
            app_id=app_id,
            name=option.get("name"),
            steps=[
                "Close the game and launcher if running",
                "Extract the bypass files",
                "Copy files to the game installation folder",
                "Run the game through the bypass executable if provided",
            ],
            warnings=[
                "Original files will be backed up automatically",
                "You can restore them by uninstalling the fix",
            ],
            notes=option.get("note"),
        ),
    )


@router.get("/{app_id}/install-guide")
def get_crack_install_guide(app_id: str, fix_type: str = Query("online-fix")) -> CrackInstallGuide:
    """Get installation guide for a crack/fix."""
    normalized_type = "bypass" if fix_type == "bypass" else "online-fix"
    detail = get_fix_entry_detail(normalized_type, app_id)

    if detail and isinstance(detail.get("guide"), dict):
        guide = detail["guide"]
        steps = [
            step.get("description", "")
            for step in guide.get("steps", [])
            if isinstance(step, dict) and step.get("description")
        ]
        warnings = [w for w in guide.get("warnings", []) if isinstance(w, str)]
        notes = "\n".join([n for n in guide.get("notes", []) if isinstance(n, str)]) or None
        return CrackInstallGuide(
            app_id=app_id,
            name=detail.get("name"),
            steps=steps or ["Guide content is not available yet."],
            warnings=warnings,
            notes=notes,
        )

    # Backward-compatible fallback
    if normalized_type == "online-fix":
        options = get_online_fix_options(app_id)
        name = options[0].get("name") if options else None
    else:
        option = get_bypass_option(app_id)
        name = option.get("name") if option else None

    return CrackInstallGuide(
        app_id=app_id,
        name=name,
        steps=[
            "Ensure the game is installed and closed",
            "Download will start automatically",
            "Original files will be backed up",
            "Fix files will be extracted to game directory",
            "Launch the game normally after installation",
        ],
        warnings=[
            "Do not run the game during installation",
            "Antivirus may flag some files - add exception if needed",
            "Make sure you have enough disk space",
        ],
        notes="You can uninstall this fix anytime to restore original files.",
    )

