from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os

from fastapi import APIRouter, HTTPException, Query

from ..updates.update_manager import UpdateManager

router = APIRouter(prefix="/v2/updates", tags=["v2-updates"])

_STORAGE_ROOT = Path(os.getenv("OTOSHI_STORAGE_DIR", "storage"))
_UPDATES_DIR = _STORAGE_ROOT / "updates"
_UPDATES_DIR.mkdir(parents=True, exist_ok=True)
_manager = UpdateManager(_UPDATES_DIR)


@router.get("/delta")
async def get_update_delta_v2(
    from_version: str = Query(..., alias="from"),
    to_version: str = Query(..., alias="to"),
):
    try:
        patch = await _manager.get_delta_patch(from_version, to_version)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not patch:
        return {
            "from_version": from_version,
            "to_version": to_version,
            "delta_available": False,
            "strategy": "full",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "plan": {
                "mode": "full_download",
                "reason": "delta_not_available",
            },
        }

    added = patch.get("added") if isinstance(patch, dict) else {}
    modified = patch.get("modified") if isinstance(patch, dict) else {}
    removed = patch.get("removed") if isinstance(patch, dict) else []
    changed_count = len(added or {}) + len(modified or {}) + len(removed or [])
    return {
        "from_version": from_version,
        "to_version": to_version,
        "delta_available": True,
        "strategy": "chunk_plus_xdelta",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "plan": {
            "mode": "delta",
            "xdelta_min_file_mb": 64,
            "changed_entries": changed_count,
        },
        "patch": patch,
    }
