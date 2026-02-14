from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ..services.launcher_diagnostics import (
    run_anticheat_compatibility_check,
    run_first_run_diagnostics,
    run_launcher_health_check,
    run_system_check,
)

router = APIRouter(prefix="/launcher-diagnostics", tags=["launcher-diagnostics"])


class FirstRunPayload(BaseModel):
    requirements: Optional[Dict[str, Any]] = None
    install_path: Optional[str] = None
    preload_limit: int = Field(default=48, ge=1, le=500)


@router.get("/system")
def system_check(install_path: Optional[str] = None):
    return run_system_check(requirements=None, install_path=install_path)


@router.get("/health")
def launcher_health():
    return run_launcher_health_check()


@router.get("/anti-cheat")
def anti_cheat():
    return run_anticheat_compatibility_check()


@router.post("/first-run")
def first_run(payload: FirstRunPayload):
    return run_first_run_diagnostics(
        requirements=payload.requirements,
        install_path=payload.install_path,
        preload_limit=payload.preload_limit,
    )

