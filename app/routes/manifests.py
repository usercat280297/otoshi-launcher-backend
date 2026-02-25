from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import json
import os
from pathlib import Path

from ..db import get_db
from ..models import Game, User
from ..services.chunk_manifests import get_version_override_for_slug
from ..services.download_source_policy import (
    apply_manifest_source_policy,
    decide_download_source_policy,
    is_vip_identity,
)
from ..services.manifest import build_manifest
from ..core.cache import cache_client
from ..core.config import MANIFEST_REMOTE_ONLY
from ..services.remote_game_data import get_manifest_from_server
from .deps import get_current_user_optional

router = APIRouter()
_MANIFEST_CACHE_TTL_SECONDS = 24 * 60 * 60


def _is_runtime_manifest(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    required_top = ("game_id", "slug", "version", "build_id", "chunk_size", "files")
    if any(key not in payload for key in required_top):
        return False
    files = payload.get("files")
    if not isinstance(files, list):
        return False
    for item in files:
        if not isinstance(item, dict):
            return False
        if "path" not in item or "size" not in item or "hash" not in item or "chunks" not in item:
            return False
    return True

def get_local_manifest(game_slug: str, version: str = None):
    """Get manifest from local auto_chunk_check_update directory"""
    env_manifest_root = os.getenv("CHUNK_MANIFEST_DIR", "").strip()
    env_path = Path(env_manifest_root) if env_manifest_root else None

    # Try different paths
    base_paths = [
        env_path,
        Path("auto_chunk_check_update"),
        Path("../auto_chunk_check_update"),
        Path("app/data/manifests")
    ]
    
    for base_path in base_paths:
        if base_path is None:
            continue
        if not base_path.exists():
            continue
            
        # Try different manifest file patterns
        manifest_files = [
            base_path / game_slug / f"manifest_{game_slug}_{version}.json" if version else None,
            base_path / game_slug / f"manifest_{version}.json" if version else None,
            base_path / game_slug / "manifest.json",
        ]
        
        # Filter out None values
        manifest_files = [f for f in manifest_files if f is not None]
        
        for manifest_file in manifest_files:
            if manifest_file.exists():
                try:
                    with open(manifest_file, 'r', encoding='utf-8') as f:
                        manifest = json.load(f)
                        manifest["source"] = "local"
                        manifest["path"] = str(manifest_file)
                        return manifest
                except Exception as e:
                    print(f"Error reading {manifest_file}: {e}")
                    continue
    
    return None


def _manifest_size_bytes(payload: dict) -> Optional[int]:
    for key in ("compressed_size", "total_size", "size_bytes"):
        value = payload.get(key)
        try:
            if value is None:
                continue
            parsed = int(value)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            continue
    return None


def _with_source_policy(
    payload: dict,
    *,
    current_user: Optional[User],
    method: Optional[str],
) -> dict:
    decision = decide_download_source_policy(
        game_size_bytes=_manifest_size_bytes(payload),
        is_vip=is_vip_identity(current_user),
        method=method,
    )
    rewritten, _ = apply_manifest_source_policy(payload, decision=decision)
    if method:
        source_policy = rewritten.get("source_policy")
        if isinstance(source_policy, dict):
            source_policy["requested_method"] = method
    return rewritten

@router.get("/{slug}")
def get_manifest(
    slug: str,
    method: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    version_hint = get_version_override_for_slug(slug) or "latest"
    cache_key = f"manifest:{slug}:{version_hint}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        if _is_runtime_manifest(cached):
            game_id = cached.get("game_id") if isinstance(cached, dict) else None
            if game_id:
                cache_client.set_json(
                    f"manifest:{game_id}",
                    cached,
                    ttl=_MANIFEST_CACHE_TTL_SECONDS,
                )
            return _with_source_policy(cached, current_user=current_user, method=method)
        # Drop stale/incompatible cache payloads from older builds.
        cache_client.delete(cache_key)
        cache_client.delete(f"manifest:{slug}")
        if isinstance(cached, dict):
            cached_game_id = cached.get("game_id")
            if cached_game_id:
                cache_client.delete(f"manifest:{cached_game_id}")

    if MANIFEST_REMOTE_ONLY:
        remote_manifest = get_manifest_from_server(slug)
        if remote_manifest is None:
            game = db.query(Game).filter(Game.slug == slug).first()
            if game:
                remote_manifest = get_manifest_from_server(game.id)
        if remote_manifest is None:
            raise HTTPException(status_code=503, detail="Manifest unavailable from remote server")
        cache_client.set_json(cache_key, remote_manifest, ttl=_MANIFEST_CACHE_TTL_SECONDS)
        cache_client.set_json(
            f"manifest:{slug}",
            remote_manifest,
            ttl=_MANIFEST_CACHE_TTL_SECONDS,
        )
        game_id = remote_manifest.get("game_id")
        if game_id:
            cache_client.set_json(
                f"manifest:{game_id}",
                remote_manifest,
                ttl=_MANIFEST_CACHE_TTL_SECONDS,
            )
        return _with_source_policy(remote_manifest, current_user=current_user, method=method)

    # Fallback to database
    game = db.query(Game).filter(Game.slug == slug).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    manifest = build_manifest(game)
    cache_client.set_json(cache_key, manifest, ttl=_MANIFEST_CACHE_TTL_SECONDS)
    cache_client.set_json(
        f"manifest:{slug}",
        manifest,
        ttl=_MANIFEST_CACHE_TTL_SECONDS,
    )
    cache_client.set_json(
        f"manifest:{game.id}",
        manifest,
        ttl=_MANIFEST_CACHE_TTL_SECONDS,
    )
    return _with_source_policy(manifest, current_user=current_user, method=method)
