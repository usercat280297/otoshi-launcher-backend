from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import json
import os
from pathlib import Path

from ..db import get_db
from ..models import Game
from ..services.chunk_manifests import get_version_override_for_slug
from ..services.manifest import build_manifest
from ..core.cache import cache_client
from ..core.config import MANIFEST_REMOTE_ONLY
from ..services.remote_game_data import get_manifest_from_server

router = APIRouter()

def get_local_manifest(game_slug: str, version: str = None):
    """Get manifest from local auto_chunk_check_update directory"""
    # Try different paths
    base_paths = [
        Path("auto_chunk_check_update"),
        Path("../auto_chunk_check_update"),
        Path("app/data/manifests")
    ]
    
    for base_path in base_paths:
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

@router.get("/{slug}")
def get_manifest(slug: str, db: Session = Depends(get_db)):
    version_hint = get_version_override_for_slug(slug) or "latest"
    cache_key = f"manifest:{slug}:{version_hint}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        game_id = cached.get("game_id") if isinstance(cached, dict) else None
        if game_id:
            cache_client.set_json(f"manifest:{game_id}", cached)
        return cached

    if MANIFEST_REMOTE_ONLY:
        remote_manifest = get_manifest_from_server(slug)
        if remote_manifest is None:
            game = db.query(Game).filter(Game.slug == slug).first()
            if game:
                remote_manifest = get_manifest_from_server(game.id)
        if remote_manifest is None:
            raise HTTPException(status_code=503, detail="Manifest unavailable from remote server")
        cache_client.set_json(cache_key, remote_manifest)
        cache_client.set_json(f"manifest:{slug}", remote_manifest)
        game_id = remote_manifest.get("game_id")
        if game_id:
            cache_client.set_json(f"manifest:{game_id}", remote_manifest)
        return remote_manifest
    
    # Try to get local manifest first (for games like PEAK)
    if slug.upper() == "PEAK":
        local_manifest = get_local_manifest("PEAK", "1.51a")
        if local_manifest:
            cache_client.set_json(cache_key, local_manifest)
            cache_client.set_json(f"manifest:{slug}", local_manifest)
            return local_manifest
    
    # Fallback to database
    game = db.query(Game).filter(Game.slug == slug).first()
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    manifest = build_manifest(game)
    cache_client.set_json(cache_key, manifest)
    cache_client.set_json(f"manifest:{slug}", manifest)
    cache_client.set_json(f"manifest:{game.id}", manifest)
    return manifest
