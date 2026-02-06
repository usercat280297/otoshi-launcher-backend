"""
Remote Game Data Service
Fetches lua files and manifests from admin server instead of bundling locally
"""
import requests
from pathlib import Path
from typing import Optional, List
from ..core.cache import cache_client
from ..core.config import ADMIN_SERVER_URL

# Admin server URL (should be set via environment variable)
GAME_DATA_CACHE_TTL = 3600  # 1 hour


def get_lua_appids_from_server() -> List[str]:
    """Fetch list of available game appids from admin server"""
    cache_key = "remote:lua_appids"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached
    
    try:
        response = requests.get(
            f"{ADMIN_SERVER_URL}/api/v1/games/appids",
            timeout=10,
            headers={"User-Agent": "otoshi-launcher/1.0"}
        )
        if response.status_code == 200:
            appids = response.json().get("appids", [])
            cache_client.set_json(cache_key, appids, ttl=GAME_DATA_CACHE_TTL)
            return appids
    except Exception as e:
        print(f"Failed to fetch appids from server: {e}")
    
    return []


def get_lua_file_from_server(appid: str) -> Optional[str]:
    """Fetch lua file content for specific appid from admin server"""
    cache_key = f"remote:lua:{appid}"
    cached = cache_client.get(cache_key)
    if cached:
        return cached.decode('utf-8')
    
    try:
        response = requests.get(
            f"{ADMIN_SERVER_URL}/api/v1/games/{appid}/lua",
            timeout=10,
            headers={"User-Agent": "otoshi-launcher/1.0"}
        )
        if response.status_code == 200:
            content = response.text
            cache_client.set(cache_key, content.encode('utf-8'), ttl=GAME_DATA_CACHE_TTL)
            return content
    except Exception as e:
        print(f"Failed to fetch lua file for {appid}: {e}")
    
    return None


def get_manifest_from_server(appid: str) -> Optional[dict]:
    """Fetch game manifest from admin server"""
    cache_key = f"remote:manifest:{appid}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached
    
    try:
        response = requests.get(
            f"{ADMIN_SERVER_URL}/api/v1/games/{appid}/manifest",
            timeout=10,
            headers={"User-Agent": "otoshi-launcher/1.0"}
        )
        if response.status_code == 200:
            manifest = response.json()
            cache_client.set_json(cache_key, manifest, ttl=GAME_DATA_CACHE_TTL)
            return manifest
    except Exception as e:
        print(f"Failed to fetch manifest for {appid}: {e}")
    
    return None
