"""
Auto-sync lua files from admin server or Hugging Face
Downloads and caches lua files in AppData on startup
Fallback to bundled/local lua files if sync fails
"""
import os
import sys
import json
import requests
import zipfile
import shutil
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from ..core.config import ADMIN_SERVER_URL, ADMIN_API_KEY, LUA_REMOTE_ONLY

logger = logging.getLogger(__name__)
HF_REPO = os.getenv("HF_LUA_REPO") or os.getenv("HF_REPO_ID", "otoshi/lua-files")
HF_REVISION = os.getenv("HF_REVISION", "main")
HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN", "")
HF_LUA_ZIP_PATH = os.getenv("HF_LUA_ZIP_PATH", "lua_files/lua_files.zip")
_LUA_ZIP_ONLY_ENV = os.getenv("LUA_ZIP_ONLY")
if _LUA_ZIP_ONLY_ENV is None:
    # Default to extract-once mode (zip-only optional).
    LUA_ZIP_ONLY = False
else:
    LUA_ZIP_ONLY = _LUA_ZIP_ONLY_ENV.lower() in ("1", "true", "yes", "on")
LUA_CACHE_DIR = Path(os.getenv("APPDATA", ".")) / "otoshi_launcher" / "lua_cache"

HF_CDN_URL = "https://huggingface.co"
HF_DATASET_API = f"https://huggingface.co/api/datasets/{HF_REPO}"


class LuaSyncService:
    def __init__(self):
        self.admin_url = ADMIN_SERVER_URL
        self.cache_dir = LUA_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.admin_headers = {"X-API-Key": ADMIN_API_KEY} if ADMIN_API_KEY else {}
        self.hf_headers = {"Authorization": f"Bearer {HUGGINGFACE_TOKEN}"} if HUGGINGFACE_TOKEN else {}

    def sync(self) -> bool:
        """Sync lua files from admin server or Hugging Face if needed"""
        print(f"[LuaSync] Cache dir: {self.cache_dir}")
        print(f"[LuaSync] LUA_REMOTE_ONLY={LUA_REMOTE_ONLY}")
        print(f"[LuaSync] LUA_ZIP_ONLY={LUA_ZIP_ONLY}")
        if LUA_REMOTE_ONLY:
            return self._sync_admin_only()
        try:
            # Try admin server first
            remote_version = self._fetch_remote_version()
            local_version = self._get_local_version()
            print(f"[LuaSync] Admin version={remote_version} local_version={local_version}")
            
            if remote_version and remote_version != local_version:
                logger.info(f"Syncing lua files: {local_version} -> {remote_version}")
                if self._download_lua_bundle(remote_version):
                    return True
            elif remote_version:
                logger.debug(f"Lua files up to date: {local_version}")
                return False
        except Exception as e:
            logger.debug(f"Admin sync failed: {e}")

        # Fallback to Hugging Face
        try:
            return self._sync_from_huggingface()
        except Exception as hf_error:
            print(f"[LuaSync] Hugging Face sync failed: {hf_error}")

        # Final fallback: copy bundled files
        self._ensure_local_lua()
        return False

    def _sync_admin_only(self) -> bool:
        """Sync lua files strictly from admin server (no HF/local fallback)."""
        remote_version = self._fetch_remote_version()
        if not remote_version:
            logger.warning("LUA_REMOTE_ONLY enabled but admin version unavailable")
            return False
        local_version = self._get_local_version()
        if remote_version != local_version:
            logger.info(f"Syncing lua files: {local_version} -> {remote_version}")
            return self._download_lua_bundle(remote_version)
        logger.debug(f"Lua files up to date: {local_version}")
        return False

    def _fetch_remote_version(self) -> Optional[str]:
        """Fetch version from admin server"""
        try:
            resp = requests.get(
                f"{self.admin_url}/api/v1/lua/version",
                headers=self.admin_headers,
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            version = data.get("version")
            if version:
                return str(version).strip()
            return None
        except Exception as e:
            logger.debug(f"Failed to fetch version from admin: {e}")
            return None

    def _download_lua_bundle(self, version: str) -> bool:
        """Download lua bundle from admin server"""
        try:
            logger.info(f"Downloading lua bundle version {version} from admin...")
            resp = requests.get(
                f"{self.admin_url}/api/v1/lua/bundle.zip",
                headers=self.admin_headers,
                timeout=120,
                stream=True
            )
            resp.raise_for_status()

            zip_path = self.cache_dir / "lua_bundle.zip"
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if LUA_ZIP_ONLY:
                if not self._index_zip_appids(zip_path):
                    logger.error("Lua bundle downloaded but no lua files indexed")
                    return False
            else:
                # Extract
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall(self.cache_dir)

                zip_path.unlink()

                if not self._normalize_lua_cache():
                    logger.error("Lua bundle extracted but no lua files found")
                    return False

            # Save version
            (self.cache_dir / "version.txt").write_text(version)
            logger.info("Lua files synced from admin successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to download lua bundle: {e}")
            return False

    def _sync_from_huggingface(self) -> bool:
        """Fallback: Sync lua files from Hugging Face with proper auth"""
        try:
            if self._has_cached_hf_files():
                logger.info("Lua cache already present (HF); skipping download")
                return False

            print(f"[LuaSync] Attempting to sync from Hugging Face: {HF_REPO}@{HF_REVISION}")
            print(f"[LuaSync] HF_LUA_ZIP_PATH={HF_LUA_ZIP_PATH}")
            print(f"[LuaSync] HF token set={bool(HUGGINGFACE_TOKEN)}")

            # Try preferred path first (supports nested lua_files/ folder)
            preferred_paths = []
            if HF_LUA_ZIP_PATH:
                preferred_paths.append(HF_LUA_ZIP_PATH.split("?", 1)[0])
            preferred_paths.append("lua-files.zip")
            preferred_paths.append("lua_files.zip")

            hf_zip_url = ""
            headers = self.hf_headers or {}
            if HF_LUA_ZIP_PATH and HF_LUA_ZIP_PATH.startswith("http"):
                candidate = HF_LUA_ZIP_PATH
                print(f"[LuaSync] Trying lua zip (full url): {candidate}")
                resp = requests.get(candidate, headers=headers, timeout=30, stream=True)
                print(f"[LuaSync] Response: {resp.status_code} for {candidate}")
                if resp.status_code == 200:
                    hf_zip_url = candidate
                resp.close()
            if not hf_zip_url:
                for path in preferred_paths:
                    candidate = f"https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}/{path}"
                    print(f"[LuaSync] Trying lua zip: {candidate}")
                    resp = requests.get(candidate, headers=headers, timeout=30, stream=True)
                    print(f"[LuaSync] Response: {resp.status_code} for {candidate}")
                    if resp.status_code == 200:
                        hf_zip_url = candidate
                        break
                    resp.close()

            if not hf_zip_url:
                # Fall back to auto-discovery in repo tree
                discovered_path = self._find_hf_lua_zip(headers)
                if discovered_path:
                    hf_zip_url = f"https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}/{discovered_path}"
                    print(f"[LuaSync] Retrying with detected lua zip: {hf_zip_url}")

            if not hf_zip_url:
                print("[LuaSync] No lua zip found in Hugging Face repo")
                return False

            resp = requests.get(hf_zip_url, headers=headers, timeout=120, stream=True)

            if resp.status_code == 404:
                alt_path = self._find_hf_lua_zip(headers)
                if alt_path:
                    hf_zip_url = f"https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}/{alt_path}"
                    print(f"[LuaSync] Retrying with detected lua zip: {hf_zip_url}")
                    resp = requests.get(hf_zip_url, headers=headers, timeout=120, stream=True)

            if resp.status_code in (401, 403) and headers.get("Authorization"):
                print("[LuaSync] Hugging Face auth failed - retrying without token")
                resp = requests.get(
                    hf_zip_url,
                    headers={},
                    timeout=120,
                    stream=True
                )
            if resp.status_code in (401, 403):
                print("[LuaSync] Hugging Face authentication failed")
                return False

            resp.raise_for_status()

            zip_path = self.cache_dir / "lua_bundle_hf.zip"
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if LUA_ZIP_ONLY:
                if not self._index_zip_appids(zip_path):
                    print("[LuaSync] Hugging Face bundle downloaded but no lua files indexed")
                    return False
            else:
                # Extract
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall(self.cache_dir)

                zip_path.unlink()

                if not self._normalize_lua_cache():
                    print("[LuaSync] Hugging Face bundle extracted but no lua files found")
                    return False

            # Mark as HF version
            (self.cache_dir / "version.txt").write_text(f"huggingface:{HF_REPO}@{HF_REVISION}")
            print("[LuaSync] Lua files synced from Hugging Face")
            return True
        except requests.exceptions.Timeout:
            print("[LuaSync] Hugging Face request timed out")
            return False
        except Exception as e:
            print(f"[LuaSync] Hugging Face sync failed: {e}")
            return False

    def _find_hf_lua_zip(self, headers: dict) -> Optional[str]:
        """Discover a lua zip file in the HF dataset."""
        api_url = f"https://huggingface.co/api/datasets/{HF_REPO}/tree/{HF_REVISION}"
        resp = requests.get(api_url, headers=headers, timeout=15)
        if resp.status_code in (401, 403) and headers.get("Authorization"):
            resp = requests.get(api_url, headers={}, timeout=15)
        if resp.status_code != 200:
            return None
        try:
            items = resp.json()
        except Exception:
            return None
        candidates = []
        lua_dirs = []
        for item in items:
            path = str(item.get("path") or "")
            lower = path.lower()
            if item.get("type") == "file":
                if lower.endswith(".zip") and "lua" in lower:
                    candidates.append(path)
            elif item.get("type") == "directory":
                if "lua" in lower:
                    lua_dirs.append(path)
        if not candidates:
            # Try to look inside lua-like folders
            for folder in lua_dirs:
                sub_url = f"https://huggingface.co/api/datasets/{HF_REPO}/tree/{HF_REVISION}/{folder}"
                sub_resp = requests.get(sub_url, headers=headers, timeout=15)
                if sub_resp.status_code in (401, 403) and headers.get("Authorization"):
                    sub_resp = requests.get(sub_url, headers={}, timeout=15)
                if sub_resp.status_code != 200:
                    continue
                try:
                    sub_items = sub_resp.json()
                except Exception:
                    continue
                for item in sub_items:
                    if item.get("type") != "file":
                        continue
                    path = str(item.get("path") or "")
                    lower = path.lower()
                    if lower.endswith(".zip") and "lua" in lower:
                        candidates.append(path)
                if candidates:
                    break
            if not candidates:
                return None
        preferred = ["lua-files.zip", "lua_files.zip", "lua.zip"]
        for name in preferred:
            for path in candidates:
                if path.lower().endswith(name):
                    return path
        candidates.sort(key=lambda p: (len(p), p.lower()))
        return candidates[0]

    def _get_local_version(self) -> Optional[str]:
        """Get cached version"""
        version_file = self.cache_dir / "version.txt"
        if version_file.exists():
            try:
                return version_file.read_text().strip()
            except Exception:
                return None
        return None

    def _has_cached_hf_files(self) -> bool:
        """Check if HF lua files are already cached."""
        version = self._get_local_version() or ""
        lua_dir = self.cache_dir / "lua_files"
        if not lua_dir.exists():
            return False
        if LUA_ZIP_ONLY:
            if not (lua_dir / "appids.json").exists():
                return False
        else:
            if not list(lua_dir.glob("*.lua")):
                return False
        if version.startswith(f"huggingface:{HF_REPO}@{HF_REVISION}"):
            return True
        # If files are already present but version metadata is missing,
        # treat as cached to avoid re-sync on every startup.
        if not version:
            return True
        return False

    def _get_bundled_lua_dir(self) -> Optional[Path]:
        """Find bundled lua files (PyInstaller, portable, or dev)"""
        paths_to_check = []

        # 1. PyInstaller bundle (_MEIPASS)
        if getattr(sys, 'frozen', False):
            paths_to_check.append(Path(sys._MEIPASS) / "lua_files")
            paths_to_check.append(Path(sys.executable).parent / "lua_files")

        # 2. Development mode
        paths_to_check.append(Path(__file__).resolve().parents[3] / "lua_files")

        # 3. Relative to backend
        paths_to_check.append(Path("./lua_files"))
        paths_to_check.append(Path("../lua_files"))

        # 4. Next to executable
        if hasattr(sys, 'argv') and sys.argv:
            paths_to_check.append(Path(sys.argv[0]).parent / "lua_files")

        for path in paths_to_check:
            if path.exists() and list(path.glob("*.lua")):
                logger.info(f"Found bundled lua files at: {path}")
                return path

        return None
    
    def _ensure_local_lua(self):
        """Ensure lua files exist locally (copy from bundle if needed)"""
        if LUA_REMOTE_ONLY:
            return
        lua_dir = self.cache_dir / "lua_files"
        
        # If cache already has lua files, we're good
        if lua_dir.exists():
            if list(lua_dir.glob("*.lua")):
                logger.info(f"Using cached lua files from: {lua_dir}")
                return
            if (lua_dir / "appids.json").exists():
                logger.info(f"Using cached lua index from: {lua_dir}")
                return
        
        # Copy from bundled location
        bundled = self._get_bundled_lua_dir()
        if bundled and bundled.exists():
            logger.info(f"Copying bundled lua files from {bundled}")
            lua_dir.mkdir(parents=True, exist_ok=True)
            file_count = 0
            for lua_file in bundled.glob("*.lua"):
                shutil.copy2(lua_file, lua_dir / lua_file.name)
                file_count += 1
            logger.info(f"Copied {file_count} lua files")
            # Mark as bundled version
            (self.cache_dir / "version.txt").write_text("bundled-local")
        else:
            logger.error("No bundled lua files found!")

    def _normalize_lua_cache(self) -> bool:
        """Ensure lua files are placed under cache_dir/lua_files."""
        lua_dir = self.cache_dir / "lua_files"
        lua_dir.mkdir(parents=True, exist_ok=True)
        if list(lua_dir.glob("*.lua")):
            self._write_appid_index(lua_dir)
            return True

        moved = 0
        # Move any lua file from any depth into lua_dir
        for lua_file in self.cache_dir.rglob("*.lua"):
            try:
                if lua_dir in lua_file.parents:
                    continue
                target = lua_dir / lua_file.name
                if target.exists():
                    continue
                shutil.move(str(lua_file), target)
                moved += 1
            except Exception:
                continue

        if moved:
            print(f"[LuaSync] Normalized lua cache: moved {moved} files into {lua_dir}")
        has_files = bool(list(lua_dir.glob("*.lua")))
        if has_files:
            self._write_appid_index(lua_dir)
        return has_files

    def _write_appid_index(self, lua_dir: Path) -> None:
        """Write a cached appid list to avoid expensive directory scans."""
        try:
            appids = []
            seen = set()
            for lua_file in lua_dir.glob("*.lua"):
                stem = lua_file.stem.strip()
                appid = None
                if stem.isdigit():
                    appid = stem
                else:
                    import re
                    match = re.search(r"\d{3,}", stem)
                    if match:
                        appid = match.group(0)
                if appid and appid not in seen:
                    seen.add(appid)
                    appids.append(appid)
            if appids:
                appids = sorted(appids, key=int)
            index_path = lua_dir / "appids.json"
            index_path.write_text(json.dumps(appids), encoding="utf-8")
            print(f"[LuaSync] Wrote appid index: {index_path} ({len(appids)} items)")
        except Exception as e:
            print(f"[LuaSync] Failed to write appid index: {e}")

    def _index_zip_appids(self, zip_path: Path) -> bool:
        """Index appids directly from a lua zip without extracting."""
        try:
            lua_dir = self.cache_dir / "lua_files"
            lua_dir.mkdir(parents=True, exist_ok=True)
            appids = []
            seen = set()
            with zipfile.ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    if not name.lower().endswith(".lua"):
                        continue
                    stem = Path(name).stem.strip()
                    appid = None
                    if stem.isdigit():
                        appid = stem
                    else:
                        import re
                        match = re.search(r"\d{3,}", stem)
                        if match:
                            appid = match.group(0)
                    if appid and appid not in seen:
                        seen.add(appid)
                        appids.append(appid)
            if appids:
                appids = sorted(appids, key=int)
            index_path = lua_dir / "appids.json"
            index_path.write_text(json.dumps(appids), encoding="utf-8")
            print(f"[LuaSync] Wrote appid index from zip: {index_path} ({len(appids)} items)")
            return True if appids else False
        except Exception as e:
            print(f"[LuaSync] Failed to index lua zip: {e}")
            return False

    def get_lua_dir(self) -> Path:
        """Get lua files directory, ensuring it exists"""
        self._ensure_local_lua()
        lua_dir = self.cache_dir / "lua_files"
        lua_dir.mkdir(parents=True, exist_ok=True)
        return lua_dir


# Global instance
_lua_sync = LuaSyncService()


def sync_lua_files() -> bool:
    """Call this on backend startup"""
    return _lua_sync.sync()


def get_lua_files_dir() -> Path:
    """Get lua files directory path"""
    return _lua_sync.get_lua_dir()
