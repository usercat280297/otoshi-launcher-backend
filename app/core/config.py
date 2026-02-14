import os
import sys
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            cleaned = value.strip().strip('"').strip("'")
            os.environ[key] = cleaned
    except OSError:
        return


def _load_env() -> None:
    candidates = []
    
    # PyInstaller frozen mode - check next to EXE and in _MEIPASS
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
        candidates.extend([
            exe_dir / ".env",
            exe_dir / "resources" / ".env",
            exe_dir.parent / ".env",
        ])
        # Also check inside _MEIPASS (bundled .env)
        meipass = Path(getattr(sys, '_MEIPASS', ''))
        if meipass.exists():
            candidates.append(meipass / ".env")
    else:
        # Development mode - relative to config.py
        current = Path(__file__).resolve()
        candidates.extend([
            current.parents[2] / ".env",
            current.parents[3] / ".env",
        ])
    
    for candidate in candidates:
        _load_env_file(candidate)


_load_env()

def _split_env_list(value: str) -> set[str]:
    if not value:
        return set()
    items = []
    for raw in value.split(","):
        cleaned = raw.strip()
        if cleaned:
            items.append(cleaned.lower())
    return set(items)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./otoshi.db")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-prod")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))
SESSION_TTL_SECONDS = int(
    os.getenv("SESSION_TTL_SECONDS", str(ACCESS_TOKEN_EXPIRE_MINUTES * 60))
)
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
ADMIN_SERVER_URL = os.getenv("ADMIN_SERVER_URL", "https://admin.otoshi.com")
ADMIN_EMAILS = _split_env_list(os.getenv("ADMIN_EMAILS", ""))
ADMIN_USERNAMES = _split_env_list(os.getenv("ADMIN_USERNAMES", ""))
ADMIN_USER_IDS = _split_env_list(os.getenv("ADMIN_USER_IDS", ""))
ADMIN_OAUTH_IDS = _split_env_list(os.getenv("ADMIN_OAUTH_IDS", ""))
ADMIN_ONLY_DEVELOPER_PORTAL = os.getenv("ADMIN_ONLY_DEVELOPER_PORTAL", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
LUA_REMOTE_ONLY = os.getenv("LUA_REMOTE_ONLY", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
MANIFEST_REMOTE_ONLY = os.getenv("MANIFEST_REMOTE_ONLY", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))

_DEFAULT_CORS_ORIGINS = (
    "tauri://localhost,http://tauri.localhost,https://tauri.localhost,"
    "http://localhost:5173,http://localhost:5174,http://localhost:5175,"
    "http://localhost:5176,http://localhost:1234,http://127.0.0.1:5173,http://127.0.0.1:5174,"
    "http://127.0.0.1:5175,http://127.0.0.1:5176,http://127.0.0.1:1234"
)

def _normalize_cors(origins: str) -> list[str]:
    items: list[str] = []
    for raw in origins.split(","):
        value = raw.strip()
        if value and value not in items:
            items.append(value)
    return items

_raw_cors = os.getenv("CORS_ORIGINS", _DEFAULT_CORS_ORIGINS)
CORS_ORIGINS = _normalize_cors(_raw_cors)
# Always allow Tauri origins even when CORS_ORIGINS is overridden.
for required_origin in ("tauri://localhost", "https://tauri.localhost", "http://tauri.localhost"):
    if required_origin not in CORS_ORIGINS:
        CORS_ORIGINS.append(required_origin)

_BACKEND_PORT = os.getenv("BACKEND_PORT", "8000").strip() or "8000"
_LOCAL_API_BASE = os.getenv("LOCAL_API_BASE", f"http://127.0.0.1:{_BACKEND_PORT}").strip()

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "http://localhost:5173")
OAUTH_CALLBACK_BASE_URL = os.getenv("OAUTH_CALLBACK_BASE_URL", _LOCAL_API_BASE)
OAUTH_DEBUG_ERRORS = os.getenv("OAUTH_DEBUG_ERRORS", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
OAUTH_STATE_TTL_SECONDS = int(os.getenv("OAUTH_STATE_TTL_SECONDS", "300"))
SETTINGS_STORAGE_PATH = os.getenv("SETTINGS_STORAGE_PATH", "storage/settings.json")

GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")
GOOGLE_OAUTH_AUTH_URL = os.getenv(
    "GOOGLE_OAUTH_AUTH_URL", "https://accounts.google.com/o/oauth2/v2/auth"
)
GOOGLE_OAUTH_TOKEN_URL = os.getenv(
    "GOOGLE_OAUTH_TOKEN_URL", "https://oauth2.googleapis.com/token"
)
GOOGLE_OAUTH_USERINFO_URL = os.getenv(
    "GOOGLE_OAUTH_USERINFO_URL", "https://openidconnect.googleapis.com/v1/userinfo"
)
GOOGLE_OAUTH_SCOPES = os.getenv("GOOGLE_OAUTH_SCOPES", "openid email profile")

EPIC_OAUTH_CLIENT_ID = os.getenv("EPIC_OAUTH_CLIENT_ID", "")
EPIC_OAUTH_CLIENT_SECRET = os.getenv("EPIC_OAUTH_CLIENT_SECRET", "")
EPIC_OAUTH_AUTH_URL = os.getenv(
    "EPIC_OAUTH_AUTH_URL", "https://www.epicgames.com/id/authorize"
)
EPIC_OAUTH_TOKEN_URL = os.getenv(
    "EPIC_OAUTH_TOKEN_URL", "https://api.epicgames.dev/epic/oauth/v1/token"
)
EPIC_OAUTH_USERINFO_URL = os.getenv(
    "EPIC_OAUTH_USERINFO_URL", "https://api.epicgames.dev/epic/oauth/v1/userinfo"
)
EPIC_OAUTH_SCOPES = os.getenv("EPIC_OAUTH_SCOPES", "basic_profile email")

# Discord OAuth
DISCORD_OAUTH_CLIENT_ID = os.getenv("DISCORD_OAUTH_CLIENT_ID", "")
DISCORD_OAUTH_CLIENT_SECRET = os.getenv("DISCORD_OAUTH_CLIENT_SECRET", "")
DISCORD_OAUTH_AUTH_URL = os.getenv(
    "DISCORD_OAUTH_AUTH_URL", "https://discord.com/api/oauth2/authorize"
)
DISCORD_OAUTH_TOKEN_URL = os.getenv(
    "DISCORD_OAUTH_TOKEN_URL", "https://discord.com/api/oauth2/token"
)
DISCORD_OAUTH_USERINFO_URL = os.getenv(
    "DISCORD_OAUTH_USERINFO_URL", "https://discord.com/api/users/@me"
)
DISCORD_OAUTH_SCOPES = os.getenv("DISCORD_OAUTH_SCOPES", "identify email")

STEAM_OPENID_URL = os.getenv("STEAM_OPENID_URL", "https://steamcommunity.com/openid/login")
STEAM_WEB_API_KEY = os.getenv("STEAM_WEB_API_KEY", "")
STEAM_WEB_API_URL = os.getenv("STEAM_WEB_API_URL", "https://api.steampowered.com")
STEAM_STORE_API_URL = os.getenv("STEAM_STORE_API_URL", "https://store.steampowered.com/api")
STEAM_STORE_SEARCH_URL = os.getenv(
    "STEAM_STORE_SEARCH_URL", "https://store.steampowered.com/api/storesearch/"
)
STEAM_CACHE_TTL_SECONDS = int(os.getenv("STEAM_CACHE_TTL_SECONDS", "3600"))
STEAM_CATALOG_CACHE_TTL_SECONDS = int(os.getenv("STEAM_CATALOG_CACHE_TTL_SECONDS", "300"))
STEAM_REQUEST_TIMEOUT_SECONDS = int(os.getenv("STEAM_REQUEST_TIMEOUT_SECONDS", "12"))
STEAM_APPDETAILS_BATCH_SIZE = int(os.getenv("STEAM_APPDETAILS_BATCH_SIZE", "60"))
LUA_FILES_DIR = os.getenv("LUA_FILES_DIR", "")
STEAM_TRENDING_CACHE_TTL_SECONDS = int(os.getenv("STEAM_TRENDING_CACHE_TTL_SECONDS", "900"))
STEAM_TRENDING_LIMIT = int(os.getenv("STEAM_TRENDING_LIMIT", "100"))
STEAM_NEWS_MAX_COUNT = int(os.getenv("STEAM_NEWS_MAX_COUNT", "200"))

STEAMGRIDDB_API_KEY = os.getenv("STEAMGRIDDB_API_KEY", "")
STEAMGRIDDB_BASE_URL = os.getenv("STEAMGRIDDB_BASE_URL", "https://www.steamgriddb.com/api/v2")
STEAMGRIDDB_CACHE_TTL_SECONDS = int(os.getenv("STEAMGRIDDB_CACHE_TTL_SECONDS", "86400"))
STEAMGRIDDB_REQUEST_TIMEOUT_SECONDS = int(os.getenv("STEAMGRIDDB_REQUEST_TIMEOUT_SECONDS", "10"))
STEAMGRIDDB_MAX_CONCURRENCY = int(os.getenv("STEAMGRIDDB_MAX_CONCURRENCY", "4"))
STEAMGRIDDB_PREWARM_ENABLED = os.getenv("STEAMGRIDDB_PREWARM_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAMGRIDDB_PREWARM_LIMIT = int(os.getenv("STEAMGRIDDB_PREWARM_LIMIT", "120"))
STEAMGRIDDB_PREWARM_CONCURRENCY = int(os.getenv("STEAMGRIDDB_PREWARM_CONCURRENCY", "2"))

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN", os.getenv("HF_TOKEN", ""))
HF_REPO_ID = os.getenv("HF_REPO_ID", "MangaVNteam/Assassin-Creed-Odyssey-Crack")
HF_REPO_TYPE = os.getenv("HF_REPO_TYPE", "dataset")
HF_REVISION = os.getenv("HF_REVISION", "main")
HF_STORAGE_BASE_PATH = os.getenv("HF_STORAGE_BASE_PATH", "")
HF_CHUNK_PATH_TEMPLATE = os.getenv("HF_CHUNK_PATH_TEMPLATE", "")
HF_CHUNK_MODE = os.getenv("HF_CHUNK_MODE", "auto")
HF_TIMEOUT_SECONDS = int(os.getenv("HF_TIMEOUT_SECONDS", "120"))
HF_CONNECT_TIMEOUT_SECONDS = int(os.getenv("HF_CONNECT_TIMEOUT_SECONDS", "10"))
HF_MAX_RETRIES = int(os.getenv("HF_MAX_RETRIES", "3"))
HF_RETRY_BACKOFF_SECONDS = float(os.getenv("HF_RETRY_BACKOFF_SECONDS", "1.25"))

REDIS_URL = os.getenv("REDIS_URL", "")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "120"))
RATE_LIMIT_DEFAULT_PER_MINUTE = int(os.getenv("RATE_LIMIT_DEFAULT_PER_MINUTE", "120"))
RATE_LIMIT_LOGIN_PER_MINUTE = int(os.getenv("RATE_LIMIT_LOGIN_PER_MINUTE", "8"))

LAUNCHER_CORE_PATH = os.getenv("LAUNCHER_CORE_PATH", "")
MANIFEST_SOURCE_DIR = os.getenv("MANIFEST_SOURCE_DIR", "")
MANIFEST_CACHE_DIR = os.getenv("MANIFEST_CACHE_DIR", ".manifests")

WORKSHOP_STORAGE_DIR = os.getenv("WORKSHOP_STORAGE_DIR", "storage/workshop")
SCREENSHOT_STORAGE_DIR = os.getenv("SCREENSHOT_STORAGE_DIR", "storage/screenshots")
BUILD_STORAGE_DIR = os.getenv("BUILD_STORAGE_DIR", "storage/builds")
WORKSHOP_STEAM_APP_ID = os.getenv("WORKSHOP_STEAM_APP_ID", "")
WORKSHOP_STEAM_APP_IDS = os.getenv("WORKSHOP_STEAM_APP_IDS", "")
WORKSHOP_STEAM_SOURCE = os.getenv("WORKSHOP_STEAM_SOURCE", "env").lower()
WORKSHOP_STEAM_MAX_APPIDS = int(os.getenv("WORKSHOP_STEAM_MAX_APPIDS", "120"))
WORKSHOP_STEAM_PER_GAME = int(os.getenv("WORKSHOP_STEAM_PER_GAME", "2"))
WORKSHOP_STEAM_LIMIT = int(os.getenv("WORKSHOP_STEAM_LIMIT", "60"))
DISCOVERY_FORCE_STEAM = os.getenv("DISCOVERY_FORCE_STEAM", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ANIME_SOURCE_URL = os.getenv("ANIME_SOURCE_URL", "https://animevietsub.vip/")
ANIME_REQUEST_TIMEOUT_SECONDS = int(os.getenv("ANIME_REQUEST_TIMEOUT_SECONDS", "12"))
ANIME_CACHE_TTL_SECONDS = int(os.getenv("ANIME_CACHE_TTL_SECONDS", "600"))

_DEFAULT_CDN_PRIMARY = f"http://127.0.0.1:{_BACKEND_PORT}"
_DEFAULT_CDN_FALLBACK = f"http://localhost:{_BACKEND_PORT},http://127.0.0.1:{_BACKEND_PORT}"
CDN_PRIMARY_URLS = os.getenv("CDN_PRIMARY_URLS", _DEFAULT_CDN_PRIMARY).split(",")
CDN_FALLBACK_URLS = os.getenv("CDN_FALLBACK_URLS", _DEFAULT_CDN_FALLBACK).split(",")

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

VNPAY_TMN_CODE = os.getenv("VNPAY_TMN_CODE", "")
VNPAY_SECRET_KEY = os.getenv("VNPAY_SECRET_KEY", "")
VNPAY_RETURN_URL = os.getenv("VNPAY_RETURN_URL", f"{_LOCAL_API_BASE}/payments/vnpay/return")
VNPAY_API_URL = os.getenv("VNPAY_API_URL", "https://sandbox.vnpayment.vn/paymentv2/vpcpay.html")
