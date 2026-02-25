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

def _default_database_url() -> str:
    explicit_path = os.getenv("OTOSHI_DB_PATH", "").strip()
    if explicit_path:
        return f"sqlite:///{Path(explicit_path).as_posix()}"

    runtime_mode = os.getenv("OTOSHI_RUNTIME_MODE", "installer").strip().lower() or "installer"
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if runtime_mode == "portable":
            portable_db = (exe_dir / "data" / "cache" / "otoshi.db").resolve()
            return f"sqlite:///{portable_db.as_posix()}"

        program_data = os.getenv("ProgramData", "").strip()
        if program_data:
            installer_db = (Path(program_data) / "Otoshi" / "cache" / "otoshi.db").resolve()
            return f"sqlite:///{installer_db.as_posix()}"

        fallback_db = (exe_dir / "cache" / "otoshi.db").resolve()
        return f"sqlite:///{fallback_db.as_posix()}"

    current = Path(__file__).resolve()
    backend_root = current.parents[2]
    dev_db = (backend_root / "otoshi.db").resolve()
    return f"sqlite:///{dev_db.as_posix()}"


DATABASE_URL = os.getenv("DATABASE_URL", _default_database_url())
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
GLOBAL_INDEX_V1 = os.getenv("GLOBAL_INDEX_V1", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_INGEST_BATCH = int(os.getenv("STEAM_GLOBAL_INDEX_INGEST_BATCH", "500"))
STEAM_GLOBAL_INDEX_DETAILS_BATCH = int(os.getenv("STEAM_GLOBAL_INDEX_DETAILS_BATCH", "80"))
STEAM_GLOBAL_INDEX_SEARCH_LIMIT = int(os.getenv("STEAM_GLOBAL_INDEX_SEARCH_LIMIT", "200"))
STEAM_GLOBAL_INDEX_MAX_PREFETCH = int(os.getenv("STEAM_GLOBAL_INDEX_MAX_PREFETCH", "500"))
STEAM_GLOBAL_INDEX_EPIC_CONFIDENCE_THRESHOLD = float(
    os.getenv("STEAM_GLOBAL_INDEX_EPIC_CONFIDENCE_THRESHOLD", "0.86")
)
STEAM_GLOBAL_INDEX_ENFORCE_COMPLETE = os.getenv(
    "STEAM_GLOBAL_INDEX_ENFORCE_COMPLETE", "true"
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_COMPLETION_BATCH = int(
    os.getenv("STEAM_GLOBAL_INDEX_COMPLETION_BATCH", "0")
)
STEAM_GLOBAL_INDEX_BOOTSTRAP_ENABLED = os.getenv(
    "STEAM_GLOBAL_INDEX_BOOTSTRAP_ENABLED", "true"
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_BOOTSTRAP_MIN_TITLES = int(
    os.getenv("STEAM_GLOBAL_INDEX_BOOTSTRAP_MIN_TITLES", "500")
)
STEAM_GLOBAL_INDEX_BOOTSTRAP_MAX_ITEMS = int(
    os.getenv("STEAM_GLOBAL_INDEX_BOOTSTRAP_MAX_ITEMS", "0")
)
STEAM_GLOBAL_INDEX_BOOTSTRAP_REQUIRE_API_KEY = os.getenv(
    "STEAM_GLOBAL_INDEX_BOOTSTRAP_REQUIRE_API_KEY", "true"
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_AUTOSYNC_ENABLED = os.getenv(
    "STEAM_GLOBAL_INDEX_AUTOSYNC_ENABLED", "true"
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_AUTOSYNC_INTERVAL_SECONDS = int(
    os.getenv("STEAM_GLOBAL_INDEX_AUTOSYNC_INTERVAL_SECONDS", "900")
)
STEAM_GLOBAL_INDEX_AUTOSYNC_INITIAL_DELAY_SECONDS = int(
    os.getenv("STEAM_GLOBAL_INDEX_AUTOSYNC_INITIAL_DELAY_SECONDS", "60")
)
STEAM_GLOBAL_INDEX_AUTOSYNC_MAX_ITEMS = int(
    os.getenv("STEAM_GLOBAL_INDEX_AUTOSYNC_MAX_ITEMS", "0")
)
STEAM_GLOBAL_INDEX_AUTOSYNC_ENRICH_DETAILS = os.getenv(
    "STEAM_GLOBAL_INDEX_AUTOSYNC_ENRICH_DETAILS", "false"
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_AUTOSYNC_REQUIRE_API_KEY = os.getenv(
    "STEAM_GLOBAL_INDEX_AUTOSYNC_REQUIRE_API_KEY", "true"
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GLOBAL_INDEX_AUTOSYNC_TARGET_MIN_TITLES = int(
    os.getenv("STEAM_GLOBAL_INDEX_AUTOSYNC_TARGET_MIN_TITLES", "215000")
)
STEAM_GLOBAL_INDEX_RUNNING_STALE_SECONDS = int(
    os.getenv("STEAM_GLOBAL_INDEX_RUNNING_STALE_SECONDS", "1800")
)
STEAM_GO_CRAWLER_ENABLED = os.getenv("STEAM_GO_CRAWLER_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAM_GO_CRAWLER_BIN = os.getenv("STEAM_GO_CRAWLER_BIN", "")
STEAM_GO_CRAWLER_TIMEOUT_SECONDS = int(os.getenv("STEAM_GO_CRAWLER_TIMEOUT_SECONDS", "30"))
STEAMDB_ENRICHMENT_ENABLED = os.getenv("STEAMDB_ENRICHMENT_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STEAMDB_BASE_URL = os.getenv("STEAMDB_BASE_URL", "https://steamdb.info")
STEAMDB_REQUEST_TIMEOUT_SECONDS = int(os.getenv("STEAMDB_REQUEST_TIMEOUT_SECONDS", "12"))
STEAMDB_ENRICHMENT_MAX_ITEMS = int(os.getenv("STEAMDB_ENRICHMENT_MAX_ITEMS", "2500"))
CROSS_STORE_MAPPING_ENABLED = os.getenv("CROSS_STORE_MAPPING_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
CROSS_STORE_MAPPING_MIN_CONFIDENCE = float(
    os.getenv("CROSS_STORE_MAPPING_MIN_CONFIDENCE", "0.62")
)
EPIC_CATALOG_FREE_GAMES_URL = os.getenv(
    "EPIC_CATALOG_FREE_GAMES_URL",
    "https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions",
)
EPIC_CATALOG_COUNTRY = os.getenv("EPIC_CATALOG_COUNTRY", "US")
EPIC_CATALOG_LOCALE = os.getenv("EPIC_CATALOG_LOCALE", "en-US")
ARTWORK_CACHE_SOFT_RATIO = float(os.getenv("ARTWORK_CACHE_SOFT_RATIO", "0.10"))
ARTWORK_CACHE_HARD_CEILING_GB = int(os.getenv("ARTWORK_CACHE_HARD_CEILING_GB", "1024"))

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
RATE_LIMIT_STEAM_CATALOG_PER_MINUTE = int(
    os.getenv("RATE_LIMIT_STEAM_CATALOG_PER_MINUTE", "3000")
)
RATE_LIMIT_AI_WRITE_PER_MINUTE = int(
    os.getenv("RATE_LIMIT_AI_WRITE_PER_MINUTE", "240")
)
RATE_LIMIT_PRIVACY_WRITE_PER_MINUTE = int(
    os.getenv("RATE_LIMIT_PRIVACY_WRITE_PER_MINUTE", "90")
)
AI_WRITE_MAX_BODY_BYTES = int(os.getenv("AI_WRITE_MAX_BODY_BYTES", "131072"))
AI_SEARCH_EVENTS_MAX_BATCH = int(os.getenv("AI_SEARCH_EVENTS_MAX_BATCH", "100"))

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

DOWNLOAD_SOURCE_POLICY_ENABLED = os.getenv("DOWNLOAD_SOURCE_POLICY_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
DOWNLOAD_BIG_GAME_THRESHOLD_BYTES = int(
    os.getenv("DOWNLOAD_BIG_GAME_THRESHOLD_BYTES", str(50 * 1024 * 1024 * 1024))
)
DOWNLOAD_SOURCE_POLICY_SCOPE = (
    os.getenv("DOWNLOAD_SOURCE_POLICY_SCOPE", "vip_only").strip().lower() or "vip_only"
)

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

VNPAY_TMN_CODE = os.getenv("VNPAY_TMN_CODE", "")
VNPAY_SECRET_KEY = os.getenv("VNPAY_SECRET_KEY", "")
VNPAY_RETURN_URL = os.getenv("VNPAY_RETURN_URL", f"{_LOCAL_API_BASE}/payments/vnpay/return")
VNPAY_API_URL = os.getenv("VNPAY_API_URL", "https://sandbox.vnpayment.vn/paymentv2/vpcpay.html")

# AI platform feature flags
AI_FEATURE_SEARCH_HYBRID = os.getenv("AI_FEATURE_SEARCH_HYBRID", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
AI_FEATURE_RECO_V2 = os.getenv("AI_FEATURE_RECO_V2", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
AI_FEATURE_SUPPORT_COPILOT = os.getenv("AI_FEATURE_SUPPORT_COPILOT", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
AI_FEATURE_ANTI_CHEAT_RISK = os.getenv("AI_FEATURE_ANTI_CHEAT_RISK", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
AI_SEARCH_DEFAULT_MODE = os.getenv("AI_SEARCH_DEFAULT_MODE", "lexical").strip().lower() or "lexical"
AI_SEARCH_VECTOR_DIM = int(os.getenv("AI_SEARCH_VECTOR_DIM", "128"))
AI_SEARCH_MAX_CANDIDATES = int(os.getenv("AI_SEARCH_MAX_CANDIDATES", "320"))
AI_PRIVACY_DEFAULT_DENY = os.getenv("AI_PRIVACY_DEFAULT_DENY", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# AI gateway/provider settings
AI_GATEWAY_CACHE_TTL_SECONDS = int(os.getenv("AI_GATEWAY_CACHE_TTL_SECONDS", "900"))
AI_GATEWAY_MAX_REQUESTS_PER_MINUTE = int(os.getenv("AI_GATEWAY_MAX_REQUESTS_PER_MINUTE", "120"))
AI_BUDGET_MONTHLY_USD = float(os.getenv("AI_BUDGET_MONTHLY_USD", "0"))
AI_PROVIDER_ORDER = [
    item.strip().lower()
    for item in os.getenv("AI_PROVIDER_ORDER", "gemini,github_models,ollama").split(",")
    if item.strip()
]

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_API_BASE_URL = os.getenv(
    "GEMINI_API_BASE_URL",
    "https://generativelanguage.googleapis.com/v1beta",
)
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

GITHUB_MODELS_API_KEY = os.getenv("GITHUB_MODELS_API_KEY", os.getenv("GITHUB_TOKEN", ""))
GITHUB_MODELS_BASE_URL = os.getenv(
    "GITHUB_MODELS_BASE_URL",
    "https://models.inference.ai.azure.com",
)
GITHUB_MODELS_MODEL = os.getenv("GITHUB_MODELS_MODEL", "gpt-4.1-mini")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
