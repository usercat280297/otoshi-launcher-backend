from pathlib import Path
import os
import re
import sys
import time
from urllib.parse import unquote
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import threading
from sqlalchemy.exc import OperationalError

from .core.config import (
    DATABASE_URL,
    CORS_ORIGINS,
    GLOBAL_INDEX_V1,
    WORKSHOP_STORAGE_DIR,
    SCREENSHOT_STORAGE_DIR,
    BUILD_STORAGE_DIR,
    STEAM_GLOBAL_INDEX_BOOTSTRAP_ENABLED,
    STEAM_GLOBAL_INDEX_BOOTSTRAP_MIN_TITLES,
    STEAM_GLOBAL_INDEX_BOOTSTRAP_MAX_ITEMS,
    STEAMGRIDDB_PREWARM_CONCURRENCY,
    STEAMGRIDDB_PREWARM_ENABLED,
    STEAMGRIDDB_PREWARM_LIMIT,
)
from .core.denuvo import DENUVO_APP_ID_SET
from .core.cache import cache_client
from .db import Base, engine, SessionLocal
from .models import ChatMessage
from .migrations import ensure_schema
from .seed import seed_games
from .services.steam_catalog import get_lua_appids
from .services.ai_observability import record_http_request, should_track_request
from .services.steamgriddb import prewarm_steamgriddb_cache
from .services.steam_global_index import get_ingest_status, ingest_full_catalog
from .routes import (
    auth,
    games,
    library,
    downloads,
    manifests,
    telemetry,
    cdn,
    users,
    payments,
    licenses,
    friends,
    achievements,
    cloud_saves,
    chat,
    workshop,
    discovery,
    inventory,
    community,
    wishlist,
    store,
    developer,
    remote_downloads,
    streaming,
    steamgriddb,
    steam,
    fixes,
    settings,
    age_gate,
    policy,
    distribute,
    properties,
    launcher_download,
    updates,
    lua_admin,
    graphics,
    launcher_diagnostics,
    manifests_v2,
    downloads_v2,
    self_heal_v2,
    updates_v2,
    cdn_v2,
    steam_index,
    p2p,
    ai,
    privacy,
)
from .websocket import manager
from fastapi import WebSocket, WebSocketDisconnect, status, HTTPException
from fastapi.responses import JSONResponse, Response
from jose import jwt, JWTError
from .core.config import SECRET_KEY, ALGORITHM
from .middleware import AuthMiddleware, RateLimitMiddleware

app = FastAPI(title="Otoshi Launcher API", version="0.1.0")

_LOCAL_ORIGIN_REGEX = re.compile(r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$", re.IGNORECASE)
_TAURI_ORIGIN_REGEX = re.compile(r"^(tauri://localhost|https?://tauri\.localhost)$", re.IGNORECASE)
_BASE_SCHEMA_LOCK = threading.Lock()
_BASE_SCHEMA_READY = False


def _ensure_base_schema() -> None:
    global _BASE_SCHEMA_READY
    if _BASE_SCHEMA_READY:
        return
    with _BASE_SCHEMA_LOCK:
        if _BASE_SCHEMA_READY:
            return
        try:
            Base.metadata.create_all(bind=engine)
        except OperationalError as exc:
            # SQLite schema init may race in portable/multi-start scenarios.
            if "already exists" not in str(exc).lower():
                raise
        _BASE_SCHEMA_READY = True


def _is_origin_allowed(origin: str) -> bool:
    if not origin:
        return False
    if "*" in CORS_ORIGINS or origin in CORS_ORIGINS:
        return True
    return bool(_LOCAL_ORIGIN_REGEX.match(origin) or _TAURI_ORIGIN_REGEX.match(origin))


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom exception handler that adds CORS headers to all HTTP exceptions."""
    origin = request.headers.get("origin", "")
    response = JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
    # Add CORS headers for cross-origin error responses
    if _is_origin_allowed(origin):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
    return response


# Note: Middleware is executed in REVERSE order of addition.
# AuthMiddleware must be added LAST so it runs AFTER CORSMiddleware adds headers.
# Order of execution: AuthMiddleware -> RateLimitMiddleware -> CORSMiddleware -> GZipMiddleware
app.add_middleware(AuthMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$|^tauri://localhost$|^https?://tauri\.localhost$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def ai_observability_middleware(request: Request, call_next):
    path = str(request.url.path or "")
    should_track = should_track_request(path)
    if not should_track:
        return await call_next(request)

    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = int(getattr(response, "status_code", 500) or 500)
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        latency_ms = (time.perf_counter() - started) * 1000.0
        record_http_request(
            path=path,
            method=request.method,
            status_code=status_code,
            latency_ms=latency_ms,
            query_params=dict(request.query_params),
        )


def _ensure_storage_dirs() -> None:
    for path in (WORKSHOP_STORAGE_DIR, SCREENSHOT_STORAGE_DIR, BUILD_STORAGE_DIR):
        if path:
            Path(path).mkdir(parents=True, exist_ok=True)

def _start_lua_sync() -> None:
    try:
        from .services.lua_sync import sync_lua_files, get_lua_files_dir
        result = sync_lua_files()
        lua_dir = get_lua_files_dir()
        lua_count = len(list(lua_dir.glob("*.lua")))
        if result:
            print(f"Lua files synced successfully ({lua_count} files)")
        else:
            print(f"Using local/bundled lua files ({lua_count} files)")
    except Exception as e:
        print(f"Lua sync error: {e} (launcher will continue)")


def _bootstrap_global_index_if_needed() -> None:
    if not GLOBAL_INDEX_V1 or not STEAM_GLOBAL_INDEX_BOOTSTRAP_ENABLED:
        return

    min_titles = max(1, int(STEAM_GLOBAL_INDEX_BOOTSTRAP_MIN_TITLES or 0))
    max_items = int(STEAM_GLOBAL_INDEX_BOOTSTRAP_MAX_ITEMS or 0)
    max_items_value = max_items if max_items > 0 else None

    db = SessionLocal()
    try:
        status = get_ingest_status(db)
        latest_job = status.get("latest_job") or {}
        totals = status.get("totals") or {}
        current_titles = int(totals.get("titles") or 0)
        job_status = str(latest_job.get("status") or "idle").strip().lower() or "idle"

        if job_status == "running":
            print("Global index bootstrap skipped (ingest already running)")
            return

        if current_titles >= min_titles:
            print(
                f"Global index bootstrap skipped (titles={current_titles}, min={min_titles})"
            )
            return

        print(
            f"Global index bootstrap started (titles={current_titles}, min={min_titles})"
        )
        result = ingest_full_catalog(db=db, max_items=max_items_value)
        print(
            "Global index bootstrap completed "
            f"(processed={int(result.get('processed') or 0)}, "
            f"success={int(result.get('success') or 0)}, "
            f"failed={int(result.get('failed') or 0)})"
        )
    except Exception as exc:
        print(f"Global index bootstrap failed: {exc}")
    finally:
        db.close()


def _should_seed_sample_games() -> bool:
    """Seed demo/sample games only when explicitly enabled in packaged builds."""
    raw = os.getenv("SEED_SAMPLE_GAMES")
    if raw is not None:
        return raw.strip().lower() in ("1", "true", "yes", "on")
    # Keep old behavior for local development, disable by default for frozen/packaged app.
    return not getattr(sys, "frozen", False)


@app.on_event("startup")
def on_startup() -> None:
    _ensure_base_schema()
    ensure_schema()
    cache_client.connect()
    _ensure_storage_dirs()

    # Sync lua files in background to avoid blocking startup/port scan
    threading.Thread(target=_start_lua_sync, daemon=True).start()
    if GLOBAL_INDEX_V1 and STEAM_GLOBAL_INDEX_BOOTSTRAP_ENABLED:
        threading.Thread(target=_bootstrap_global_index_if_needed, daemon=True).start()

    if _should_seed_sample_games():
        db = SessionLocal()
        try:
            seed_games(db)
        finally:
            db.close()

    if STEAMGRIDDB_PREWARM_ENABLED:
        appids = get_lua_appids()
        denuvo = [app_id for app_id in appids if app_id in DENUVO_APP_ID_SET]
        remaining = [app_id for app_id in appids if app_id not in DENUVO_APP_ID_SET]
        limit = STEAMGRIDDB_PREWARM_LIMIT if STEAMGRIDDB_PREWARM_LIMIT > 0 else len(appids)
        if limit <= len(denuvo):
            prewarm_ids = denuvo
        else:
            prewarm_ids = denuvo + remaining[: max(0, limit - len(denuvo))]
        thread = threading.Thread(
            target=prewarm_steamgriddb_cache,
            args=(prewarm_ids, STEAMGRIDDB_PREWARM_CONCURRENCY),
            daemon=True,
        )
        thread.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    cache_client.disconnect()


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "news_enhanced": True,
        "cdn_chunk_size_limit_bytes": 2 * 1024 * 1024 * 1024,
    }


@app.head("/health")
def health_check_head():
    return Response(status_code=200)


def _resolve_runtime_db_path(database_url: str) -> str | None:
    if not database_url.lower().startswith("sqlite:///"):
        return None
    try:
        raw = database_url[len("sqlite:///") :]
        decoded = unquote(raw)
        # Normalize URLs like /C:/path on Windows to C:/path.
        if decoded.startswith("/") and len(decoded) >= 3 and decoded[2] == ":":
            decoded = decoded[1:]
        return str(Path(decoded))
    except Exception:
        return None


@app.get("/health/runtime")
def health_runtime():
    db_path = _resolve_runtime_db_path(DATABASE_URL)
    ingest_status = {}
    ingest_state = "idle"
    last_error = None
    try:
        with SessionLocal() as db:
            ingest_status = get_ingest_status(db)
            ingest_state = (
                str((ingest_status.get("latest_job") or {}).get("status") or "idle").strip()
                or "idle"
            )
            last_error = (ingest_status.get("latest_job") or {}).get("error_message")
    except Exception as exc:
        ingest_state = "error"
        last_error = str(exc)

    index_mode = os.getenv("OTOSHI_INDEX_MODE", "hybrid").strip().lower() or "hybrid"
    runtime_mode = os.getenv("OTOSHI_RUNTIME_MODE", "installer").strip().lower() or "installer"

    return {
        "status": "ok",
        "sidecar_ready": True,
        "runtime_mode": runtime_mode,
        "index_mode": index_mode,
        "cdn_chunk_size_limit_bytes": 2 * 1024 * 1024 * 1024,
        "global_index_v1": bool(GLOBAL_INDEX_V1),
        "db_path": db_path,
        "db_exists": bool(db_path and Path(db_path).exists()),
        "ingest_state": ingest_state,
        "ingest_status": ingest_status,
        "last_error": last_error,
    }


app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(games.router, prefix="/games", tags=["games"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(library.router, prefix="/library", tags=["library"])
app.include_router(downloads.router, prefix="/downloads", tags=["downloads"])
app.include_router(payments.router, prefix="/payments", tags=["payments"])
app.include_router(licenses.router, prefix="/licenses", tags=["licenses"])
app.include_router(friends.router, prefix="/friends", tags=["friends"])
app.include_router(achievements.router, prefix="/achievements", tags=["achievements"])
app.include_router(cloud_saves.router, prefix="/cloud-saves", tags=["cloud-saves"])
app.include_router(chat.router, prefix="/chat", tags=["chat"])
app.include_router(manifests.router, prefix="/manifests", tags=["manifests"])
app.include_router(cdn.router, prefix="/cdn", tags=["cdn"])
app.include_router(telemetry.router, prefix="/telemetry", tags=["telemetry"])
app.include_router(workshop.router, prefix="/workshop", tags=["workshop"])
app.include_router(discovery.router, prefix="/discovery", tags=["discovery"])
app.include_router(inventory.router, prefix="/inventory", tags=["inventory"])
app.include_router(community.router, prefix="/community", tags=["community"])
app.include_router(wishlist.router, prefix="/wishlist", tags=["wishlist"])
app.include_router(store.router, prefix="/store", tags=["store"])
app.include_router(developer.router, prefix="/developer", tags=["developer"])
app.include_router(remote_downloads.router, prefix="/remote-downloads", tags=["remote-downloads"])
app.include_router(streaming.router, prefix="/streaming", tags=["streaming"])
app.include_router(steamgriddb.router, prefix="/steamgriddb", tags=["steamgriddb"])
app.include_router(steam.router, prefix="/steam", tags=["steam"])
app.include_router(fixes.router, prefix="/fixes", tags=["fixes"])
app.include_router(settings.router, prefix="/settings", tags=["settings"])
app.include_router(age_gate.router, prefix="/age-gate", tags=["age-gate"])
app.include_router(policy.router, prefix="/policy", tags=["policy"])
app.include_router(distribute.router, prefix="/distribute", tags=["distribute"])
app.include_router(properties.router, prefix="/properties", tags=["properties"])
app.include_router(launcher_download.router, prefix="/launcher-download", tags=["launcher-download"])
app.include_router(updates.router)
app.include_router(lua_admin.router)
app.include_router(graphics.router, prefix="/games", tags=["graphics"])
app.include_router(launcher_diagnostics.router)
app.include_router(manifests_v2.router)
app.include_router(downloads_v2.router)
app.include_router(self_heal_v2.router)
app.include_router(updates_v2.router)
app.include_router(cdn_v2.router)
app.include_router(steam_index.router, prefix="/steam/index", tags=["steam-index"])
app.include_router(p2p.router, prefix="/p2p", tags=["p2p"])
app.include_router(ai.router, prefix="/ai", tags=["ai"])
app.include_router(privacy.router, prefix="/privacy", tags=["privacy"])


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
    except JWTError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await manager.connect(websocket, user_id)
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "heartbeat":
                await websocket.send_json({"type": "pong"})
            elif data.get("type") == "broadcast":
                await manager.broadcast(data.get("payload", {}))
            elif data.get("type") == "chat_message":
                recipient_id = data.get("recipient_id")
                body = data.get("body")
                if recipient_id and body:
                    with SessionLocal() as db:
                        db.add(ChatMessage(sender_id=user_id, recipient_id=recipient_id, body=body))
                        db.commit()
                    await manager.send_to_user(
                        recipient_id,
                        {
                            "type": "chat_message",
                            "sender_id": user_id,
                            "body": body,
                        },
                    )
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
