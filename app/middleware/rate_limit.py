from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from ..core.cache import cache_client
from ..core.config import (
    AI_WRITE_MAX_BODY_BYTES,
    CORS_ORIGINS,
    RATE_LIMIT_AI_WRITE_PER_MINUTE,
    RATE_LIMIT_DEFAULT_PER_MINUTE,
    RATE_LIMIT_LOGIN_PER_MINUTE,
    RATE_LIMIT_PRIVACY_WRITE_PER_MINUTE,
    RATE_LIMIT_STEAM_CATALOG_PER_MINUTE,
)

_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_AI_WRITE_PATHS = (
    "/ai/search/events",
    "/ai/recommendations/impression",
    "/ai/recommendations/feedback",
    "/ai/support/suggest",
    "/ai/anti-cheat/signals",
)


def _is_ai_write_request(method: str, path: str) -> bool:
    if method not in _WRITE_METHODS:
        return False
    return any(path.startswith(prefix) for prefix in _AI_WRITE_PATHS)


def _resolve_limit(method: str, path: str) -> int:
    if path.startswith("/auth/login"):
        return RATE_LIMIT_LOGIN_PER_MINUTE
    if _is_ai_write_request(method, path):
        return RATE_LIMIT_AI_WRITE_PER_MINUTE
    if method in _WRITE_METHODS and path.startswith("/privacy/"):
        return RATE_LIMIT_PRIVACY_WRITE_PER_MINUTE
    if method == "GET" and path.startswith("/steam/catalog"):
        return RATE_LIMIT_STEAM_CATALOG_PER_MINUTE
    return RATE_LIMIT_DEFAULT_PER_MINUTE


def _add_cors_headers(response: JSONResponse, request: Request) -> JSONResponse:
    """Add CORS headers to error responses."""
    origin = request.headers.get("origin", "")
    if origin in CORS_ORIGINS or "*" in CORS_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
    return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for preflight OPTIONS requests
        if request.method == "OPTIONS":
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        method = request.method
        path = request.url.path

        # Fast reject oversized AI write payloads before reading request body.
        if _is_ai_write_request(method, path):
            raw_length = request.headers.get("content-length", "").strip()
            if raw_length:
                try:
                    content_length = int(raw_length)
                except ValueError:
                    content_length = 0
                if content_length > max(0, int(AI_WRITE_MAX_BODY_BYTES or 0)):
                    response = JSONResponse(
                        status_code=413,
                        content={"detail": "Payload too large"},
                    )
                    return _add_cors_headers(response, request)

        limit = _resolve_limit(method, path)

        allowed = cache_client.check_rate_limit(
            f"{client_ip}:{method}:{path}",
            limit,
            window_seconds=60,
        )
        if not allowed:
            response = JSONResponse(
                status_code=429,
                content={"detail": "Too many requests"},
            )
            response.headers["Retry-After"] = "60"
            return _add_cors_headers(response, request)

        return await call_next(request)
