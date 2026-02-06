from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from ..core.cache import cache_client
from ..core.config import RATE_LIMIT_DEFAULT_PER_MINUTE, RATE_LIMIT_LOGIN_PER_MINUTE, CORS_ORIGINS


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
        path = request.url.path
        if path.startswith("/auth/login"):
            limit = RATE_LIMIT_LOGIN_PER_MINUTE
        else:
            limit = RATE_LIMIT_DEFAULT_PER_MINUTE

        allowed = cache_client.check_rate_limit(
            f"{client_ip}:{path}",
            limit,
            window_seconds=60,
        )
        if not allowed:
            response = JSONResponse(
                status_code=429,
                content={"detail": "Too many requests"},
            )
            return _add_cors_headers(response, request)

        return await call_next(request)
