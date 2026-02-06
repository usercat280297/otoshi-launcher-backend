from fastapi import Request, status
from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import logging

from ..core.config import ALGORITHM, SECRET_KEY, CORS_ORIGINS

logger = logging.getLogger(__name__)


def _add_cors_headers(response: JSONResponse, request: Request) -> JSONResponse:
    """Add CORS headers to error responses."""
    origin = request.headers.get("origin", "")
    if origin in CORS_ORIGINS or "*" in CORS_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
    return response


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip authentication for preflight OPTIONS requests
        if request.method == "OPTIONS":
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        request.state.user_id = None
        request.state.token_error = None

        if auth_header.startswith("Bearer "):
            token = auth_header.split(" ", 1)[1]
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                if payload.get("type") not in (None, "access"):
                    # Invalid token type - log but don't block
                    # Route-level auth will handle this
                    request.state.token_error = "Invalid token type"
                    logger.debug("Invalid token type received")
                else:
                    request.state.user_id = payload.get("sub")
            except JWTError as exc:
                # Invalid/expired token - log but don't block
                # Route-level auth will handle this via get_current_user dependency
                request.state.token_error = str(exc)
                logger.debug(f"JWT decode error: {exc}")

        return await call_next(request)
