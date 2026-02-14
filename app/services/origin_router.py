from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote
import os
import time
import hmac
import hashlib


def _split_env(name: str, default: str) -> list[str]:
    value = os.getenv(name, default)
    items = [item.strip().rstrip("/") for item in value.split(",") if item.strip()]
    return items or [default.rstrip("/")]


HF_ORIGINS = _split_env("HF_V2_ORIGINS", "https://huggingface.co")
CDN_ORIGINS = _split_env("CDN_V2_ORIGINS", "http://127.0.0.1:8000")
SIGNING_SECRET = os.getenv("CDN_V2_SIGNING_SECRET", "").encode("utf-8")


@dataclass(frozen=True)
class OriginRoute:
    origin: str
    url: str
    fallbacks: list[str]

    def to_dict(self) -> dict:
        return {"origin": self.origin, "url": self.url, "fallbacks": self.fallbacks}


def _normalize_path(path: str) -> str:
    cleaned = str(path or "").replace("\\", "/").lstrip("/")
    return "/".join(quote(part, safe="-_.~") for part in cleaned.split("/") if part)


def _sign(url: str, expires_at: int) -> str:
    if not SIGNING_SECRET:
        return url
    sep = "&" if "?" in url else "?"
    payload = f"{url}|{expires_at}".encode("utf-8")
    digest = hmac.new(SIGNING_SECRET, payload, hashlib.sha256).hexdigest()
    return f"{url}{sep}expires={expires_at}&sig={digest}"


def resolve_origin_url(
    relative_path: str,
    *,
    channel: str = "stable",
    signed: bool = False,
    ttl_seconds: int = 600,
) -> OriginRoute:
    normalized = _normalize_path(relative_path)
    channel_name = (channel or "stable").strip().lower()

    if channel_name in {"stable", "production", "release"}:
        primary_pool = CDN_ORIGINS
        fallback_pool = HF_ORIGINS
        primary_name = "cdn"
    else:
        primary_pool = HF_ORIGINS
        fallback_pool = CDN_ORIGINS
        primary_name = "huggingface"

    primary = f"{primary_pool[0]}/{normalized}" if normalized else primary_pool[0]
    fallbacks = [
        f"{origin}/{normalized}" if normalized else origin
        for origin in fallback_pool
    ]

    if signed:
        expires_at = int(time.time()) + max(60, int(ttl_seconds))
        primary = _sign(primary, expires_at)
        fallbacks = [_sign(value, expires_at) for value in fallbacks]

    return OriginRoute(origin=primary_name, url=primary, fallbacks=fallbacks)

