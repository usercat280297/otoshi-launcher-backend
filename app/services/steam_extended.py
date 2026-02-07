"""
Steam Extended API - DLC, Achievements, News, and Updates
"""

from __future__ import annotations

import logging
from typing import Optional, List
from hashlib import sha1
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from datetime import timezone
import re
from xml.etree import ElementTree as ET
from pydantic import BaseModel
from bs4 import BeautifulSoup
import requests

from ..core.cache import cache_client
from ..core.config import (
    STEAM_CACHE_TTL_SECONDS,
    STEAM_REQUEST_TIMEOUT_SECONDS,
    STEAM_STORE_API_URL,
    STEAM_WEB_API_KEY,
    STEAM_WEB_API_URL,
    STEAM_NEWS_MAX_COUNT,
)

logger = logging.getLogger(__name__)

TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = TAG_RE.sub("", text)
    return text.strip() or None


def _resolve_dlc_image(dlc_info: dict, dlc_id: str) -> Optional[str]:
    if not isinstance(dlc_info, dict):
        return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{dlc_id}/header.jpg"
    for key in (
        "header_image",
        "capsule_imagev5",
        "capsule_image",
        "small_capsule_image",
        "library_capsule",
        "library_600x900",
        "library_header",
    ):
        value = dlc_info.get(key)
        if value:
            return value
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{dlc_id}/header.jpg"


def _fetch_store_meta(app_id: str) -> dict:
    cache_key = f"steam:dlc:meta:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached

    url = f"https://store.steampowered.com/app/{app_id}/?l=en"
    try:
        response = requests.get(
            url,
            timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "otoshi-launcher/1.0"},
        )
    except requests.RequestException:
        cache_client.set_json(cache_key, {}, ttl=STEAM_CACHE_TTL_SECONDS)
        return {}

    if response.status_code != 200:
        cache_client.set_json(cache_key, {}, ttl=STEAM_CACHE_TTL_SECONDS)
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    def _meta(prop: str) -> Optional[str]:
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return tag.get("content")
        tag = soup.find("meta", attrs={"name": prop})
        if tag and tag.get("content"):
            return tag.get("content")
        return None

    meta = {
        "image": _meta("og:image") or _meta("twitter:image"),
        "title": _meta("og:title"),
        "description": _meta("og:description"),
    }
    cache_client.set_json(cache_key, meta, ttl=STEAM_CACHE_TTL_SECONDS)
    return meta


class SteamDLC(BaseModel):
    app_id: str
    name: str
    header_image: Optional[str] = None
    description: Optional[str] = None
    release_date: Optional[str] = None
    price: Optional[dict] = None


class SteamAchievement(BaseModel):
    name: str
    display_name: str
    description: Optional[str] = None
    icon: Optional[str] = None
    icon_gray: Optional[str] = None
    hidden: bool = False
    global_percent: Optional[float] = None


class SteamNewsItem(BaseModel):
    gid: str
    title: str
    url: str
    author: Optional[str] = None
    contents: Optional[str] = None
    image: Optional[str] = None
    images: List[str] = []
    feed_label: Optional[str] = None
    date: int
    feed_name: Optional[str] = None
    tags: List[str] = []


class SteamUpdate(BaseModel):
    version: str
    date: Optional[str] = None
    changes: List[str] = []


def _request(url: str, params: dict) -> Optional[dict]:
    """Make a request to Steam API"""
    try:
        response = requests.get(
            url,
            params=params,
            timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "otoshi-launcher/1.0"},
        )
        if response.status_code != 200:
            return None
        return response.json()
    except Exception:
        return None


def get_steam_dlc(app_id: str) -> List[dict]:
    """
    Fetch DLC list for a Steam game
    Uses Steam Store API appdetails endpoint
    """
    cache_key = f"steam:dlc:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        def _has_required_fields(item: dict) -> bool:
            if not isinstance(item, dict):
                return False
            return bool(item.get("header_image")) and bool(item.get("description") or item.get("short_description"))
        if all(_has_required_fields(item) for item in cached):
            return cached

    url = f"{STEAM_STORE_API_URL.rstrip('/')}/appdetails"
    params = {
        "appids": app_id,
        "cc": "us",
        "l": "en",
    }

    payload = _request(url, params)
    if not payload:
        return []

    app_data = payload.get(app_id, {})
    if not app_data.get("success"):
        return []

    data = app_data.get("data", {})
    dlc_ids = data.get("dlc", [])

    if not dlc_ids:
        cache_client.set_json(cache_key, [], ttl=STEAM_CACHE_TTL_SECONDS)
        return []

    # Fetch details for DLC items (batch up to 20 at a time)
    dlc_list = []
    batch_size = 20
    enrich_budget = 12

    for i in range(0, len(dlc_ids), batch_size):
        batch_ids = dlc_ids[i:i + batch_size]
        batch_str = ",".join(str(dlc_id) for dlc_id in batch_ids)

        dlc_payload = _request(url, {
            "appids": batch_str,
            "cc": "us",
            "l": "en",
            "filters": "basic,price_overview,short_description,release_date",
        })

        if dlc_payload is None:
            dlc_payload = {}

        for dlc_id in batch_ids:
            dlc_data = dlc_payload.get(str(dlc_id), {})
            dlc_info = dlc_data.get("data", {}) if dlc_data.get("success") else {}

            price_overview = dlc_info.get("price_overview")
            price = None
            if price_overview:
                price = {
                    "initial": price_overview.get("initial"),
                    "final": price_overview.get("final"),
                    "discount_percent": price_overview.get("discount_percent", 0),
                    "currency": price_overview.get("currency"),
                    "formatted": price_overview.get("initial_formatted"),
                    "final_formatted": price_overview.get("final_formatted"),
                }
            elif dlc_info.get("is_free"):
                price = {
                    "initial": 0,
                    "final": 0,
                    "discount_percent": 0,
                    "formatted": "Free",
                    "final_formatted": "Free",
                }

            description = _strip_html(dlc_info.get("short_description")) or "Additional content for this game."
            release_date = None
            if isinstance(dlc_info.get("release_date"), dict):
                release_date = dlc_info.get("release_date", {}).get("date")

            fallback_header = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{dlc_id}/header.jpg"
            header_image = _resolve_dlc_image(dlc_info, str(dlc_id))
            name = dlc_info.get("name", f"DLC {dlc_id}")

            needs_meta = (
                not header_image
                or header_image == fallback_header
                or name.startswith("DLC ")
                or description == "Additional content for this game."
            )
            if needs_meta and enrich_budget > 0:
                meta = _fetch_store_meta(str(dlc_id))
                if meta.get("image"):
                    header_image = meta["image"]
                if meta.get("title") and (name.startswith("DLC ") or name == f"DLC {dlc_id}"):
                    cleaned = meta["title"].replace(" on Steam", "").strip()
                    if cleaned:
                        name = cleaned
                if meta.get("description") and description == "Additional content for this game.":
                    description = meta["description"]
                enrich_budget -= 1

            dlc_list.append({
                "app_id": str(dlc_id),
                "name": name,
                "header_image": header_image,
                "description": description,
                "release_date": release_date,
                "price": price,
            })

    cache_client.set_json(cache_key, dlc_list, ttl=STEAM_CACHE_TTL_SECONDS)
    return dlc_list


def get_steam_achievements(app_id: str) -> List[dict]:
    """
    Fetch achievements for a Steam game
    Uses Steam Web API
    """
    cache_key = f"steam:achievements:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached

    achievements = []

    # Get achievement schema
    if not STEAM_WEB_API_KEY:
        logger.warning("STEAM_WEB_API_KEY not configured")
        cache_client.set_json(cache_key, [], ttl=STEAM_CACHE_TTL_SECONDS)
        return []
    
    try:
        schema_url = f"{STEAM_WEB_API_URL.rstrip('/')}/ISteamUserStats/GetSchemaForGame/v2/"
        schema_payload = _request(schema_url, {
            "key": STEAM_WEB_API_KEY,
            "appid": app_id,
            "l": "en",
        })

        if not schema_payload:
            logger.warning(f"No schema payload for app {app_id}")
            cache_client.set_json(cache_key, [], ttl=STEAM_CACHE_TTL_SECONDS)
            return []

        game = schema_payload.get("game", {})
        available_stats = game.get("availableGameStats", {})
        achievement_list = available_stats.get("achievements", [])

        if not achievement_list:
            logger.info(f"No achievements found for app {app_id}")
            cache_client.set_json(cache_key, [], ttl=STEAM_CACHE_TTL_SECONDS)
            return []

        # Get global achievement percentages
        global_url = f"{STEAM_WEB_API_URL.rstrip('/')}/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/"
        global_payload = _request(global_url, {"gameid": app_id})

        global_percents = {}
        if global_payload:
            for ach in global_payload.get("achievementpercentages", {}).get("achievements", []):
                global_percents[ach.get("name", "")] = ach.get("percent", 0)

        for ach in achievement_list:
            name = ach.get("name", "")
            achievements.append({
                "name": name,
                "display_name": ach.get("displayName", name),
                "description": ach.get("description"),
                "icon": ach.get("icon"),
                "icon_gray": ach.get("icongray"),
                "hidden": bool(ach.get("hidden", 0)),
                "global_percent": global_percents.get(name),
            })

        logger.info(f"Loaded {len(achievements)} achievements for app {app_id}")
    except Exception as e:
        logger.error(f"Error fetching achievements for app {app_id}: {e}")
        achievements = []

    cache_client.set_json(cache_key, achievements, ttl=STEAM_CACHE_TTL_SECONDS)
    return achievements


def get_steam_news(app_id: str, count: int = 10) -> List[dict]:
    """
    Fetch news/updates for a Steam game
    Uses Steam Web API GetNewsForApp
    """
    resolved_count = STEAM_NEWS_MAX_COUNT if count <= 0 else min(count, STEAM_NEWS_MAX_COUNT)
    cache_key = f"steam:news:v7:{app_id}:{resolved_count}"
    cached = cache_client.get_json(cache_key)
    if cached is not None and _news_items_have_images(cached):
        return cached

    url = f"{STEAM_WEB_API_URL.rstrip('/')}/ISteamNews/GetNewsForApp/v2/"
    params = {
        "appid": app_id,
        "count": resolved_count,
        # 0 = no truncation; required to keep image tags in contents
        "maxlength": 0,
        "format": "json",
    }

    payload = _request(url, params)
    if not payload:
        return []

    app_news = payload.get("appnews", {})
    news_items = app_news.get("newsitems", [])

    parsed_items: list[tuple[dict, list[str]]] = []
    for item in news_items:
        item_data = dict(item)
        contents = item_data.get("contents") or ""
        images = _extract_news_images(contents)
        if not images:
            og_image = _get_news_image_from_url(item_data.get("url"))
            if not og_image:
                alt_url = _extract_first_link(contents)
                if alt_url:
                    og_image = _get_news_image_from_url(alt_url)
            if og_image:
                images = [og_image]
        parsed_items.append((item_data, images))

    needs_rss = any(_is_community_announcement(item_data) for item_data, _ in parsed_items)
    if needs_rss:
        rss_entries = _get_rss_news_entries(app_id)
        if rss_entries:
            updated: list[tuple[dict, list[str]]] = []
            for item_data, images in parsed_items:
                is_community = _is_community_announcement(item_data)
                entry = _match_rss_entry(item_data, rss_entries) if is_community else None
                if entry:
                    rss_image = entry.get("image")
                    if rss_image:
                        images = _prefer_rss_image(images, rss_image)
                    # Prefer RSS description if it's longer (usually full patch notes)
                    rss_desc = entry.get("description") or ""
                    current_desc = item_data.get("contents") or ""
                    if rss_desc and len(rss_desc) > len(current_desc):
                        item_data["contents"] = rss_desc
                    if entry.get("url"):
                        item_data["url"] = entry.get("url")
                updated.append((item_data, images))
            parsed_items = updated

    needs_fallback = any(len(images) == 0 for _, images in parsed_items)
    fallback_image = _get_news_fallback_image(app_id) if needs_fallback else None

    result = []
    for item_data, images in parsed_items:
        image = images[0] if images else fallback_image
        if image and not images:
            images = [image]
        
        # Clean content for display
        raw_contents = item_data.get("contents") or ""
        cleaned_content = _clean_html_content(raw_contents)
        
        result.append({
            "gid": str(item_data.get("gid", "")),
            "title": item_data.get("title", ""),
            "url": item_data.get("url", ""),
            "author": item_data.get("author"),
            "contents": raw_contents,
            "feed_label": item_data.get("feedlabel"),
            "date": item_data.get("date", 0),
            "feed_name": item_data.get("feedname"),
            "tags": item_data.get("tags", []),
            "image": image,
            "images": images,
            "patch_notes": None,
            "structured_content": {
                "cleaned": cleaned_content,
                "raw": raw_contents,
            },
        })

    _merge_enhanced_news_details(app_id, count, result)

    cache_client.set_json(cache_key, result, ttl=STEAM_CACHE_TTL_SECONDS // 2)  # Shorter cache for news
    return result


_CLAN_IMAGE_BASE = "https://clan.akamai.steamstatic.com/images"
_IMG_TAG_RE = re.compile(r"\[img\](.*?)\[/img\]", re.IGNORECASE | re.DOTALL)
_HTML_IMG_RE = re.compile(r"<img[^>]+src=[\"']([^\"']+)[\"']", re.IGNORECASE)
_URL_IMG_RE = re.compile(r"https?://[^\s\"']+\.(?:jpg|jpeg|png|gif|webp)", re.IGNORECASE)
_STEAM_ASSET_RE = re.compile(r"https?://[^\s\"']*steamstatic.com[^\s\"']+\.(?:jpg|jpeg|png|gif|webp)", re.IGNORECASE)
_OG_IMAGE_RE = re.compile(r"<meta[^>]+property=[\"']og:image[\"'][^>]*content=[\"']([^\"']+)[\"']", re.IGNORECASE)
_TWITTER_IMAGE_RE = re.compile(r"<meta[^>]+name=[\"']twitter:image[\"'][^>]*content=[\"']([^\"']+)[\"']", re.IGNORECASE)
_LINK_IMAGE_RE = re.compile(r"<link[^>]+rel=[\"']image_src[\"'][^>]*href=[\"']([^\"']+)[\"']", re.IGNORECASE)
_BB_URL_RE = re.compile(r"\[url=([^\]]+)\]", re.IGNORECASE)
_PLAIN_URL_RE = re.compile(r"https?://[^\s\]]+", re.IGNORECASE)
_TITLE_NORMALIZE_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)


def _normalize_title(title: str) -> str:
    if not title:
        return ""
    clean = title.lower()
    clean = clean.replace("\u2014", "-")
    clean = clean.replace("\u2013", "-")
    clean = clean.replace("\u201c", "")
    clean = clean.replace("\u201d", "")
    clean = clean.replace("\u2019", "")
    clean = clean.replace("\"", "")
    clean = clean.replace("'", "")
    clean = _TITLE_NORMALIZE_RE.sub(" ", clean)
    return re.sub(r"\s+", " ", clean).strip()


def _clean_html_content(content: str) -> str:
    """Clean HTML/BB code from news content for display"""
    if not content:
        return ""
    
    text = content
    
    # Remove img tags completely
    text = re.sub(r"\[img\].*?\[/img\]", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<img[^>]*>", "", text, flags=re.IGNORECASE)
    
    # Convert BB code URL to plain text
    text = re.sub(r"\[url=([^\]]*)\]([^\[]*)\[/url\]", r"\2 (\1)", text, flags=re.IGNORECASE)
    
    # Remove BB code headers
    text = re.sub(r"\[h[1-6]\]([^\[]*)\[/h[1-6]\]", r"\1", text, flags=re.IGNORECASE)
    
    # Remove other BB code
    text = re.sub(r"\[b\]([^\[]*)\[/b\]", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\[i\]([^\[]*)\[/i\]", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\[u\]([^\[]*)\[/u\]", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\[s\]([^\[]*)\[/s\]", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\[list\](.*?)\[/list\]", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
    text = text.replace("[*]", "• ")
    
    # Remove STEAM_CLAN_IMAGE references
    text = re.sub(r"\{STEAM_CLAN_IMAGE\}[^\s]*", "", text)
    
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    
    # Decode HTML entities
    text = text.replace("&nbsp;", " ")
    text = text.replace("&quot;", '"')
    text = text.replace("&apos;", "'")
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    
    # Normalize whitespace
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    text = text.strip()
    
    # Limit to reasonable length
    if len(text) > 500:
        text = text[:497] + "..."
    
    return text


def _news_items_have_images(items: list[dict]) -> bool:
    for item in items:
        if not isinstance(item, dict):
            return False
        if item.get("image"):
            continue
        images = item.get("images") or []
        if not isinstance(images, list) or len(images) == 0:
            return False
        if "structured_content" not in item:
            return False
    return True


def _is_community_announcement(item: dict) -> bool:
    feed_name = (item.get("feedname") or "").lower()
    feed_label = (item.get("feedlabel") or "").lower()
    if feed_name == "steam_community_announcements":
        return True
    return "community" in feed_label


def _normalize_image_url(url: str) -> Optional[str]:
    if not url:
        return None
    clean = url.strip()
    if not clean:
        return None
    if clean.startswith("{STEAM_CLAN_IMAGE}"):
        clean = clean.replace("{STEAM_CLAN_IMAGE}", _CLAN_IMAGE_BASE)
    if clean.startswith("//"):
        clean = f"https:{clean}"
    return clean


def _prefer_rss_image(images: List[str], rss_image: str) -> List[str]:
    normalized = _normalize_image_url(rss_image)
    if not normalized:
        return images
    merged: list[str] = [normalized]
    for img in images:
        if img and img not in merged:
            merged.append(img)
    return merged


def _extract_news_images(contents: str) -> List[str]:
    if not contents:
        return []
    found: list[str] = []
    for match in _IMG_TAG_RE.findall(contents):
        normalized = _normalize_image_url(match)
        if normalized:
            found.append(normalized)
    for match in _HTML_IMG_RE.findall(contents):
        normalized = _normalize_image_url(match)
        if normalized:
            found.append(normalized)
    for match in _URL_IMG_RE.findall(contents):
        normalized = _normalize_image_url(match)
        if normalized:
            found.append(normalized)
    seen = set()
    unique: list[str] = []
    for url in found:
        if url not in seen:
            seen.add(url)
            unique.append(url)
    return unique


def _extract_og_image(html: str) -> Optional[str]:
    if not html:
        return None
    for pattern in (_OG_IMAGE_RE, _TWITTER_IMAGE_RE, _LINK_IMAGE_RE):
        match = pattern.search(html)
        if match:
            return _normalize_image_url(match.group(1))
    return None


def _extract_first_link(contents: str) -> Optional[str]:
    if not contents:
        return None
    match = _BB_URL_RE.search(contents)
    if match:
        return match.group(1).strip()
    match = _PLAIN_URL_RE.search(contents)
    if match:
        return match.group(0).strip()
    return None


def _get_news_image_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    cache_key = f"steam:news:og:{sha1(url.encode('utf-8')).hexdigest()[:16]}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached or None
    try:
        response = requests.get(
            url,
            timeout=6,
            headers={"User-Agent": "otoshi-launcher/1.0"},
            allow_redirects=True,
        )
        if response.status_code != 200:
            cache_client.set_json(cache_key, "", ttl=600)
            return None
        html = response.text or ""
        image = _extract_og_image(html)
        if not image:
            # Prefer steamstatic assets if present
            matches = _STEAM_ASSET_RE.findall(html)
            if matches:
                image = _normalize_image_url(matches[0])
        if not image:
            # Fallback: first image URL inside HTML
            matches = _URL_IMG_RE.findall(html)
            if matches:
                image = _normalize_image_url(matches[0])
        cache_client.set_json(cache_key, image or "", ttl=STEAM_CACHE_TTL_SECONDS // 2)
        return image
    except Exception:
        cache_client.set_json(cache_key, "", ttl=600)
        return None


def _get_news_fallback_image(app_id: str) -> Optional[str]:
    cache_key = f"steam:news:img:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached
    url = f"{STEAM_STORE_API_URL.rstrip('/')}/appdetails"
    params = {
        "appids": app_id,
        "cc": "us",
        "l": "en",
        "filters": "basic",
    }
    payload = _request(url, params)
    app_data = payload.get(str(app_id), {}) if payload else {}
    if not app_data.get("success"):
        return None
    data = app_data.get("data") or {}
    image = data.get("header_image") or data.get("capsule_image") or data.get("background")
    if image:
        cache_client.set_json(cache_key, image, ttl=STEAM_CACHE_TTL_SECONDS)
    return image


def _merge_enhanced_news_details(app_id: str, count: int, result: list[dict]) -> None:
    try:
        from .steam_news_enhanced import fetch_news_enhanced, get_news_fetcher
        fetcher = get_news_fetcher()
    except Exception:
        fetcher = None
        return

    try:
        enhanced_items = fetch_news_enhanced(app_id, count)
    except Exception:
        enhanced_items = []

    if not enhanced_items:
        return

    enhanced_by_gid = {item.get("gid"): item for item in enhanced_items if item.get("gid")}
    enhanced_by_title = {
        _normalize_title(item.get("title", "")): item
        for item in enhanced_items
        if item.get("title")
    }

    for item in result:
        enhanced = enhanced_by_gid.get(item.get("gid"))
        if not enhanced:
            enhanced = enhanced_by_title.get(_normalize_title(item.get("title", "")))
        if not enhanced:
            continue

        # Prefer enhanced content when it is longer.
        enhanced_contents = enhanced.get("contents") or ""
        if enhanced_contents and len(enhanced_contents) > len(item.get("contents") or ""):
            item["contents"] = enhanced_contents

        # Merge images if missing
        if not item.get("image") and enhanced.get("image"):
            item["image"] = enhanced.get("image")
        if not item.get("images") and enhanced.get("images"):
            item["images"] = enhanced.get("images")

        # Refresh structured content based on the final contents (RSS-enhanced)
        if fetcher:
            final_contents = item.get("contents") or ""
            feed_label = item.get("feed_label") or ""
            if final_contents:
                structured = fetcher._structure_content(final_contents, feed_label)
                existing_cleaned = (item.get("structured_content") or {}).get("cleaned", "")
                if not existing_cleaned or len(structured.get("cleaned", "")) > len(existing_cleaned):
                    item["structured_content"] = structured

                if item.get("patch_notes") is None and fetcher._is_patch_note(
                    item.get("title", ""), final_contents, feed_label
                ):
                    notes = fetcher._parse_patch_notes(final_contents)
                    item["patch_notes"] = [
                        {
                            "title": note.title,
                            "content": note.content,
                            "category": note.category,
                        }
                        for note in notes
                    ]

        if item.get("structured_content") is None and enhanced.get("structured_content") is not None:
            item["structured_content"] = enhanced.get("structured_content")
        if item.get("patch_notes") is None and enhanced.get("patch_notes") is not None:
            item["patch_notes"] = enhanced.get("patch_notes")


def _get_rss_news_entries(app_id: str) -> list[dict]:
    cache_key = f"steam:news:rss:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached
    url = f"https://store.steampowered.com/feeds/news/app/{app_id}?l=english"
    try:
        response = requests.get(
            url,
            timeout=STEAM_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "otoshi-launcher/1.0"},
        )
        if response.status_code != 200:
            cache_client.set_json(cache_key, [], ttl=600)
            return []
        root = ET.fromstring(response.text)
        channel = root.find("channel")
        if channel is None:
            cache_client.set_json(cache_key, [], ttl=600)
            return []
        entries: list[dict] = []
        for item in channel.findall("item"):
            title = item.findtext("title") or ""
            enclosure = item.find("enclosure")
            image = enclosure.attrib.get("url") if enclosure is not None else None
            description = item.findtext("description") or ""
            url = item.findtext("link") or item.findtext("guid") or ""
            pub_date = item.findtext("pubDate") or ""
            ts = 0
            if pub_date:
                try:
                    dt = parsedate_to_datetime(pub_date)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    ts = int(dt.timestamp())
                except Exception:
                    ts = 0
            if image or description or url:
                entries.append({
                    "title": title,
                    "image": image,
                    "date": ts,
                    "description": description,
                    "url": url,
                })
        cache_client.set_json(cache_key, entries, ttl=STEAM_CACHE_TTL_SECONDS // 2)
        return entries
    except Exception:
        cache_client.set_json(cache_key, [], ttl=600)
        return []


def _match_rss_entry(item: dict, entries: list[dict]) -> Optional[dict]:
    title = item.get("title") or ""
    if not title or not entries:
        return None
    norm_title = _normalize_title(title)
    # Exact normalized match first
    for entry in entries:
        if _normalize_title(entry.get("title", "")) == norm_title:
            return entry

    # Fuzzy match on title similarity
    best_entry = None
    best_score = 0.0
    for entry in entries:
        score = SequenceMatcher(None, norm_title, _normalize_title(entry.get("title", ""))).ratio()
        if score > best_score:
            best_score = score
            best_entry = entry
    if best_entry and best_score >= 0.78:
        return best_entry

    # Fall back to closest date match for community announcements (title may differ slightly)
    item_date = item.get("date") or 0
    if item_date:
        closest_entry = None
        closest_delta = None
        for entry in entries:
            entry_date = entry.get("date") or 0
            if not entry_date:
                continue
            delta = abs(entry_date - item_date)
            if closest_delta is None or delta < closest_delta:
                closest_delta = delta
                closest_entry = entry
        if closest_entry and closest_delta is not None and closest_delta <= 14 * 86400:
            return closest_entry

    return None


def get_steam_player_count(app_id: str) -> Optional[int]:
    """
    Get current player count for a Steam game
    """
    if not STEAM_WEB_API_KEY:
        return None

    cache_key = f"steam:players:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached

    url = f"{STEAM_WEB_API_URL.rstrip('/')}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    payload = _request(url, {"appid": app_id})

    if not payload:
        return None

    response = payload.get("response", {})
    player_count = response.get("player_count")

    if player_count is not None:
        cache_client.set_json(cache_key, player_count, ttl=300)  # 5 min cache

    return player_count


def get_steam_reviews_summary(app_id: str) -> Optional[dict]:
    """
    Get review summary for a Steam game
    """
    cache_key = f"steam:reviews:{app_id}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached

    url = f"https://store.steampowered.com/appreviews/{app_id}"
    params = {
        "json": 1,
        "language": "all",
        "purchase_type": "all",
        "num_per_page": 0,
    }

    payload = _request(url, params)
    if not payload:
        return None

    query_summary = payload.get("query_summary", {})
    result = {
        "total_positive": query_summary.get("total_positive", 0),
        "total_negative": query_summary.get("total_negative", 0),
        "total_reviews": query_summary.get("total_reviews", 0),
        "review_score": query_summary.get("review_score", 0),
        "review_score_desc": query_summary.get("review_score_desc", ""),
    }

    cache_client.set_json(cache_key, result, ttl=STEAM_CACHE_TTL_SECONDS)
    return result
