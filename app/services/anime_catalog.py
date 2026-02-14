from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..core.cache import cache_client
from ..core.config import (
    ANIME_CACHE_TTL_SECONDS,
    ANIME_REQUEST_TIMEOUT_SECONDS,
    ANIME_SOURCE_URL,
)

DEFAULT_SECTION_FALLBACK = "Anime Picks"
DEFAULT_SERVER_GROUP = "AnimeVsub"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
ALLOWED_HOST_SUFFIXES = (
    "animevietsub.vip",
    "animevietsub.ee",
    "animevietsub.show",
)
DETAIL_BREADCRUMB_SELECTORS = (
    ".breadcrumb a[href]",
    ".BrdCrm a[href]",
    ".BreadCrumb a[href]",
    ".Bc a[href]",
)


def _safe_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _slugify(value: str, fallback: str = "item") -> str:
    lowered = value.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return slug or fallback


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    if not parsed.scheme:
        return urljoin(ANIME_SOURCE_URL, url.strip())
    return url.strip()


def _safe_join(base_url: str, href: str) -> str:
    if not href:
        return ""
    return _normalize_url(urljoin(base_url, href))


def _same_host_or_subdomain(base: str, candidate: str) -> bool:
    base_host = (urlparse(base).hostname or "").lower()
    cand_host = (urlparse(candidate).hostname or "").lower()
    if not base_host or not cand_host:
        return False
    if cand_host == base_host or cand_host.endswith(f".{base_host}"):
        return True
    return any(
        cand_host == suffix or cand_host.endswith(f".{suffix}")
        for suffix in ALLOWED_HOST_SUFFIXES
    )


def _request_html(url: str) -> str:
    last_error: Optional[Exception] = None
    for _ in range(3):
        try:
            response = requests.get(
                url,
                timeout=ANIME_REQUEST_TIMEOUT_SECONDS,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
                },
            )
            response.raise_for_status()
            return response.text
        except Exception as exc:  # pragma: no cover - network branch
            last_error = exc
    if last_error:
        raise last_error
    raise RuntimeError("Failed to request anime source")


def _home_source_candidates() -> List[str]:
    candidates: List[str] = []
    configured = _normalize_url(ANIME_SOURCE_URL)
    if configured:
        candidates.append(configured)
    for suffix in ALLOWED_HOST_SUFFIXES:
        url = f"https://{suffix}/"
        if url not in candidates:
            candidates.append(url)
    return candidates


def _extract_background_from_style(style: str) -> str:
    if not style:
        return ""
    match = re.search(r"url\((['\"]?)([^)'\"]+)\1\)", style, re.IGNORECASE)
    return _safe_text(match.group(2) if match else "")


def _looks_like_default_image(url: str) -> bool:
    lowered = (url or "").lower()
    return (
        "cast-image" in lowered
        or "/statics/default/images/" in lowered
        or lowered.endswith("default.jpg")
    )


def _extract_card(card: Any, source_url: str) -> Optional[Dict[str, Any]]:
    anchor = card.select_one("a[href]")
    title_node = card.select_one(".Title")
    image_node = card.select_one("img")
    episode_node = card.select_one(".mli-eps")
    rating_node = card.select_one(".anime-avg-user-rating")

    detail_url = _safe_join(source_url, anchor["href"]) if anchor else ""
    if not detail_url:
        return None
    if not _same_host_or_subdomain(source_url, detail_url):
        return None

    title = _safe_text(title_node.get_text(" ", strip=True) if title_node else "")
    if not title and image_node:
        title = _safe_text(image_node.get("alt"))
    if not title:
        title = _safe_text(anchor.get("title")) if anchor else ""
    if not title:
        return None

    image = ""
    if image_node:
        image = _safe_join(
            source_url,
            image_node.get("src")
            or image_node.get("data-src")
            or image_node.get("data-lazy-src")
            or "",
        )

    rating = _safe_text(rating_node.get_text(" ", strip=True) if rating_node else "")
    episode = _safe_text(episode_node.get_text(" ", strip=True) if episode_node else "")
    slug = urlparse(detail_url).path.strip("/").replace("/", "-")
    if not slug:
        slug = _slugify(title, fallback="anime")

    return {
        "id": slug,
        "title": title,
        "detail_url": detail_url,
        "poster_image": image or None,
        "background_image": image or None,
        "episode_label": episode or None,
        "rating_label": rating or None,
    }


def _extract_menu_tags(soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
    root = soup.select_one("header .Menu > ul") or soup.select_one(".Menu > ul")
    if not root:
        return []

    menu_groups: List[Dict[str, Any]] = []
    for li in root.select(":scope > li"):
        anchor = li.select_one(":scope > a[href]")
        if not anchor:
            continue
        title = _safe_text(anchor.get_text(" ", strip=True))
        if not title:
            continue

        href_raw = _safe_text(anchor.get("href"))
        href = _safe_join(source_url, href_raw) if href_raw and href_raw != "#" else None

        children: List[Dict[str, str]] = []
        for sub_anchor in li.select(":scope > ul li > a[href]"):
            label = _safe_text(sub_anchor.get_text(" ", strip=True))
            sub_href = _safe_join(source_url, _safe_text(sub_anchor.get("href")))
            if not label or not sub_href:
                continue
            if not _same_host_or_subdomain(source_url, sub_href):
                continue
            children.append(
                {
                    "id": _slugify(label, fallback="tag"),
                    "label": label,
                    "href": sub_href,
                }
            )

        menu_groups.append(
            {
                "id": _slugify(title, fallback=f"menu-{len(menu_groups) + 1}"),
                "title": title,
                "href": href,
                "items": children[:40],
            }
        )

    return menu_groups[:16]


def _extract_carousel_items(
    soup: BeautifulSoup,
    source_url: str,
    limit: int,
) -> List[Dict[str, Any]]:
    cards = soup.select(".MovieListTopCn .MovieListTop .TPostMv")
    if not cards:
        cards = soup.select(".MovieListTop .TPostMv")
    if not cards:
        return []

    bg_images: List[str] = []
    for node in soup.select(".MovieListTopCn .TPostBg, .MovieListTop .TPostBg"):
        style_url = _extract_background_from_style(node.get("style") or "")
        if not style_url:
            continue
        resolved = _safe_join(source_url, style_url)
        if resolved:
            bg_images.append(resolved)

    carousel: List[Dict[str, Any]] = []
    seen = set()
    for index, card in enumerate(cards):
        item = _extract_card(card, source_url)
        if not item:
            continue
        key = item.get("detail_url")
        if key in seen:
            continue
        seen.add(key)
        if index < len(bg_images):
            item["background_image"] = bg_images[index]
        carousel.append(item)
        if len(carousel) >= limit:
            break
    return carousel


def get_home_sections(limit_per_section: int = 12, refresh: bool = False) -> Dict[str, Any]:
    safe_limit = max(1, min(limit_per_section, 24))
    cache_key = f"anime:home:{safe_limit}"
    if not refresh:
        cached = cache_client.get_json(cache_key)
        if cached:
            return cached

    source_url = _normalize_url(ANIME_SOURCE_URL)
    html = ""
    soup: Optional[BeautifulSoup] = None
    last_error: Optional[Exception] = None
    for candidate in _home_source_candidates():
        try:
            html = _request_html(candidate)
            source_url = candidate
            soup = BeautifulSoup(html, "html.parser")
            break
        except Exception as exc:  # pragma: no cover - network branch
            last_error = exc
    if not soup:
        if last_error:
            raise last_error
        raise RuntimeError("Failed to load anime home source")

    menu_tags = _extract_menu_tags(soup, source_url)
    carousel = _extract_carousel_items(soup, source_url, safe_limit)

    sections: List[Dict[str, Any]] = []
    for section in soup.select("section"):
        cards = section.select(".TPostMv")
        if not cards:
            continue
        heading = (
            section.select_one("h1")
            or section.select_one("h2")
            or section.select_one("h3")
        )
        title = _safe_text(heading.get_text(" ", strip=True) if heading else "")
        title = title or DEFAULT_SECTION_FALLBACK

        seen_urls = set()
        items: List[Dict[str, Any]] = []
        for card in cards:
            parsed = _extract_card(card, source_url)
            if not parsed:
                continue
            url = parsed["detail_url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            items.append(parsed)
            if len(items) >= safe_limit:
                break

        if items:
            sections.append(
                {
                    "id": _slugify(title, fallback=f"section-{len(sections) + 1}"),
                    "title": title,
                    "items": items,
                }
            )

    if not sections:
        cards = soup.select(".TPostMv")
        seen_urls = set()
        items: List[Dict[str, Any]] = []
        for card in cards:
            parsed = _extract_card(card, source_url)
            if not parsed:
                continue
            url = parsed["detail_url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            items.append(parsed)
            if len(items) >= safe_limit:
                break
        if items:
            sections.append(
                {
                    "id": "anime-picks",
                    "title": DEFAULT_SECTION_FALLBACK,
                    "items": items,
                }
            )

    if not carousel and sections:
        carousel = sections[0].get("items", [])[:safe_limit]

    payload = {
        "source": source_url,
        "menu_tags": menu_tags,
        "carousel": carousel,
        "sections": sections,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    cache_client.set_json(cache_key, payload, ttl=ANIME_CACHE_TTL_SECONDS)
    return payload


def search_home_catalog(query: str, limit: int = 24, refresh: bool = False) -> List[Dict[str, Any]]:
    normalized = _safe_text(query).lower()
    if not normalized:
        return []
    safe_limit = max(1, min(limit, 50))
    home = get_home_sections(limit_per_section=24, refresh=refresh)
    seen_urls = set()
    results: List[Dict[str, Any]] = []
    for section in home.get("sections", []):
        for item in section.get("items", []):
            haystack = " ".join(
                [
                    str(item.get("title") or ""),
                    str(item.get("episode_label") or ""),
                ]
            ).lower()
            if normalized not in haystack:
                continue
            detail_url = item.get("detail_url")
            if not detail_url or detail_url in seen_urls:
                continue
            seen_urls.add(detail_url)
            enriched = dict(item)
            enriched["section_title"] = section.get("title")
            results.append(enriched)
            if len(results) >= safe_limit:
                return results
    return results


def _extract_detail_title(soup: BeautifulSoup) -> str:
    selectors = [
        ".Title",
        "h1",
        "meta[property='og:title']",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            value = _safe_text(node.get("content"))
        else:
            value = _safe_text(node.get_text(" ", strip=True))
        if value:
            return value
    return ""


def _extract_description(soup: BeautifulSoup) -> Optional[str]:
    selectors = [
        ".Description",
        ".entry-content",
        ".MovieInfo .Description",
        "meta[name='description']",
        "meta[property='og:description']",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            value = _safe_text(node.get("content"))
        else:
            value = _safe_text(node.get_text(" ", strip=True))
        if value:
            return value
    return None


def _extract_banner_image(soup: BeautifulSoup, page_url: str) -> Optional[str]:
    selectors = [
        "meta[property='og:image']",
        "meta[name='twitter:image']",
        ".MovieInfo img",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            src = node.get("content")
        else:
            src = node.get("src") or node.get("data-src") or node.get("data-lazy-src")
        if not src:
            continue
        resolved = _safe_join(page_url, src)
        if resolved and not _looks_like_default_image(resolved):
            return resolved
    return None


def _extract_cover_image(soup: BeautifulSoup, page_url: str) -> Optional[str]:
    selectors = [
        ".MvTbCn .Image img",
        ".MovieInfo img",
        ".TPost img",
        "meta[property='og:image']",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            src = node.get("content")
        else:
            src = node.get("src") or node.get("data-src") or node.get("data-lazy-src")
        if not src:
            continue
        resolved = _safe_join(page_url, src)
        if resolved and not _looks_like_default_image(resolved):
            return resolved
    return None


def _extract_breadcrumbs(soup: BeautifulSoup, page_url: str) -> List[Dict[str, str]]:
    breadcrumbs: List[Dict[str, str]] = []
    seen = set()
    for selector in DETAIL_BREADCRUMB_SELECTORS:
        for anchor in soup.select(selector):
            href = _safe_join(page_url, anchor.get("href") or "")
            label = _safe_text(anchor.get_text(" ", strip=True))
            if not href or not label:
                continue
            if href in seen:
                continue
            seen.add(href)
            breadcrumbs.append(
                {
                    "id": _slugify(label, fallback="crumb"),
                    "label": label,
                    "href": href,
                }
            )
    return breadcrumbs[:20]


def _extract_metadata_entries(soup: BeautifulSoup) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    for item in soup.select(".InfoList li"):
        text = _safe_text(item.get_text(" ", strip=True))
        if not text:
            continue
        key_node = item.select_one("strong, b")
        key = ""
        value = text
        if key_node:
            raw_key = _safe_text(key_node.get_text(" ", strip=True))
            key = raw_key.rstrip(":")
            value = text.replace(raw_key, "", 1).lstrip(": ").strip()
        else:
            match = re.match(r"^([^:]{2,30}):\s*(.+)$", text)
            if match:
                key = _safe_text(match.group(1))
                value = _safe_text(match.group(2))
        if not key:
            key = f"meta-{len(entries) + 1}"
        entries.append({"key": key, "value": value})
    return entries[:40]


def _quality_from_metadata(metadata: List[Dict[str, str]], html: str) -> Optional[str]:
    for entry in metadata:
        key = _safe_text(entry.get("key", "")).lower()
        if "chất lượng" in key or "quality" in key:
            value = _safe_text(entry.get("value"))
            if value:
                return value

    html_lower = html.lower()
    for token in ("2160p", "1440p", "1080p", "720p", "fhd", "hd", "sd"):
        if token in html_lower:
            return token.upper()
    return None


def _episode_label_from_href(href: str) -> str:
    match = re.search(r"/tap-(\d+)-", href)
    if match:
        return match.group(1)
    return "Watch"


def _extract_episodes(soup: BeautifulSoup, page_url: str, limit: int) -> List[Dict[str, str]]:
    episodes: List[Dict[str, str]] = []
    seen_urls = set()
    selectors = (
        "a.episode-link[href]",
        ".list-episode a[href]",
        ".EpisodeList a[href]",
        ".eps a[href]",
        "a[href*='/tap-']",
        "a[href*='/xem-phim']",
    )
    for selector in selectors:
        for anchor in soup.select(selector):
            href = _safe_join(page_url, anchor.get("href") or "")
            if not href:
                continue
            if "/tap-" not in href and "/xem-phim" not in href:
                continue
            if href in seen_urls:
                continue
            if not _same_host_or_subdomain(page_url, href):
                continue
            seen_urls.add(href)
            label = _safe_text(anchor.get_text(" ", strip=True))
            if not label:
                label = _episode_label_from_href(href)
            episodes.append({"label": label, "url": href})
            if len(episodes) >= limit:
                return episodes
    return episodes


def get_anime_detail(url: str, episode_limit: int = 40) -> Dict[str, Any]:
    normalized_url = _normalize_url(url)
    source_url = _normalize_url(ANIME_SOURCE_URL)
    if not normalized_url:
        raise ValueError("Missing anime url")
    if not _same_host_or_subdomain(source_url, normalized_url):
        raise ValueError("Anime url host is not allowed")

    safe_episode_limit = max(1, min(episode_limit, 100))
    cache_key = f"anime:detail:{normalized_url}:{safe_episode_limit}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached

    html = _request_html(normalized_url)
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_detail_title(soup) or normalized_url
    description = _extract_description(soup)
    cover_image = _extract_cover_image(soup, normalized_url)
    banner_image = _extract_banner_image(soup, normalized_url)
    metadata = _extract_metadata_entries(soup)
    quality_label = _quality_from_metadata(metadata, html)
    breadcrumbs = _extract_breadcrumbs(soup, normalized_url)
    episodes = _extract_episodes(soup, normalized_url, safe_episode_limit)

    payload = {
        "url": normalized_url,
        "title": title,
        "description": description,
        "cover_image": cover_image,
        "banner_image": banner_image,
        "quality_label": quality_label,
        "metadata": metadata,
        "breadcrumbs": breadcrumbs,
        "episodes": episodes,
    }
    cache_client.set_json(cache_key, payload, ttl=ANIME_CACHE_TTL_SECONDS)
    return payload


def _extract_media_urls(html: str) -> List[str]:
    urls = re.findall(r"https?://[^\"'\s]+", html, re.IGNORECASE)
    candidates = []
    for url in urls:
        lower = url.lower()
        if any(ext in lower for ext in [".m3u8", ".mp4", ".webm", ".mkv"]):
            candidates.append(url)
            continue
        if any(token in lower for token in ["playlist", "stream", "manifest", "master.m3u8"]):
            candidates.append(url)
    deduped: List[str] = []
    seen = set()
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _extract_player_hints(html: str) -> Dict[str, Any]:
    ajax_match = re.search(r"AjaxURL\s*=\s*['\"]([^'\"]+)['\"]", html, re.IGNORECASE)
    main_match = re.search(r"MAIN_URL\s*=\s*['\"]([^'\"]+)['\"]", html, re.IGNORECASE)
    episode_match = re.search(r"filmInfo\.episodeID\s*=\s*parseInt\(['\"]?(\d+)", html, re.IGNORECASE)
    film_match = re.search(r"filmInfo\.filmID\s*=\s*parseInt\(['\"]?(\d+)", html, re.IGNORECASE)
    return {
        "ajax_url": ajax_match.group(1) if ajax_match else None,
        "main_url": main_match.group(1) if main_match else None,
        "episode_id": episode_match.group(1) if episode_match else None,
        "film_id": film_match.group(1) if film_match else None,
    }


def _extract_server_groups(soup: BeautifulSoup, page_url: str) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []
    for group_node in soup.select(".server.server-group"):
        name_node = group_node.select_one(".server-name")
        group_name = _safe_text(name_node.get_text(" ", strip=True) if name_node else "")
        if not group_name:
            group_name = DEFAULT_SERVER_GROUP

        episodes: List[Dict[str, Any]] = []
        seen = set()
        for anchor in group_node.select("a.episode-link[href]"):
            href = _safe_join(page_url, anchor.get("href") or "")
            if not href:
                continue
            if href in seen:
                continue
            seen.add(href)
            label = _safe_text(anchor.get_text(" ", strip=True)) or _episode_label_from_href(href)
            episodes.append(
                {
                    "label": label,
                    "url": href,
                    "source_key": _safe_text(anchor.get("data-source")) or None,
                    "play_mode": _safe_text(anchor.get("data-play")) or None,
                    "episode_id": _safe_text(anchor.get("data-id")) or None,
                    "episode_hash": _safe_text(anchor.get("data-hash")) or None,
                }
            )

        if episodes:
            groups.append({"name": group_name, "episodes": episodes})

    if groups:
        return groups

    fallback_items: List[Dict[str, Any]] = []
    seen_fallback = set()
    for anchor in soup.select("a.episode-link[href]"):
        href = _safe_join(page_url, anchor.get("href") or "")
        if not href or href in seen_fallback:
            continue
        seen_fallback.add(href)
        fallback_items.append(
            {
                "label": _safe_text(anchor.get_text(" ", strip=True)) or _episode_label_from_href(href),
                "url": href,
                "source_key": _safe_text(anchor.get("data-source")) or None,
                "play_mode": _safe_text(anchor.get("data-play")) or None,
                "episode_id": _safe_text(anchor.get("data-id")) or None,
                "episode_hash": _safe_text(anchor.get("data-hash")) or None,
            }
        )
    if fallback_items:
        return [{"name": DEFAULT_SERVER_GROUP, "episodes": fallback_items}]
    return []


def get_episode_sources(url: str) -> Dict[str, Any]:
    normalized_url = _normalize_url(url)
    source_url = _normalize_url(ANIME_SOURCE_URL)
    if not normalized_url:
        raise ValueError("Missing episode url")
    if not _same_host_or_subdomain(source_url, normalized_url):
        raise ValueError("Episode url host is not allowed")

    cache_key = f"anime:episode:{normalized_url}"
    cached = cache_client.get_json(cache_key)
    if cached:
        return cached

    html = _request_html(normalized_url)
    soup = BeautifulSoup(html, "html.parser")

    scripts = []
    for node in soup.select("script[src]"):
        src = _safe_join(normalized_url, node.get("src") or "")
        if src:
            scripts.append(src)

    title = _extract_detail_title(soup) or normalized_url
    media_urls = _extract_media_urls(html)
    hints = _extract_player_hints(html)
    server_groups = _extract_server_groups(soup, normalized_url)
    quality_label = _quality_from_metadata([], html) or _safe_text(
        re.search(r"\b(FHD|HD|SD|720p|1080p|2160p)\b", title, re.IGNORECASE).group(1)
        if re.search(r"\b(FHD|HD|SD|720p|1080p|2160p)\b", title, re.IGNORECASE)
        else ""
    )

    payload = {
        "url": normalized_url,
        "title": title,
        "quality_label": quality_label or None,
        "server_groups": server_groups,
        "media_urls": media_urls,
        "player_scripts": scripts[:25],
        "player_hints": hints,
    }
    cache_client.set_json(cache_key, payload, ttl=ANIME_CACHE_TTL_SECONDS)
    return payload
