"""
Enhanced Steam News Fetcher - Python Implementation
Properly extracts and formats Steam patch notes and news updates
"""

import re
import json
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

from ..core.config import STEAM_NEWS_MAX_COUNT

# Constants
STEAM_WEB_API_URL = "https://api.steampowered.com"
STEAM_STORE_URL = "https://store.steampowered.com"
STEAM_CDN_URL = "https://cdn.akamai.steamstatic.com"
REQUEST_TIMEOUT = 10


@dataclass
class NewsPatchNote:
    """Represents a single patch note entry"""
    title: str
    content: str
    category: str = "General"


@dataclass
class NewsArticle:
    """Enhanced news article with proper Steam formatting"""
    gid: str
    title: str
    url: str
    author: Optional[str]
    contents: str
    feed_label: str
    date: int
    feed_name: str
    tags: List[str]
    image: Optional[str]
    images: List[str]
    patch_notes: Optional[List[NewsPatchNote]] = None
    structured_content: Optional[Dict[str, Any]] = None


class SteamNewsEnhanced:
    """Enhanced Steam news fetcher matching Steam UI format"""

    # Regex patterns for content parsing
    PATCH_NOTE_PATTERNS = [
        r"## (.*?)(?=##|$)",  # Markdown headers
        r"\*\*(.*?)\*\*",  # Markdown bold
        r"### (.*?)(?=###|$)",  # H3 headers
    ]

    BB_PATTERNS = {
        r"\[h1\](.*?)\[/h1\]": "h1",
        r"\[h2\](.*?)\[/h2\]": "h2",
        r"\[h3\](.*?)\[/h3\]": "h3",
        r"\[b\](.*?)\[/b\]": "strong",
        r"\[i\](.*?)\[/i\]": "em",
        r"\[u\](.*?)\[/u\]": "u",
        r"\[s\](.*?)\[/s\]": "strike",
        r"\[list\](.*?)\[/list\]": "ul",
        r"\[\*\](.*?)(?=\[\*\]|\[/list\])": "li",
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OtoshiLauncher/1.0 +http://otoshi.com"
        })

    def fetch_news(self, app_id: str, count: int = 10) -> List[NewsArticle]:
        """Fetch and parse Steam news for an app"""
        resolved_count = _resolve_news_count(count)
        url = f"{STEAM_WEB_API_URL}/ISteamNews/GetNewsForApp/v2/"
        params = {
            "appid": app_id,
            "count": resolved_count,
            "maxlength": 0,
            "format": "json",
        }

        try:
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching news: {e}")
            return []

        app_news = data.get("appnews", {})
        news_items = app_news.get("newsitems", [])

        rss_entries = self._get_rss_entries(app_id)
        articles = []
        for item in news_items:
            article = self._parse_news_item(item, app_id, rss_entries)
            if article:
                articles.append(article)

        return articles

    def _parse_news_item(
        self,
        item: Dict[str, Any],
        app_id: str,
        rss_entries: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[NewsArticle]:
        """Parse a single news item into structured NewsArticle"""
        try:
            gid = str(item.get("gid", ""))
            title = item.get("title", "").strip()
            url = item.get("url", "")
            author = item.get("author", "").strip() or None
            contents = item.get("contents", "").strip()
            feed_label = item.get("feedlabel", "")
            date = item.get("date", 0)
            feed_name = item.get("feedname", "")
            tags = item.get("tags", [])

            # Extract images
            images = self._extract_all_images(contents, url)
            primary_image = images[0] if images else self._get_fallback_image(app_id)

            # Prefer RSS enclosure images for community announcements (Steam UI style)
            if self._is_community_announcement(feed_name, feed_label) and rss_entries:
                entry = self._match_rss_entry(title, date, rss_entries)
                if entry:
                    rss_image = entry.get("image")
                    if rss_image:
                        images = [rss_image] + [img for img in images if img != rss_image]
                        primary_image = rss_image
                    rss_desc = entry.get("description") or ""
                    if rss_desc and len(rss_desc) > len(contents):
                        contents = rss_desc
                    if entry.get("url"):
                        url = entry.get("url")

            # Parse patch notes if it looks like a patch
            patch_notes = None
            if self._is_patch_note(title, contents, feed_label):
                patch_notes = self._parse_patch_notes(contents)

            # Create structured content
            structured_content = self._structure_content(contents, feed_label)

            return NewsArticle(
                gid=gid,
                title=title,
                url=url,
                author=author,
                contents=contents,
                feed_label=feed_label,
                date=date,
                feed_name=feed_name,
                tags=tags,
                image=primary_image,
                images=images,
                patch_notes=patch_notes,
                structured_content=structured_content,
            )
        except Exception as e:
            print(f"Error parsing news item: {e}")
            return None

    def _extract_all_images(self, contents: str, url: str) -> List[str]:
        """Extract all images from content and metadata"""
        images: List[str] = []

        # Extract [img] tags
        img_tags: List[str] = re.findall(r"\[img\](.*?)\[/img\]", contents, re.IGNORECASE | re.DOTALL)
        images.extend([img.strip() for img in img_tags if img.strip()])

        # Extract HTML img tags
        html_imgs: List[str] = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', contents, re.IGNORECASE)
        images.extend(html_imgs)

        # Extract Steam static images
        steam_imgs: List[str] = re.findall(
            r'https?://[^\s"\']*steamstatic\.com[^\s"\']*\.(?:jpg|jpeg|png|gif|webp)',
            contents,
            re.IGNORECASE
        )
        images.extend(steam_imgs)

        # NOTE: Removed _get_og_image to avoid slow HTTP request per news item
        # This was adding 5+ seconds of latency per news item

        # Filter and normalize
        images = [self._normalize_image_url(img) for img in images]
        images = [img for img in images if img and img.startswith("http")]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_images = []
        for img in images:
            if img not in seen:
                seen.add(img)
                unique_images.append(img)

        return unique_images[:5]  # Limit to 5 images

    def _get_og_image(self, url: str) -> Optional[str]:
        """Fetch Open Graph image from URL"""
        if not url:
            return None

        try:
            response = self.session.get(url, timeout=5)
            response.raise_for_status()
            
            # Extract og:image meta tag
            og_match = re.search(
                r"<meta[^>]+property=[\"']og:image[\"'][^>]*content=[\"']([^\"']+)[\"']",
                response.text,
                re.IGNORECASE
            )
            if og_match:
                return og_match.group(1)

            # Extract twitter:image
            twitter_match = re.search(
                r"<meta[^>]+name=[\"']twitter:image[\"'][^>]*content=[\"']([^\"']+)[\"']",
                response.text,
                re.IGNORECASE
            )
            if twitter_match:
                return twitter_match.group(1)
        except Exception:
            pass

        return None

    def _normalize_image_url(self, url: str) -> str:
        """Normalize and validate image URL"""
        url = url.strip()

        if url.startswith("{STEAM_CLAN_IMAGE}"):
            url = url.replace("{STEAM_CLAN_IMAGE}", "https://clan.akamai.steamstatic.com/images")
        
        # Convert relative URLs to absolute
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = urljoin(STEAM_STORE_URL, url)

        return url

    def _get_fallback_image(self, app_id: str) -> Optional[str]:
        """Get game header image from Steam as fallback"""
        try:
            return f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg"
        except Exception:
            return None

    def _is_patch_note(self, title: str, contents: str, feed_label: str) -> bool:
        """Check if this looks like a patch note"""
        patch_keywords = [
            "patch", "update", "fix", "hotfix", "bug fix",
            "balance", "release", "version", "changelog"
        ]
        
        combined = f"{title} {feed_label} {contents[:200]}".lower()
        return any(keyword in combined for keyword in patch_keywords)

    def _parse_patch_notes(self, contents: str) -> List[NewsPatchNote]:
        """Parse patch notes into structured format"""
        notes: List[NewsPatchNote] = []
        
        # Clean content
        clean_content = self._clean_bb_content(contents)

        # Split by major sections/categories
        sections: List[str] = re.split(r"\n(?:#{1,3}\s+|###\s+)", clean_content)

        for section in sections:
            if not section.strip():
                continue

            lines: List[str] = section.strip().split("\n")
            if not lines:
                continue

            # First line is category/title
            title = lines[0].strip() if lines else "General"
            content = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""

            # Determine category
            category = self._categorize_patch(title)

            if content:
                notes.append(NewsPatchNote(
                    title=title,
                    content=content,
                    category=category
                ))

        return notes if notes else [NewsPatchNote(
            title="Patch Notes",
            content=self._clean_bb_content(contents)
        )]

    def _categorize_patch(self, text: str) -> str:
        """Categorize patch note section"""
        text_lower = text.lower()

        categories = {
            "Bug Fixes": ["bug", "fix", "fixed", "issue", "crash", "error"],
            "Balance": ["balance", "adjusted", "tweaked", "changed", "buffed", "nerfed"],
            "Features": ["new", "added", "feature", "content", "mode"],
            "Performance": ["performance", "optimization", "optimized", "fps", "lag"],
            "System": ["system", "system update", "infrastructure"],
        }

        for category, keywords in categories.items():
            if any(keyword in text_lower for keyword in keywords):
                return category

        return "General"

    def _clean_bb_content(self, content: str) -> str:
        """Convert Steam BB codes to plain text"""
        if not content:
            return ""

        # Normalize HTML breaks and list items before stripping tags
        content = re.sub(r"<br\s*/?>", "\n", content, flags=re.IGNORECASE)
        content = re.sub(r"<li[^>]*>", "\nâ€¢ ", content, flags=re.IGNORECASE)
        content = re.sub(r"</li>", "\n", content, flags=re.IGNORECASE)
        content = re.sub(r"<div[^>]*bb_h3[^>]*>", "\n", content, flags=re.IGNORECASE)
        content = re.sub(r"</div>", "\n", content, flags=re.IGNORECASE)

        # Remove [img] tags
        content = re.sub(r"\[img\].*?\[/img\]", "", content, flags=re.IGNORECASE | re.DOTALL)

        # Remove [url] tags
        content = re.sub(r"\[url=.*?\](.*?)\[/url\]", r"\1", content, flags=re.IGNORECASE)

        # Remove [h*] tags
        content = re.sub(r"\[h[1-6]\](.*?)\[/h[1-6]\]", r"\1", content, flags=re.IGNORECASE)

        # Convert [list] to markdown
        content = re.sub(r"\[list\]", "", content, flags=re.IGNORECASE)
        content = re.sub(r"\[/list\]", "", content, flags=re.IGNORECASE)
        content = re.sub(r"\[\*\]", "• ", content, flags=re.IGNORECASE)

        # Remove other formatting
        content = re.sub(r"\[.*?\]", "", content, flags=re.IGNORECASE)

        # Remove HTML tags
        content = re.sub(r"<[^>]+>", "", content)

        # Decode basic HTML entities
        content = (
            content.replace("&nbsp;", " ")
            .replace("&quot;", '"')
            .replace("&apos;", "'")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
        )

        # Clean up whitespace
        content = re.sub(r"\n{3,}", "\n\n", content)
        content = content.strip()

        return content

    def _structure_content(self, contents: str, feed_label: str) -> Dict[str, Any]:
        """Create structured representation of content"""
        sections, intro, meta = self._parse_structured_sections(contents)
        return {
            "raw": contents,
            "cleaned": self._clean_bb_content(contents),
            "feed_label": feed_label,
            "has_media": bool(self._extract_all_images(contents, "")),
            "intro": intro,
            "sections": sections,
            "meta": meta,
        }

    def _parse_structured_sections(self, contents: str) -> tuple[List[Dict[str, Any]], List[str], Dict[str, str]]:
        """Parse BBCode/HTML content into sections + bullets (Steam-style)"""
        if not contents:
            return [], [], {}

        meta: Dict[str, str] = {}
        text = contents.replace("\r\n", "\n")

        # Normalize common HTML into line-based markers before meta extraction
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<div[^>]*bb_h3[^>]*>", "\n### ", text, flags=re.IGNORECASE)
        text = re.sub(r"</div>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<ul[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</ul>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<li[^>]*>", "\n- ", text, flags=re.IGNORECASE)
        text = re.sub(r"</li>", "\n", text, flags=re.IGNORECASE)

        # Extract simple meta lines
        remaining_lines: List[str] = []
        for line in text.split("\n"):
            stripped = line.strip()
            lower = stripped.lower()
            if lower.startswith("version:"):
                meta["version"] = stripped.split(":", 1)[1].strip()
                continue
            if lower.startswith("update time:"):
                meta["update_time"] = stripped.split(":", 1)[1].strip()
                continue
            remaining_lines.append(line)
        text = "\n".join(remaining_lines)

        # Normalize BBCode to a line-based format
        text = re.sub(r"\[h[1-6]\](.*?)\[/h[1-6]\]", r"\n### \1\n", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\n\s*\[b\](.*?)\[/b\]\s*\n", r"\n#### \1\n", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\n\s*<b>(.*?)</b>\s*\n", r"\n#### \1\n", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\n\s*<strong>(.*?)</strong>\s*\n", r"\n#### \1\n", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\[list\]", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"\[/list\]", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"\[\*\]", "\n- ", text)
        text = re.sub(r"\[url=([^\]]+)\]([^\[]*?)\[/url\]", r"\2 (\1)", text, flags=re.IGNORECASE)
        text = re.sub(r"\[b\](.*?)\[/b\]", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\[i\](.*?)\[/i\]", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\[u\](.*?)\[/u\]", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\[s\](.*?)\[/s\]", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"\[img\].*?\[/img\]", "", text, flags=re.IGNORECASE | re.DOTALL)

        # Remove HTML tags + decode entities
        text = re.sub(r"<[^>]+>", "", text)
        text = (
            text.replace("&nbsp;", " ")
            .replace("&quot;", '"')
            .replace("&apos;", "'")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
        )

        lines = [line.strip() for line in text.split("\n")]
        lines = [line for line in lines if line]

        sections: List[Dict[str, Any]] = []
        intro: List[str] = []
        current: Optional[Dict[str, Any]] = None
        current_sub: Optional[Dict[str, Any]] = None

        def ensure_section(title: Optional[str] = None) -> Dict[str, Any]:
            nonlocal current
            if current is None:
                current = {
                    "title": title or "General",
                    "bullets": [],
                    "paragraphs": [],
                    "subsections": [],
                }
                sections.append(current)
            return current

        for line in lines:
            if line.startswith("### "):
                title = line[4:].strip()
                current = {
                    "title": title,
                    "bullets": [],
                    "paragraphs": [],
                    "subsections": [],
                }
                sections.append(current)
                current_sub = None
                continue

            if line.startswith("#### "):
                section = ensure_section()
                current_sub = {
                    "title": line[5:].strip(),
                    "bullets": [],
                    "paragraphs": [],
                }
                section["subsections"].append(current_sub)
                continue

            if line.startswith("- "):
                if current_sub is not None:
                    current_sub["bullets"].append(line[2:].strip())
                else:
                    section = ensure_section()
                    section["bullets"].append(line[2:].strip())
                continue

            if line.startswith("•"):
                if current_sub is not None:
                    current_sub["bullets"].append(line.lstrip("•").strip())
                else:
                    section = ensure_section()
                    section["bullets"].append(line.lstrip("•").strip())
                continue

            if current is None:
                intro.append(line)
            else:
                if current_sub is not None:
                    current_sub["paragraphs"].append(line)
                else:
                    current["paragraphs"].append(line)

        return sections, intro, meta

    def _get_rss_entries(self, app_id: str) -> Optional[List[Dict[str, Any]]]:
        """Fetch Steam RSS feed entries (cached)"""
        try:
            from ..core.cache import cache_client
        except Exception:
            cache_client = None

        cache_key = f"steam:news:rss:{app_id}"
        if cache_client:
            cached = cache_client.get_json(cache_key)
            if cached is not None:
                return cached

        url = f"https://store.steampowered.com/feeds/news/app/{app_id}?l=english"
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            channel = root.find("channel")
            if channel is None:
                return []
            entries: List[Dict[str, Any]] = []
            for item in channel.findall("item"):
                title = (item.findtext("title") or "").strip()
                enclosure = item.find("enclosure")
                image = enclosure.attrib.get("url") if enclosure is not None else None
                description = (item.findtext("description") or "").strip()
                link = (item.findtext("link") or item.findtext("guid") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                ts = 0
                if pub_date:
                    try:
                        dt = parsedate_to_datetime(pub_date)
                        ts = int(dt.timestamp())
                    except Exception:
                        ts = 0
                if title and image:
                    entries.append({
                        "title": title,
                        "image": image,
                        "description": description,
                        "url": link,
                        "date": ts,
                    })
            if cache_client:
                cache_client.set_json(cache_key, entries, ttl=1800)
            return entries
        except Exception:
            if cache_client:
                cache_client.set_json(cache_key, [], ttl=600)
            return []

    def _match_rss_entry(
        self,
        title: str,
        date: int,
        entries: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not title or not entries:
            return None
        norm_title = self._normalize_title(title)
        for entry in entries:
            if self._normalize_title(entry.get("title", "")) == norm_title:
                return entry
        best_entry = None
        best_score = 0.0
        for entry in entries:
            score = self._title_similarity(norm_title, self._normalize_title(entry.get("title", "")))
            if score > best_score:
                best_score = score
                best_entry = entry
        if best_entry and best_score >= 0.78:
            return best_entry
        if date:
            closest = None
            closest_delta = None
            for entry in entries:
                entry_date = entry.get("date") or 0
                if not entry_date:
                    continue
                delta = abs(entry_date - date)
                if closest_delta is None or delta < closest_delta:
                    closest_delta = delta
                    closest = entry
            if closest and closest_delta is not None and closest_delta <= 14 * 86400:
                return closest
        return None

    def _is_community_announcement(self, feed_name: str, feed_label: str) -> bool:
        if (feed_name or "").lower() == "steam_community_announcements":
            return True
        return "community" in (feed_label or "").lower()

    def _normalize_title(self, title: str) -> str:
        if not title:
            return ""
        clean = title.lower()
        clean = clean.replace("—", "-").replace("–", "-")
        clean = clean.replace("\"", "").replace("'", "")
        clean = re.sub(r"[^a-z0-9]+", " ", clean)
        return re.sub(r"\s+", " ", clean).strip()

    def _title_similarity(self, a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        set_a = set(a.split())
        set_b = set(b.split())
        if not set_a or not set_b:
            return 0.0
        return len(set_a & set_b) / float(len(set_a | set_b))

    def to_dict(self, article: NewsArticle) -> Dict[str, Any]:
        """Convert NewsArticle to dictionary"""
        patch_notes_list: Optional[List[Dict[str, Any]]] = None
        if article.patch_notes:
            patch_notes_list = [
                {
                    "title": pn.title,
                    "content": pn.content,
                    "category": pn.category
                }
                for pn in article.patch_notes
            ]
        
        return {
            "gid": article.gid,
            "title": article.title,
            "url": article.url,
            "author": article.author,
            "contents": article.contents,
            "feed_label": article.feed_label,
            "date": article.date,
            "feed_name": article.feed_name,
            "tags": article.tags,
            "image": article.image,
            "images": article.images,
            "patch_notes": patch_notes_list,
            "structured_content": article.structured_content,
        }


# Singleton instance
_news_fetcher: Optional[SteamNewsEnhanced] = None


def _resolve_news_count(count: int) -> int:
    if not count or count <= 0:
        return STEAM_NEWS_MAX_COUNT
    return min(count, STEAM_NEWS_MAX_COUNT)


def get_news_fetcher() -> SteamNewsEnhanced:
    """Get or create news fetcher instance"""
    global _news_fetcher
    if _news_fetcher is None:
        _news_fetcher = SteamNewsEnhanced()
    return _news_fetcher


def fetch_news_enhanced(app_id: str, count: int = 10) -> List[Dict[str, Any]]:
    """Public API to fetch enhanced news - includes caching"""
    from ..core.cache import cache_client
    
    # Check cache first
    resolved_count = _resolve_news_count(count)
    cache_key = f"steam:news:enhanced:v3:{app_id}:{resolved_count}"
    cached = cache_client.get_json(cache_key)
    if cached is not None:
        return cached
    
    fetcher = get_news_fetcher()
    articles = fetcher.fetch_news(app_id, resolved_count)
    result = [fetcher.to_dict(article) for article in articles]
    
    # Cache for 30 minutes
    cache_client.set_json(cache_key, result, ttl=1800)
    
    return result
