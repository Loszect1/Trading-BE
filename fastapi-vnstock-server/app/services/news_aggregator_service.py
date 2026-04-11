from __future__ import annotations

import html as html_module
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from xml.etree import ElementTree

import httpx

from app.core.news_feed_registry import NEWS_FEED_SOURCES, NewsCategory, NewsFeedSource

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_DEFAULT_UA = (
    "Mozilla/5.0 (compatible; VNStockNewsBot/1.0; +https://github.com/) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _local_tag(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _strip_html(raw: str | None, max_len: int = 400) -> str:
    if not raw:
        return ""
    text = _HTML_TAG_RE.sub(" ", raw)
    text = html_module.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        return text[: max_len - 1].rstrip() + "…"
    return text


def _parse_datetime_value(raw: str | None) -> datetime | None:
    if not raw:
        return None
    value = raw.strip()
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %y %H:%M:%S %z",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None and fmt.endswith("Z"):
                return dt.replace(tzinfo=timezone.utc)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _text_or_none(element: ElementTree.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    return element.text.strip()


def _rss_find_channel(root: ElementTree.Element) -> ElementTree.Element | None:
    for child in root:
        if _local_tag(child.tag) == "channel":
            return child
    return None


def _rss_find_items(channel: ElementTree.Element) -> list[ElementTree.Element]:
    return [child for child in channel if _local_tag(child.tag) == "item"]


def _atom_find_entries(root: ElementTree.Element) -> list[ElementTree.Element]:
    return [child for child in root if _local_tag(child.tag) == "entry"]


def _atom_entry_link(entry: ElementTree.Element) -> str | None:
    for child in entry:
        if _local_tag(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href.strip()
    return None


def _parse_feed_xml(content: bytes) -> list[dict[str, Any]]:
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError:
        return []

    local_root = _local_tag(root.tag)
    items: list[dict[str, Any]] = []

    if local_root == "rss":
        channel = _rss_find_channel(root)
        if channel is None:
            return []
        for item in _rss_find_items(channel):
            title_el = next(
                (c for c in item if _local_tag(c.tag) == "title"),
                None,
            )
            link_el = next(
                (c for c in item if _local_tag(c.tag) == "link"),
                None,
            )
            desc_el = next(
                (c for c in item if _local_tag(c.tag) in ("description", "summary")),
                None,
            )
            pub_el = next(
                (
                    c
                    for c in item
                    if _local_tag(c.tag) in ("pubDate", "published", "updated", "date")
                ),
                None,
            )
            guid_el = next(
                (c for c in item if _local_tag(c.tag) == "guid"),
                None,
            )
            title = _text_or_none(title_el) or ""
            link = (_text_or_none(link_el) or "").strip()
            summary = _strip_html(_text_or_none(desc_el))
            pub_raw = _text_or_none(pub_el)
            guid = (_text_or_none(guid_el) or "").strip()
            if not link and guid.startswith("http"):
                link = guid
            items.append(
                {
                    "title": html_module.unescape(title).strip(),
                    "link": link,
                    "summary": summary,
                    "published_at": _parse_datetime_value(pub_raw),
                    "guid": guid or link,
                }
            )
        return items

    if local_root == "feed":
        for entry in _atom_find_entries(root):
            title_el = next(
                (c for c in entry if _local_tag(c.tag) == "title"),
                None,
            )
            summary_el = next(
                (
                    c
                    for c in entry
                    if _local_tag(c.tag) in ("summary", "content")
                ),
                None,
            )
            updated_el = next(
                (
                    c
                    for c in entry
                    if _local_tag(c.tag) in ("updated", "published")
                ),
                None,
            )
            title = _text_or_none(title_el) or ""
            link = _atom_entry_link(entry) or ""
            summary = _strip_html(_text_or_none(summary_el))
            pub_raw = _text_or_none(updated_el)
            items.append(
                {
                    "title": html_module.unescape(title).strip(),
                    "link": link.strip(),
                    "summary": summary,
                    "published_at": _parse_datetime_value(pub_raw),
                    "guid": link.strip() or title,
                }
            )
        return items

    return []


def _fetch_feed_http(
    client: httpx.Client,
    source: NewsFeedSource,
    per_feed_limit: int,
) -> tuple[NewsFeedSource, list[dict[str, Any]] | None, str | None]:
    try:
        response = client.get(source.feed_url)
        response.raise_for_status()
        parsed = _parse_feed_xml(response.content)
        trimmed: list[dict[str, Any]] = []
        for row in parsed[: max(1, per_feed_limit)]:
            if not row.get("title") and not row.get("link"):
                continue
            trimmed.append(row)
        return source, trimmed, None
    except httpx.HTTPStatusError as exc:
        return source, None, f"HTTP {exc.response.status_code}"
    except httpx.RequestError as exc:
        return source, None, f"Request error: {exc.__class__.__name__}"
    except Exception as exc:
        return source, None, f"Unexpected error: {exc.__class__.__name__}"


class NewsAggregatorService:
    def aggregate(
        self,
        categories: set[NewsCategory] | None,
        per_feed_limit: int,
        max_workers: int,
        request_timeout_seconds: float,
    ) -> dict[str, Any]:
        selected_sources = [
            src
            for src in NEWS_FEED_SOURCES
            if categories is None or src.category in categories
        ]
        feed_errors: dict[str, str] = {}
        merged: list[dict[str, Any]] = []

        limits = httpx.Limits(max_keepalive_connections=5, max_connections=20)
        timeout = httpx.Timeout(request_timeout_seconds)
        headers = {"User-Agent": _DEFAULT_UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"}

        with httpx.Client(timeout=timeout, limits=limits, headers=headers, follow_redirects=True) as client:
            workers = max(1, min(max_workers, len(selected_sources) or 1))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(_fetch_feed_http, client, src, per_feed_limit)
                    for src in selected_sources
                ]
                for future in as_completed(futures):
                    try:
                        source, rows, err = future.result()
                    except Exception as exc:
                        feed_errors["unknown"] = f"Worker error: {exc.__class__.__name__}"
                        continue
                    if err:
                        feed_errors[source.id] = err
                        continue
                    if not rows:
                        feed_errors[source.id] = "Empty or unparsable feed"
                        continue
                    for row in rows:
                        published = row.get("published_at")
                        merged.append(
                            {
                                "title": row.get("title") or "",
                                "link": row.get("link") or "",
                                "summary": row.get("summary") or "",
                                "published_at": published.isoformat()
                                if isinstance(published, datetime)
                                else None,
                                "published_sort": published.timestamp()
                                if isinstance(published, datetime)
                                else 0.0,
                                "source_id": source.id,
                                "source_name": source.display_name,
                                "category": source.category,
                            }
                        )

        seen_links: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for item in sorted(merged, key=lambda row: row["published_sort"], reverse=True):
            link_key = (item.get("link") or "").strip()
            if link_key:
                if link_key in seen_links:
                    continue
                seen_links.add(link_key)
            deduped.append({k: v for k, v in item.items() if k != "published_sort"})

        return {
            "items": deduped,
            "feed_errors": feed_errors,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
