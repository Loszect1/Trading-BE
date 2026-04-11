from __future__ import annotations

from typing import Any, Literal

import httpx

from app.core.config import settings
from app.core.firecrawl_press_config import (
    PressScope,
    build_press_today_query,
    press_search_keywords_clause,
)

_FIRECRAWL_BASE_URL = "https://api.firecrawl.dev/v2"


class FirecrawlSearchService:
    """Gọi Firecrawl Search API (v2) để tìm trang / feed; cần biến môi trường FIRECRAWL_API_KEY."""

    def __init__(self) -> None:
        self._api_key = (settings.firecrawl_api_key or "").strip()

    def is_configured(self) -> bool:
        return bool(self._api_key)

    def search(
        self,
        query: str,
        *,
        limit: int,
        sources: list[dict[str, Any]],
        country: str | None,
        timeout_ms: int,
        tbs: str | None = None,
    ) -> dict[str, Any]:
        if not self.is_configured():
            raise RuntimeError("Firecrawl API key is not configured")

        payload: dict[str, Any] = {
            "query": query.strip(),
            "limit": max(1, min(limit, 100)),
            "sources": sources,
            "timeout": max(1000, min(timeout_ms, 300_000)),
        }
        if tbs:
            payload["tbs"] = tbs.strip()
        if country:
            payload["country"] = country.strip().upper()

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        timeout = httpx.Timeout(payload["timeout"] / 1000.0 + 5.0)
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.post(
                    f"{_FIRECRAWL_BASE_URL}/search",
                    json=payload,
                    headers=headers,
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text[:2000] if exc.response is not None else ""
            raise RuntimeError(
                f"Firecrawl HTTP {exc.response.status_code if exc.response else 'error'}: {detail}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(f"Firecrawl request failed: {exc.__class__.__name__}") from exc

        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("Firecrawl returned non-JSON body") from exc

        if not isinstance(body, dict):
            raise RuntimeError("Firecrawl returned unexpected JSON shape")
        return body

    def fetch_press_today(
        self,
        *,
        scope: PressScope,
        max_items: int,
        timeout_ms: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Tin báo trong ngày (tbs=qdr:d) từ các domain đã cấu hình; trả về (items, meta).
        """
        meta: dict[str, Any] = {
            "query": None,
            "country": None,
            "error": None,
            "credits_used": None,
            "warning": None,
        }
        if not self.is_configured():
            meta["error"] = "firecrawl_api_key_missing"
            return [], meta

        query, country = build_press_today_query(scope)
        meta["query"] = query
        meta["country"] = country
        cap = max(1, min(int(max_items), 100))
        try:
            raw = self.search(
                query,
                limit=cap,
                sources=[{"type": "news"}, {"type": "web"}],
                country=country,
                timeout_ms=timeout_ms,
                tbs="qdr:d",
            )
        except RuntimeError as exc:
            meta["error"] = str(exc)
            return [], meta

        meta["credits_used"] = raw.get("creditsUsed")
        meta["warning"] = raw.get("warning")
        items = firecrawl_raw_to_press_items(raw, cap)
        return items, meta

    def fetch_rss_fallback_for_source(
        self,
        *,
        feed_url: str,
        display_name: str,
        source_id: str,
        category: str,
        max_items: int,
        timeout_ms: int,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """
        Khi RSS feed lỗi: tìm bài trong ngày trên cùng domain qua Firecrawl Search (tbs=qdr:d).
        """
        if not self.is_configured():
            return [], "firecrawl_api_key_missing"

        host = _domain_from_feed_url(feed_url)
        if not host:
            return [], "cannot_parse_feed_host"

        cap = max(1, min(int(max_items), 25))
        query = f"site:{host} ({press_search_keywords_clause()})"
        country = _search_country_for_host(host)
        try:
            raw = self.search(
                query,
                limit=cap,
                sources=[{"type": "news"}, {"type": "web"}],
                country=country,
                timeout_ms=timeout_ms,
                tbs="qdr:d",
            )
        except RuntimeError as exc:
            return [], str(exc)

        base = firecrawl_raw_to_press_items(raw, cap)
        items: list[dict[str, Any]] = []
        for row in base:
            item = dict(row)
            item["item_origin"] = "firecrawl_rss_fallback"
            item["fallback_for_source_id"] = source_id
            item["fallback_for_feed_url"] = feed_url
            item["source_id"] = source_id
            item["source_name"] = f"{display_name} (Firecrawl)"
            item["category"] = category
            items.append(item)
        return items, None


def _domain_from_feed_url(feed_url: str) -> str | None:
    from urllib.parse import urlparse

    try:
        parsed = urlparse((feed_url or "").strip())
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def _search_country_for_host(host: str) -> str | None:
    h = host.lower()
    if h.endswith(".vn"):
        return "VN"
    if any(marker in h for marker in ("reuters.", "bbc.", "bloomberg.", "yahoo.", "reddit.")):
        return "US"
    return None


def _press_host_category(url: str) -> Literal["domestic", "world"]:
    from urllib.parse import urlparse

    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
    except Exception:
        return "domestic"
    world_markers = (
        "reuters.",
        "bbc.",
        "bloomberg.",
        "yahoo.",
    )
    if any(marker in netloc for marker in world_markers):
        return "world"
    return "domestic"


def _normalize_press_date(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _map_press_row_to_item(bucket: str, row: dict[str, Any]) -> dict[str, Any]:
    url = str(row.get("url") or "").strip()
    title = str(row.get("title") or "").strip()
    description = str(row.get("snippet") or row.get("description") or "").strip()
    date_raw = row.get("date") if bucket == "news" else None
    published = _normalize_press_date(date_raw)
    return {
        "title": title,
        "link": url,
        "summary": description[:500] if description else "",
        "published_at": published,
        "source_id": "firecrawl_today",
        "source_name": "Firecrawl (tin trong ngày)",
        "category": _press_host_category(url),
        "item_origin": "firecrawl",
    }


def firecrawl_raw_to_press_items(raw: dict[str, Any], max_items: int) -> list[dict[str, Any]]:
    """Gộp bucket news (ưu tiên) + web, loại trùng URL, tối đa max_items."""
    data = raw.get("data")
    if not isinstance(data, dict):
        return []
    seen: set[str] = set()
    candidates: list[tuple[str, dict[str, Any]]] = []
    for bucket in ("news", "web"):
        rows = data.get(bucket)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            url = str(row.get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            candidates.append((bucket, row))

    out: list[dict[str, Any]] = []
    for bucket, row in candidates[:max_items]:
        out.append(_map_press_row_to_item(bucket, row))
    return out


def _rss_hint(url: str) -> bool:
    lower = url.lower()
    return any(
        token in lower
        for token in (".rss", "/rss", "/feed", "/atom", "format=rss", "type=rss")
    )


def normalize_firecrawl_search_response(api_response: dict[str, Any]) -> dict[str, Any]:
    """Thu gọn payload Firecrawl để trả về client (không lưu markdown/html đầy đủ)."""
    success = api_response.get("success")
    data_block = api_response.get("data")
    credits = api_response.get("creditsUsed")
    warning = api_response.get("warning")
    job_id = api_response.get("id")
    buckets: dict[str, list[dict[str, Any]]] = {}

    if isinstance(data_block, dict):
        for bucket in ("web", "news", "images"):
            rows = data_block.get(bucket)
            if not isinstance(rows, list):
                continue
            normalized: list[dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                url = str(row.get("url") or "").strip()
                title = str(row.get("title") or "").strip()
                description = str(
                    row.get("description") or row.get("snippet") or ""
                ).strip()
                item: dict[str, Any] = {
                    "url": url,
                    "title": title,
                    "description": description[:800] if description else "",
                    "rss_hint": _rss_hint(url),
                }
                if bucket == "news" and row.get("date"):
                    item["date"] = row.get("date")
                if bucket == "images" and row.get("imageUrl"):
                    item["image_url"] = row.get("imageUrl")
                normalized.append(item)
            if normalized:
                buckets[bucket] = normalized

    return {
        "success": bool(success),
        "credits_used": credits,
        "warning": warning,
        "job_id": job_id,
        "buckets": buckets,
    }
