from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import subprocess
import tempfile
import threading
import time
import unicodedata
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.gmail_service import GmailFetchService
from app.services.signal_engine_service import ensure_market_symbol_tables, vnstock_api_service

logger = logging.getLogger(__name__)

_gmail = GmailFetchService()
_NEWS_MAIL_ANALYSIS_BATCH_SIZE = 4
_NEWS_MAIL_CODEX_TIMEOUT_SECONDS = 1800
_NEWS_MAIL_SCHEMA_LOCK_CLASS_ID = 20260601
_NEWS_MAIL_SCHEMA_LOCK_OBJECT_ID = 8
_news_mail_tables_ready = False
_news_mail_tables_lock = threading.Lock()
_ARTICLE_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
_ARTICLE_FETCH_HEADERS = {
    "User-Agent": _ARTICLE_BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Upgrade-Insecure-Requests": "1",
}
_ARTICLE_FETCH_RETRY_HEADERS = {
    **_ARTICLE_FETCH_HEADERS,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Referer": "https://finance.yahoo.com/",
}
_BLOCKED_FETCH_HINTS = (
    "datadome",
    "please enable js",
    "please enable javascript",
    "disable any ad blocker",
    "enable cookies",
)
_GENERAL_NEWS_CATEGORY = "General"
_WORLD_NEWS_CATEGORY = "World"
_NEWS_MAIL_CATEGORY_LABELS = [
    "Dầu khí",
    "Tài nguyên Cơ bản",
    "Hàng & Dịch vụ Công nghiệp",
    "Thực phẩm và đồ uống",
    "Y tế",
    "Truyền thông",
    "Viễn thông",
    "Ngân hàng",
    "Bất động sản",
    "Công nghệ Thông tin",
    "Hóa chất",
    "Xây dựng và Vật liệu",
    "Ô tô và phụ tùng",
    "Hàng cá nhân & Gia dụng",
    "Bán lẻ",
    "Du lịch và Giải trí",
    "Điện, nước & xăng dầu khí đốt",
    "Bảo hiểm",
    "Dịch vụ tài chính",
    _WORLD_NEWS_CATEGORY,
    _GENERAL_NEWS_CATEGORY,
]
_KBS_INDUSTRY_CATEGORY_BY_NAME = {
    "Bán buôn": "Bán lẻ",
    "Bảo hiểm": "Bảo hiểm",
    "Bất động sản": "Bất động sản",
    "Chứng khoán": "Dịch vụ tài chính",
    "Công nghệ và thông tin": "Công nghệ Thông tin",
    "Bán lẻ": "Bán lẻ",
    "Chăm sóc sức khỏe": "Y tế",
    "Khai khoáng": "Tài nguyên Cơ bản",
    "Ngân hàng": "Ngân hàng",
    "Nông - Lâm - Ngư": "Thực phẩm và đồ uống",
    "SX Thiết bị, máy móc": "Hàng & Dịch vụ Công nghiệp",
    "SX Hàng gia dụng": "Hàng cá nhân & Gia dụng",
    "Sản phẩm cao su": "Hóa chất",
    "SX Nhựa - Hóa chất": "Hóa chất",
    "Thực phẩm - Đồ uống": "Thực phẩm và đồ uống",
    "Chế biến Thủy sản": "Thực phẩm và đồ uống",
    "Vật liệu xây dựng": "Xây dựng và Vật liệu",
    "Tiện ích": "Điện, nước & xăng dầu khí đốt",
    "Vận tải - kho bãi": "Hàng & Dịch vụ Công nghiệp",
    "Xây dựng": "Xây dựng và Vật liệu",
    "Dịch vụ lưu trú, ăn uống, giải trí": "Du lịch và Giải trí",
    "SX Phụ trợ": "Hàng & Dịch vụ Công nghiệp",
    "Thiết bị điện": "Hàng & Dịch vụ Công nghiệp",
    "Dịch vụ tư vấn, hỗ trợ": "Hàng & Dịch vụ Công nghiệp",
    "Tài chính khác": "Dịch vụ tài chính",
}
_VN_SYMBOL_CATEGORY_CACHE: dict[str, dict[str, str]] | None = None
_CODEX_NEWS_MAIL_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["articles"],
    "properties": {
        "articles": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "article_id",
                    "summary",
                    "key_points",
                    "sector_tags",
                    "market_tags",
                    "data_gaps",
                    "symbols",
                ],
                "properties": {
                    "article_id": {"type": "string", "minLength": 1},
                    "summary": {"type": "string"},
                    "key_points": {"type": "array", "items": {"type": "string"}},
                    "sector_tags": {"type": "array", "items": {"type": "string"}},
                    "market_tags": {"type": "array", "items": {"type": "string"}},
                    "data_gaps": {"type": "array", "items": {"type": "string"}},
                    "symbols": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "symbol",
                                "company_name",
                                "relevance_score",
                                "sentiment_label",
                                "sentiment_score",
                                "impact_score",
                                "impact_horizon",
                                "confidence",
                                "rationale",
                            ],
                            "properties": {
                                "symbol": {"type": "string", "minLength": 1, "maxLength": 20},
                                "company_name": {"type": ["string", "null"]},
                                "relevance_score": {"type": "number", "minimum": 0, "maximum": 100},
                                "sentiment_label": {
                                    "type": "string",
                                    "enum": ["positive", "negative", "neutral", "mixed"],
                                },
                                "sentiment_score": {"type": "number", "minimum": -100, "maximum": 100},
                                "impact_score": {"type": "number", "minimum": 0, "maximum": 100},
                                "impact_horizon": {"type": ["string", "null"]},
                                "confidence": {"type": "number", "minimum": 0, "maximum": 100},
                                "rationale": {"type": ["string", "null"]},
                            },
                        },
                    },
                },
            },
        },
    },
}
_URL_RE = re.compile(r"https?://[^\s<>()\"']+", flags=re.IGNORECASE)
_WRAPPED_URL_RE = re.compile(
    r"https?://[^\s<>()\"']+"
    r"(?:\s*\r?\n\s*[A-Za-z0-9][A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*[./?&=_%]"
    r"[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)*",
    flags=re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_RE = re.compile(
    r"<(script|style|noscript|svg|iframe)\b[^>]*>.*?</\1>",
    flags=re.IGNORECASE | re.DOTALL,
)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _local_today() -> date:
    return datetime.now(tz=ZoneInfo(settings.mail_signal_scheduler_timezone)).date()


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _as_json(value: Any) -> Json:
    return Json(_jsonable(value))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _host_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def _normalize_url(raw: str) -> str:
    value = html.unescape(str(raw or "")).strip()
    value = re.sub(r"=\r?\n", "", value)
    if re.match(r"^https?://", value, flags=re.IGNORECASE):
        value = re.sub(r"\s+", "", value)
    value = value.rstrip(".,;)]}")
    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    return value


def _text_url_candidates(text: str) -> list[str]:
    raw_text = html.unescape(str(text or ""))
    candidates: list[str] = []
    seen_spans: set[tuple[int, int]] = set()
    for match in _WRAPPED_URL_RE.finditer(raw_text):
        seen_spans.add(match.span())
        candidates.append(match.group(0))
    for match in _URL_RE.finditer(raw_text):
        span = match.span()
        if any(start <= span[0] and span[1] <= end for start, end in seen_spans):
            continue
        candidates.append(match.group(0))
    return candidates


def _clean_mail_heading(raw: str) -> str:
    text = re.sub(r"^\s*#{1,6}\s+", "", str(raw or "")).strip()
    text = text.replace("**", "").replace("`", "")
    return _compact_text(text, 240)


def _explicit_link_field_rows(text: str) -> list[dict[str, Any]]:
    raw_text = html.unescape(str(text or ""))
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    current_title = ""
    lines = raw_text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r"^#{1,6}\s+", stripped):
            current_title = _clean_mail_heading(stripped)
            continue
        plain_label = stripped.replace("**", "")
        match = re.match(r"^(?:[-*]\s*)?Link\s*:\s*(?P<rest>.*)$", plain_label, flags=re.IGNORECASE)
        if not match:
            continue
        candidate_text = match.group("rest").strip()
        for next_line in lines[index + 1 : index + 5]:
            next_stripped = next_line.strip()
            if not next_stripped:
                break
            if next_stripped.startswith("#") or re.match(r"^[-*]\s+\*\*", next_stripped):
                break
            if re.match(r"^[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+$", next_stripped):
                candidate_text = f"{candidate_text}\n{next_stripped}"
                continue
            break
        for raw_url in _text_url_candidates(candidate_text):
            url = _normalize_url(raw_url)
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append(
                {
                    "section_index": len(rows) + 1,
                    "section_title": current_title,
                    "url": url,
                }
            )
    return rows


def _compact_text(raw: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", str(raw or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def _ascii_slug(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.replace("Đ", "D").replace("đ", "d")
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_text).strip("_").lower()
    return slug


_NEWS_MAIL_CATEGORY_LABEL_BY_SLUG = {_ascii_slug(label): label for label in _NEWS_MAIL_CATEGORY_LABELS}


def _canonical_news_mail_category(raw: str | None) -> tuple[str, str]:
    slug = _ascii_slug(raw or "")
    if not slug:
        slug = _ascii_slug(_GENERAL_NEWS_CATEGORY)
    label = _NEWS_MAIL_CATEGORY_LABEL_BY_SLUG.get(slug)
    if label:
        return label, slug
    return _GENERAL_NEWS_CATEGORY, _ascii_slug(_GENERAL_NEWS_CATEGORY)


def _category_from_industry_name(industry: str | None) -> tuple[str, str]:
    raw = str(industry or "").strip()
    if not raw:
        return _canonical_news_mail_category(_GENERAL_NEWS_CATEGORY)
    mapped = _KBS_INDUSTRY_CATEGORY_BY_NAME.get(raw)
    if mapped:
        return _canonical_news_mail_category(mapped)
    industry_slug = _ascii_slug(raw)
    for key, category in {
        "dau_khi": "Dầu khí",
        "xang_dau": "Dầu khí",
        "hoa_dau": "Dầu khí",
        "petro": "Dầu khí",
        "oil": "Dầu khí",
        "gas": "Dầu khí",
        "ngan_hang": "Ngân hàng",
        "bank": "Ngân hàng",
        "chung_khoan": "Dịch vụ tài chính",
        "securities": "Dịch vụ tài chính",
        "bao_hiem": "Bảo hiểm",
        "insurance": "Bảo hiểm",
        "bat_dong_san": "Bất động sản",
        "real_estate": "Bất động sản",
        "cong_nghe": "Công nghệ Thông tin",
        "technology": "Công nghệ Thông tin",
        "vien_thong": "Viễn thông",
        "telecom": "Viễn thông",
        "truyen_thong": "Truyền thông",
        "media": "Truyền thông",
        "hang_khong": "Du lịch và Giải trí",
        "du_lich": "Du lịch và Giải trí",
        "travel": "Du lịch và Giải trí",
        "thuc_pham": "Thực phẩm và đồ uống",
        "do_uong": "Thực phẩm và đồ uống",
        "y_te": "Y tế",
        "health": "Y tế",
        "hoa_chat": "Hóa chất",
        "chemical": "Hóa chất",
        "xay_dung": "Xây dựng và Vật liệu",
        "vat_lieu": "Xây dựng và Vật liệu",
        "oto": "Ô tô và phụ tùng",
        "automobile": "Ô tô và phụ tùng",
        "gia_dung": "Hàng cá nhân & Gia dụng",
        "ban_le": "Bán lẻ",
        "retail": "Bán lẻ",
        "dien": "Điện, nước & xăng dầu khí đốt",
        "nuoc": "Điện, nước & xăng dầu khí đốt",
        "utility": "Điện, nước & xăng dầu khí đốt",
        "khai_khoang": "Tài nguyên Cơ bản",
        "mining": "Tài nguyên Cơ bản",
    }.items():
        if key in industry_slug:
            return _canonical_news_mail_category(category)
    return _canonical_news_mail_category(_GENERAL_NEWS_CATEGORY)


def _category_from_symbol_info(info: dict[str, Any] | None) -> tuple[str, str]:
    if not isinstance(info, dict):
        return _canonical_news_mail_category(_GENERAL_NEWS_CATEGORY)
    for key in ("industry", "industry_name", "industryName", "icb_name", "icbName", "sector", "company_type"):
        value = info.get(key)
        if isinstance(value, str) and value.strip():
            category = _category_from_industry_name(value)
            if category[1] != _ascii_slug(_GENERAL_NEWS_CATEGORY):
                return category
    combined = " ".join(
        str(info.get(key) or "")
        for key in ("organ_name", "en_organ_name", "business_model", "website")
    )
    return _category_from_industry_name(combined)


def _load_vn_symbol_category_cache() -> dict[str, dict[str, str]]:
    global _VN_SYMBOL_CATEGORY_CACHE
    if _VN_SYMBOL_CATEGORY_CACHE is not None:
        return _VN_SYMBOL_CATEGORY_CACHE
    out: dict[str, dict[str, str]] = {}
    try:
        rows = vnstock_api_service.call_listing(
            "symbols_by_industries",
            source="KBS",
            random_agent=False,
            show_log=False,
            method_kwargs={"lang": "vi"},
        )
    except Exception:
        logger.debug("news_mail_symbol_industry_lookup_failed", exc_info=True)
        rows = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("code") or "").strip().upper()
            if not symbol:
                continue
            industry = str(row.get("industry_name") or row.get("industry") or "").strip()
            category, slug = _category_from_industry_name(industry)
            out[symbol] = {"category": category, "category_slug": slug, "industry": industry}
    _VN_SYMBOL_CATEGORY_CACHE = out
    return out


def _market_symbol_info_by_symbol(symbols: list[str]) -> dict[str, dict[str, Any]]:
    normalized = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not normalized:
        return {}
    try:
        ensure_market_symbol_tables()
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT symbol, info
                    FROM market_symbols
                    WHERE symbol = ANY(%(symbols)s)
                    """,
                    {"symbols": normalized},
                )
                rows = cur.fetchall()
    except Exception:
        logger.debug("news_mail_market_symbol_info_lookup_failed", exc_info=True)
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        info = row.get("info")
        if symbol:
            out[symbol] = info if isinstance(info, dict) else {}
    return out


def _news_mail_symbol_category_lookup(symbols: list[str], known_symbols: set[str]) -> dict[str, dict[str, str]]:
    normalized = sorted({s.strip().upper() for s in symbols if s.strip()})
    industry_cache = _load_vn_symbol_category_cache()
    info_by_symbol = _market_symbol_info_by_symbol(normalized)
    out: dict[str, dict[str, str]] = {}
    for symbol in normalized:
        if symbol not in known_symbols:
            category, slug = _canonical_news_mail_category(_WORLD_NEWS_CATEGORY)
            out[symbol] = {"category": category, "category_slug": slug, "is_vn": "false"}
            continue
        cached = industry_cache.get(symbol)
        if cached:
            out[symbol] = {**cached, "is_vn": "true"}
            continue
        category, slug = _category_from_symbol_info(info_by_symbol.get(symbol))
        out[symbol] = {"category": category, "category_slug": slug, "is_vn": "true"}
    return out


def _news_mail_article_category_from_symbols(
    symbols: list[Any],
    symbol_categories: dict[str, dict[str, str]],
) -> tuple[str, str]:
    ranked: list[tuple[float, float, float, int, str]] = []
    for index, symbol_row in enumerate(symbols or []):
        if not isinstance(symbol_row, dict):
            continue
        symbol = str(symbol_row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        impact_score = _safe_float(symbol_row.get("impact_score"), 0) or 0.0
        relevance_score = _safe_float(symbol_row.get("relevance_score"), 0) or 0.0
        confidence = _safe_float(symbol_row.get("confidence"), 0) or 0.0
        ranked.append((impact_score, relevance_score, confidence, -index, symbol))
    if not ranked:
        return _canonical_news_mail_category(_GENERAL_NEWS_CATEGORY)
    ranked.sort(reverse=True)
    top_symbol = ranked[0][4]
    category = symbol_categories.get(top_symbol)
    if not category:
        return _canonical_news_mail_category(_WORLD_NEWS_CATEGORY)
    return _canonical_news_mail_category(category.get("category") or _GENERAL_NEWS_CATEGORY)


class _AnchorLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._active_href: str | None = None
        self._active_chunks: list[str] = []
        self.links: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = ""
        for key, value in attrs:
            if key.lower() == "href":
                href = value or ""
                break
        normalized = _normalize_url(href)
        if not normalized:
            return
        self._active_href = normalized
        self._active_chunks = []

    def handle_data(self, data: str) -> None:
        if self._active_href:
            self._active_chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._active_href:
            return
        title = _compact_text(" ".join(self._active_chunks), 240)
        self.links.append({"url": self._active_href, "section_title": title})
        self._active_href = None
        self._active_chunks = []


def extract_section_links(*, html_text: str, text: str) -> list[dict[str, Any]]:
    explicit_rows = _explicit_link_field_rows(text)
    if explicit_rows:
        return explicit_rows

    parser = _AnchorLinkParser()
    if html_text:
        try:
            parser.feed(html_text)
        except Exception:
            logger.debug("news_mail_html_link_parse_failed", exc_info=True)

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in parser.links:
        url = _normalize_url(item.get("url", ""))
        if not url or url in seen:
            continue
        seen.add(url)
        rows.append(
            {
                "section_index": len(rows) + 1,
                "section_title": item.get("section_title") or "",
                "url": url,
            }
        )

    if rows:
        return rows

    for raw_url in _text_url_candidates(text):
        url = _normalize_url(raw_url)
        if not url or url in seen:
            continue
        seen.add(url)
        rows.append(
            {
                "section_index": len(rows) + 1,
                "section_title": "",
                "url": url,
            }
        )
    return rows


def clean_article_html(content: str, max_chars: int | None = None) -> dict[str, str]:
    raw = content or ""
    title = ""
    title_match = re.search(r"<title[^>]*>(.*?)</title>", raw, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        title = _compact_text(html.unescape(_TAG_RE.sub(" ", title_match.group(1))), 300)
    body = _SCRIPT_STYLE_RE.sub(" ", raw)
    body = _TAG_RE.sub(" ", body)
    body = html.unescape(body)
    text = _compact_text(body, max_chars or int(settings.news_mail_article_text_max_chars))
    return {"title": title, "text": text}


def _blocked_fetch_provider(response: httpx.Response) -> str:
    status = int(response.status_code or 0)
    if status not in {401, 403, 429, 503}:
        return ""
    server = str(response.headers.get("server") or "").lower()
    body_sample = response.text[:5000].lower()
    if "datadome" in server or "datadome" in body_sample:
        return "datadome"
    if any(hint in body_sample for hint in _BLOCKED_FETCH_HINTS):
        return "anti_bot_challenge"
    return ""


def _fetch_attempt_metadata(response: httpx.Response, attempt: str) -> dict[str, Any]:
    blocked_by = _blocked_fetch_provider(response)
    metadata: dict[str, Any] = {
        "attempt": attempt,
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type", ""),
        "server": response.headers.get("server", ""),
        "final_url": str(response.url),
    }
    if blocked_by:
        metadata["blocked_by"] = blocked_by
    return metadata


def ensure_news_mail_tables() -> None:
    global _news_mail_tables_ready
    if _news_mail_tables_ready:
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS news_mail_runs (
        id UUID PRIMARY KEY,
        run_date DATE NOT NULL,
        source_query TEXT NOT NULL,
        status VARCHAR(32) NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ NULL,
        error TEXT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (run_date, source_query)
    );
    CREATE INDEX IF NOT EXISTS idx_news_mail_runs_date
    ON news_mail_runs(run_date DESC);

    CREATE TABLE IF NOT EXISTS news_mail_messages (
        id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
        gmail_message_id TEXT NOT NULL,
        subject TEXT NULL,
        internal_date TIMESTAMPTZ NULL,
        snippet TEXT NULL,
        raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (run_id, gmail_message_id)
    );
    CREATE INDEX IF NOT EXISTS idx_news_mail_messages_run
    ON news_mail_messages(run_id);

    CREATE TABLE IF NOT EXISTS news_mail_articles (
        id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
        message_id UUID NULL REFERENCES news_mail_messages(id) ON DELETE SET NULL,
        section_index INTEGER NOT NULL DEFAULT 0 CHECK (section_index >= 0),
        section_title TEXT NULL,
        url TEXT NOT NULL,
        url_hash VARCHAR(64) NOT NULL,
        source_host TEXT NULL,
        category TEXT NOT NULL DEFAULT 'General',
        category_slug VARCHAR(64) NOT NULL DEFAULT 'general',
        fetch_status VARCHAR(32) NOT NULL DEFAULT 'pending',
        http_status INTEGER NULL,
        fetch_error TEXT NULL,
        title TEXT NULL,
        article_text TEXT NULL,
        article_excerpt TEXT NULL,
        codex_summary TEXT NULL,
        codex_analysis_status VARCHAR(32) NOT NULL DEFAULT 'pending',
        codex_analysis_started_at TIMESTAMPTZ NULL,
        codex_analysis_finished_at TIMESTAMPTZ NULL,
        codex_analysis_error TEXT NULL,
        key_points JSONB NOT NULL DEFAULT '[]'::jsonb,
        sector_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
        market_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
        data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
        raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (run_id, url_hash)
    );
    CREATE INDEX IF NOT EXISTS idx_news_mail_articles_run_status
    ON news_mail_articles(run_id, fetch_status);
    CREATE INDEX IF NOT EXISTS idx_news_mail_articles_updated
    ON news_mail_articles(updated_at DESC);
    ALTER TABLE news_mail_articles
    ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'General';
    ALTER TABLE news_mail_articles
    ADD COLUMN IF NOT EXISTS category_slug VARCHAR(64) NOT NULL DEFAULT 'general';
    ALTER TABLE news_mail_articles
    ADD COLUMN IF NOT EXISTS codex_analysis_status VARCHAR(32) NOT NULL DEFAULT 'pending';
    ALTER TABLE news_mail_articles
    ADD COLUMN IF NOT EXISTS codex_analysis_started_at TIMESTAMPTZ NULL;
    ALTER TABLE news_mail_articles
    ADD COLUMN IF NOT EXISTS codex_analysis_finished_at TIMESTAMPTZ NULL;
    ALTER TABLE news_mail_articles
    ADD COLUMN IF NOT EXISTS codex_analysis_error TEXT NULL;
    UPDATE news_mail_articles
    SET codex_analysis_status = 'completed',
        codex_analysis_finished_at = COALESCE(codex_analysis_finished_at, updated_at)
    WHERE codex_analysis_status <> 'completed'
      AND codex_summary IS NOT NULL
      AND BTRIM(codex_summary) <> '';
    CREATE INDEX IF NOT EXISTS idx_news_mail_articles_category_slug
    ON news_mail_articles(category_slug);
    CREATE INDEX IF NOT EXISTS idx_news_mail_articles_analysis_status
    ON news_mail_articles(run_id, codex_analysis_status, section_index, created_at);

    CREATE TABLE IF NOT EXISTS news_mail_symbol_impacts (
        id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
        article_id UUID NOT NULL REFERENCES news_mail_articles(id) ON DELETE CASCADE,
        symbol VARCHAR(20) NOT NULL,
        company_name TEXT NULL,
        known_symbol BOOLEAN NOT NULL DEFAULT FALSE,
        relevance_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (relevance_score >= 0 AND relevance_score <= 100),
        sentiment_label VARCHAR(16) NOT NULL DEFAULT 'neutral',
        sentiment_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (sentiment_score >= -100 AND sentiment_score <= 100),
        impact_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (impact_score >= 0 AND impact_score <= 100),
        impact_horizon VARCHAR(16) NULL,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 100),
        rationale TEXT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (article_id, symbol)
    );
    CREATE INDEX IF NOT EXISTS idx_news_mail_impacts_symbol_created
    ON news_mail_symbol_impacts(symbol, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_news_mail_impacts_run_impact
    ON news_mail_symbol_impacts(run_id, impact_score DESC);

    CREATE TABLE IF NOT EXISTS news_mail_return_research (
        id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES news_mail_runs(id) ON DELETE CASCADE,
        article_id UUID NOT NULL REFERENCES news_mail_articles(id) ON DELETE CASCADE,
        symbol_impact_id UUID NOT NULL REFERENCES news_mail_symbol_impacts(id) ON DELETE CASCADE,
        symbol VARCHAR(20) NOT NULL,
        event_date DATE NOT NULL,
        horizon_days INTEGER NOT NULL CHECK (horizon_days IN (1, 3, 5)),
        base_trading_date DATE NULL,
        base_close DOUBLE PRECISION NULL,
        future_trading_date DATE NULL,
        future_close DOUBLE PRECISION NULL,
        return_pct DOUBLE PRECISION NULL,
        status VARCHAR(32) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (symbol_impact_id, horizon_days)
    );
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_symbol VARCHAR(20) NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_base_trading_date DATE NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_base_close DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_future_trading_date DATE NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_future_close DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_return_pct DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS abnormal_return_pct DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS base_volume DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS future_volume DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS volume_change_pct DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS outcome_label VARCHAR(32) NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS outcome_score DOUBLE PRECISION NULL CHECK (
        outcome_score IS NULL OR (outcome_score >= 0 AND outcome_score <= 100)
    );
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
    CREATE INDEX IF NOT EXISTS idx_news_mail_return_research_symbol_event
    ON news_mail_return_research(symbol, event_date DESC);
    """
    with _news_mail_tables_lock:
        if _news_mail_tables_ready:
            return
        with connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s, %s)",
                    (_NEWS_MAIL_SCHEMA_LOCK_CLASS_ID, _NEWS_MAIL_SCHEMA_LOCK_OBJECT_ID),
                )
                cur.execute(ddl)
            conn.commit()
        _news_mail_tables_ready = True


def _fetch_existing_run(run_date: date, query: str) -> dict[str, Any] | None:
    ensure_news_mail_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, run_date, source_query, status, started_at, finished_at, error, metadata
                FROM news_mail_runs
                WHERE run_date = %(run_date)s AND source_query = %(source_query)s
                """,
                {"run_date": run_date, "source_query": query},
            )
            row = cur.fetchone()
    return dict(row) if row else None


def _create_or_reset_run(*, run_date: date, query: str, force_refresh: bool) -> dict[str, Any]:
    existing = _fetch_existing_run(run_date, query)
    if existing and not force_refresh:
        return existing

    run_id = existing.get("id") if existing else uuid4()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            if existing:
                cur.execute("DELETE FROM news_mail_articles WHERE run_id = %(run_id)s", {"run_id": run_id})
                cur.execute("DELETE FROM news_mail_messages WHERE run_id = %(run_id)s", {"run_id": run_id})
            cur.execute(
                """
                INSERT INTO news_mail_runs (id, run_date, source_query, status, started_at, finished_at, error, metadata)
                VALUES (%(id)s, %(run_date)s, %(source_query)s, 'preparing', NOW(), NULL, NULL, '{}'::jsonb)
                ON CONFLICT (run_date, source_query)
                DO UPDATE SET
                    status = 'preparing',
                    started_at = NOW(),
                    finished_at = NULL,
                    error = NULL,
                    metadata = '{}'::jsonb,
                    updated_at = NOW()
                """,
                {"id": run_id, "run_date": run_date, "source_query": query},
            )
        conn.commit()

    fresh = _fetch_existing_run(run_date, query)
    if fresh is None:
        raise RuntimeError("Failed to create news mail run.")
    return fresh


def _insert_message(run_id: UUID, row: dict[str, Any]) -> UUID:
    message_id = uuid4()
    with connect(settings.database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO news_mail_messages (
                    id, run_id, gmail_message_id, subject, internal_date, snippet, raw_metadata
                )
                VALUES (
                    %(id)s, %(run_id)s, %(gmail_message_id)s, %(subject)s,
                    %(internal_date)s, %(snippet)s, %(raw_metadata)s
                )
                ON CONFLICT (run_id, gmail_message_id)
                DO UPDATE SET
                    subject = EXCLUDED.subject,
                    internal_date = COALESCE(EXCLUDED.internal_date, news_mail_messages.internal_date),
                    snippet = EXCLUDED.snippet,
                    raw_metadata = EXCLUDED.raw_metadata
                RETURNING id
                """,
                {
                    "id": message_id,
                    "run_id": run_id,
                    "gmail_message_id": str(row.get("gmail_message_id") or ""),
                    "subject": str(row.get("subject") or ""),
                    "internal_date": row.get("internal_date"),
                    "snippet": _compact_text(str(row.get("text") or ""), 500),
                    "raw_metadata": _as_json({"thread_id": row.get("thread_id")}),
                },
            )
            saved = cur.fetchone() or {}
        conn.commit()
    return saved.get("id") or message_id


def _fetch_article(url: str) -> dict[str, Any]:
    try:
        attempts: list[dict[str, Any]] = []
        response: httpx.Response | None = None
        with httpx.Client(
            follow_redirects=True,
            timeout=float(settings.news_mail_article_fetch_timeout_seconds),
        ) as client:
            for attempt, headers in (
                ("browser_headers", _ARTICLE_FETCH_HEADERS),
                ("browser_headers_with_referer", _ARTICLE_FETCH_RETRY_HEADERS),
            ):
                response = client.get(url, headers=headers)
                attempts.append(_fetch_attempt_metadata(response, attempt))
                if response.status_code < 400 and not _blocked_fetch_provider(response):
                    break
                if not _blocked_fetch_provider(response):
                    break
        if response is None:
            raise RuntimeError("article_fetch_no_response")
        content_type = response.headers.get("content-type", "")
        blocked_by = _blocked_fetch_provider(response)
        if response.status_code >= 400:
            fetch_error = f"HTTP {response.status_code}"
            if blocked_by:
                fetch_error = f"{fetch_error} blocked_by_{blocked_by}"
            return {
                "fetch_status": "fetch_failed",
                "http_status": response.status_code,
                "fetch_error": fetch_error,
                "title": "",
                "article_text": "",
                "article_excerpt": "",
                "metadata": {
                    "content_type": content_type,
                    "final_url": str(response.url),
                    "fetch_attempts": attempts,
                    **({"blocked_by": blocked_by} if blocked_by else {}),
                },
            }
        cleaned = clean_article_html(response.text)
        text = cleaned.get("text", "")
        if blocked_by:
            return {
                "fetch_status": "fetch_failed",
                "http_status": response.status_code,
                "fetch_error": f"blocked_by_{blocked_by}",
                "title": cleaned.get("title", ""),
                "article_text": "",
                "article_excerpt": "",
                "metadata": {
                    "content_type": content_type,
                    "final_url": str(response.url),
                    "fetch_attempts": attempts,
                    "blocked_by": blocked_by,
                },
            }
        return {
            "fetch_status": "fetched" if text else "fetch_failed",
            "http_status": response.status_code,
            "fetch_error": "" if text else "empty_article_text",
            "title": cleaned.get("title", ""),
            "article_text": text,
            "article_excerpt": _compact_text(text, 700),
            "metadata": {
                "content_type": content_type,
                "final_url": str(response.url),
                "fetch_attempts": attempts,
            },
        }
    except Exception as exc:
        return {
            "fetch_status": "fetch_failed",
            "http_status": None,
            "fetch_error": str(exc)[:1000],
            "title": "",
            "article_text": "",
            "article_excerpt": "",
            "metadata": {},
        }


def _insert_article(run_id: UUID, message_id: UUID, link: dict[str, Any]) -> UUID:
    url = str(link.get("url") or "")
    article_id = uuid4()
    fetched = _fetch_article(url)
    fallback_title = str(link.get("section_title") or "")
    fetched_title = str(fetched.get("title") or fallback_title)
    fetched_excerpt = str(fetched.get("article_excerpt") or fallback_title)
    category, category_slug = _canonical_news_mail_category(_GENERAL_NEWS_CATEGORY)
    with connect(settings.database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO news_mail_articles (
                    id, run_id, message_id, section_index, section_title, url, url_hash,
                    source_host, category, category_slug, fetch_status, http_status,
                    fetch_error, title, article_text, article_excerpt, raw_metadata, updated_at
                )
                VALUES (
                    %(id)s, %(run_id)s, %(message_id)s, %(section_index)s, %(section_title)s,
                    %(url)s, %(url_hash)s, %(source_host)s, %(category)s, %(category_slug)s,
                    %(fetch_status)s, %(http_status)s, %(fetch_error)s, %(title)s,
                    %(article_text)s, %(article_excerpt)s, %(raw_metadata)s, NOW()
                )
                ON CONFLICT (run_id, url_hash)
                DO UPDATE SET
                    message_id = COALESCE(news_mail_articles.message_id, EXCLUDED.message_id),
                    section_index = LEAST(news_mail_articles.section_index, EXCLUDED.section_index),
                    section_title = COALESCE(NULLIF(news_mail_articles.section_title, ''), EXCLUDED.section_title),
                    fetch_status = EXCLUDED.fetch_status,
                    http_status = EXCLUDED.http_status,
                    fetch_error = EXCLUDED.fetch_error,
                    title = COALESCE(NULLIF(EXCLUDED.title, ''), news_mail_articles.title),
                    article_text = COALESCE(NULLIF(EXCLUDED.article_text, ''), news_mail_articles.article_text),
                    article_excerpt = COALESCE(NULLIF(EXCLUDED.article_excerpt, ''), news_mail_articles.article_excerpt),
                    raw_metadata = EXCLUDED.raw_metadata,
                    updated_at = NOW()
                RETURNING id
                """,
                {
                    "id": article_id,
                    "run_id": run_id,
                    "message_id": message_id,
                    "section_index": int(link.get("section_index") or 0),
                    "section_title": str(link.get("section_title") or ""),
                    "url": url,
                    "url_hash": _sha256_text(url),
                    "source_host": _host_from_url(url),
                    "category": category,
                    "category_slug": category_slug,
                    "fetch_status": fetched.get("fetch_status"),
                    "http_status": fetched.get("http_status"),
                    "fetch_error": fetched.get("fetch_error") or None,
                    "title": fetched_title,
                    "article_text": fetched.get("article_text") or "",
                    "article_excerpt": fetched_excerpt,
                    "raw_metadata": _as_json(fetched.get("metadata") or {}),
                },
            )
            saved = cur.fetchone() or {}
        conn.commit()
    return saved.get("id") or article_id


def _insert_article_if_new(run_id: UUID, message_id: UUID, link: dict[str, Any]) -> dict[str, Any]:
    url = str(link.get("url") or "")
    if not url:
        return {"inserted": False, "article_id": None, "skip_reason": "empty_url"}
    url_hash = _sha256_text(url)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, run_id
                FROM news_mail_articles
                WHERE url_hash = %(url_hash)s
                LIMIT 1
                """,
                {"url_hash": url_hash},
            )
            existing = cur.fetchone()
    if existing:
        return {
            "inserted": False,
            "article_id": existing.get("id"),
            "skip_reason": "duplicate_url",
        }
    article_id = _insert_article(run_id, message_id, link)
    return {"inserted": True, "article_id": article_id, "skip_reason": None}


def _mark_run(run_id: UUID | str, status: str, *, error: str | None = None, metadata: Any | None = None) -> None:
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE news_mail_runs
                SET status = %(status)s::VARCHAR,
                    finished_at = CASE
                        WHEN %(status)s::TEXT IN ('prepared', 'completed', 'failed') THEN NOW()
                        ELSE finished_at
                    END,
                    error = %(error)s,
                    metadata = COALESCE(%(metadata)s::JSONB, metadata),
                    updated_at = NOW()
                WHERE id = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "status": status,
                    "error": error,
                    "metadata": _as_json(metadata) if metadata is not None else None,
                },
            )
        conn.commit()


def _run_summary(run_id: UUID | str) -> dict[str, Any]:
    ensure_news_mail_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, run_date, source_query, status, started_at, finished_at, error, metadata
                FROM news_mail_runs
                WHERE id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            run = cur.fetchone()
            if not run:
                raise RuntimeError(f"News mail run not found: {run_id}")
            cur.execute(
                """
                SELECT
                    COUNT(*)::INT AS article_count,
                    COUNT(*) FILTER (WHERE fetch_status = 'fetched')::INT AS fetched_count,
                    COUNT(*) FILTER (WHERE fetch_status <> 'fetched')::INT AS failed_count
                FROM news_mail_articles
                WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            counts = cur.fetchone() or {}
    return {**_serialize_row(dict(run)), **dict(counts)}


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            out[key] = value.isoformat()
        elif isinstance(value, UUID):
            out[key] = str(value)
        else:
            out[key] = value
    return out


def prepare_news_mail_run(
    *,
    query: str | None = None,
    max_results: int | None = None,
    force_refresh: bool = False,
    article_fetch_limit: int | None = None,
) -> dict[str, Any]:
    ensure_news_mail_tables()
    source_query = (query or settings.news_mail_gmail_query or "Tin tức chứng khoán").strip()
    run_date = _local_today()
    max_mail = max(1, min(_safe_int(max_results, int(settings.news_mail_gmail_max_results)), 100))
    max_articles = max(1, min(_safe_int(article_fetch_limit, int(settings.news_mail_article_fetch_limit)), 200))

    run = _create_or_reset_run(run_date=run_date, query=source_query, force_refresh=force_refresh)
    run_id = run["id"]
    if not force_refresh:
        existing = _articles_for_codex(run_id, include_failed=True)
        if existing:
            if str(run.get("status") or "") in {"preparing", "failed"}:
                _mark_run(
                    run_id,
                    "prepared",
                    metadata={
                        "reused_existing": True,
                        "article_count": len(existing),
                    },
                )
            return {
                "success": True,
                "run": _run_summary(run_id),
                "articles": existing,
                "reused_existing": True,
            }

    try:
        messages = _gmail.fetch_today_message_documents(query=source_query, max_results=max_mail)
        article_ids: list[str] = []
        for message in messages:
            message_id = _insert_message(run_id, message)
            links = extract_section_links(
                html_text=str(message.get("html") or ""),
                text=str(message.get("text") or ""),
            )
            for link in links:
                if len(article_ids) >= max_articles:
                    break
                article_id = _insert_article(run_id, message_id, link)
                article_ids.append(str(article_id))
            if len(article_ids) >= max_articles:
                break
        _mark_run(
            run_id,
            "prepared",
            metadata={
                "message_count": len(messages),
                "article_count": len(set(article_ids)),
                "article_fetch_limit": max_articles,
            },
        )
        return {
            "success": True,
            "run": _run_summary(run_id),
            "articles": _articles_for_codex(run_id, include_failed=True),
            "reused_existing": False,
        }
    except Exception as exc:
        _mark_run(run_id, "failed", error=str(exc))
        raise


def refresh_news_mail_from_gmail(
    *,
    query: str | None = None,
    max_results: int | None = None,
    article_fetch_limit: int | None = None,
) -> dict[str, Any]:
    ensure_news_mail_tables()
    source_query = (query or settings.news_mail_gmail_query or "Tin tuc chung khoan").strip()
    run_date = _local_today()
    max_mail = max(1, min(_safe_int(max_results, int(settings.news_mail_gmail_max_results)), 100))
    max_new_articles = max(1, min(_safe_int(article_fetch_limit, int(settings.news_mail_article_fetch_limit)), 200))

    run = _create_or_reset_run(run_date=run_date, query=source_query, force_refresh=False)
    run_id = run["id"]
    inserted_article_ids: list[str] = []
    skipped_existing = 0
    skipped_empty = 0
    links_scanned = 0
    seen_urls: set[str] = set()

    try:
        messages = _gmail.fetch_today_message_documents(query=source_query, max_results=max_mail)
        for message in messages:
            message_id = _insert_message(run_id, message)
            links = extract_section_links(
                html_text=str(message.get("html") or ""),
                text=str(message.get("text") or ""),
            )
            for link in links:
                url = str(link.get("url") or "").strip()
                if not url:
                    skipped_empty += 1
                    continue
                if url in seen_urls:
                    skipped_existing += 1
                    continue
                seen_urls.add(url)
                links_scanned += 1
                result = _insert_article_if_new(run_id, message_id, link)
                if result.get("inserted"):
                    inserted_article_ids.append(str(result.get("article_id") or ""))
                    if len(inserted_article_ids) >= max_new_articles:
                        break
                elif result.get("skip_reason") == "duplicate_url":
                    skipped_existing += 1
                else:
                    skipped_empty += 1
            if len(inserted_article_ids) >= max_new_articles:
                break
        _mark_run(
            run_id,
            "prepared",
            metadata={
                "message_count": len(messages),
                "new_article_count": len(inserted_article_ids),
                "duplicate_skipped_count": skipped_existing,
                "empty_skipped_count": skipped_empty,
                "links_scanned": links_scanned,
                "article_fetch_limit": max_new_articles,
                "refresh_mode": "skip_existing",
            },
        )
        return {
            "success": True,
            "run": _run_summary(run_id),
            "articles": _articles_for_codex(run_id, include_failed=True),
            "new_article_count": len(inserted_article_ids),
            "duplicate_skipped_count": skipped_existing,
            "empty_skipped_count": skipped_empty,
            "links_scanned": links_scanned,
        }
    except Exception as exc:
        _mark_run(run_id, "failed", error=str(exc))
        raise


def _articles_for_codex(
    run_id: UUID | str,
    *,
    include_failed: bool,
    only_missing_analysis: bool = False,
) -> list[dict[str, Any]]:
    where_parts = ["run_id = %(run_id)s"]
    if not include_failed:
        where_parts.append("fetch_status = 'fetched'")
    if only_missing_analysis:
        where_parts.append(
            "("
            "codex_analysis_status IS DISTINCT FROM 'completed' "
            "AND (codex_summary IS NULL OR BTRIM(codex_summary) = '' OR codex_analysis_status IN ('pending', 'failed'))"
            ")"
        )
    where = " AND ".join(where_parts)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT id, section_index, section_title, url, source_host, fetch_status,
                       fetch_error, title, article_text, article_excerpt,
                       codex_analysis_status, codex_analysis_started_at,
                       codex_analysis_finished_at, codex_analysis_error
                FROM news_mail_articles
                WHERE {where}
                ORDER BY section_index ASC, created_at ASC
                """,
                {"run_id": run_id},
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = _serialize_row(dict(row))
        item["article_id"] = item.pop("id")
        out.append(item)
    return out


def build_codex_prompt(articles: list[dict[str, Any]]) -> str:
    payload = [
        {
            "article_id": str(row.get("article_id") or row.get("id") or ""),
            "section_title": row.get("section_title") or "",
            "title": row.get("title") or "",
            "url": row.get("url") or "",
            "source_host": row.get("source_host") or "",
            "fetch_status": row.get("fetch_status") or "",
            "fetch_error": row.get("fetch_error") or "",
            "article_text": _compact_text(str(row.get("article_text") or row.get("article_excerpt") or ""), 6000),
        }
        for row in articles
    ]
    return (
        "You are analyzing Vietnamese stock-market news for a trading dashboard.\n"
        "Return strict JSON only. Do not include markdown.\n"
        "Return one output item for every input article_id, in the same order. "
        "Write all user-facing fields in Vietnamese: summary, key_points, sector_tags, "
        "market_tags, data_gaps, and symbols[].rationale. Translate non-Vietnamese source "
        "content into Vietnamese; do not translate ticker symbols or company names. "
        "For each article, summarize the article, detect all relevant stock symbols, "
        "and score sentiment plus stock-price impact.\n"
        "Use uppercase symbols. If no symbol is relevant, return an empty symbols array.\n"
        "sentiment_label must be positive, negative, neutral, or mixed. "
        "sentiment_score is -100..100. impact_score is 0..100. confidence is 0..100.\n"
        "Output schema root: {\"articles\": [...]}.\n"
        "Input articles JSON:\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )


def _extract_json_object(raw_text: str) -> dict[str, Any] | None:
    text = str(raw_text or "").strip()
    if not text:
        return None
    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    candidates = [fenced.group(1)] if fenced else []
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _string_list(value: Any, *, max_items: int = 12, max_chars: int = 300) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value[:max_items]:
        text = str(item or "").strip()
        if text:
            out.append(text[:max_chars])
    return out


def _fallback_analysis_article(article_id: str, reason: str) -> dict[str, Any]:
    return {
        "article_id": article_id,
        "summary": "Chua co ket qua phan tich tu AI cho bai viet nay.",
        "key_points": [],
        "sector_tags": [],
        "market_tags": [],
        "data_gaps": [reason],
        "symbols": [],
    }


def _parse_news_mail_analysis_output(raw_text: str, expected_article_ids: list[str]) -> list[dict[str, Any]]:
    payload = _extract_json_object(raw_text)
    if not payload:
        raise RuntimeError("news_mail_analysis_json_parse_failed")
    raw_articles = payload.get("articles")
    if not isinstance(raw_articles, list):
        raise RuntimeError("news_mail_analysis_articles_missing")

    expected = [str(article_id) for article_id in expected_article_ids if str(article_id)]
    expected_set = set(expected)
    parsed_by_id: dict[str, dict[str, Any]] = {}
    for row in raw_articles:
        if not isinstance(row, dict):
            continue
        article_id = str(row.get("article_id") or "").strip()
        if not article_id or article_id not in expected_set:
            continue
        symbols: list[dict[str, Any]] = []
        raw_symbols = row.get("symbols")
        if isinstance(raw_symbols, list):
            for symbol_row in raw_symbols:
                if not isinstance(symbol_row, dict):
                    continue
                symbol = str(symbol_row.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                symbols.append(
                    {
                        "symbol": symbol[:20],
                        "company_name": str(symbol_row.get("company_name") or "").strip()[:500] or None,
                        "relevance_score": _safe_float(symbol_row.get("relevance_score"), 0) or 0,
                        "sentiment_label": _normalize_sentiment_label(symbol_row.get("sentiment_label")),
                        "sentiment_score": _safe_float(symbol_row.get("sentiment_score"), 0) or 0,
                        "impact_score": _safe_float(symbol_row.get("impact_score"), 0) or 0,
                        "impact_horizon": str(symbol_row.get("impact_horizon") or "").strip()[:16] or None,
                        "confidence": _safe_float(symbol_row.get("confidence"), 0) or 0,
                        "rationale": str(symbol_row.get("rationale") or "").strip()[:4000] or None,
                    }
                )
        parsed_by_id[article_id] = {
            "article_id": article_id,
            "summary": str(row.get("summary") or "").strip()[:4000],
            "key_points": _string_list(row.get("key_points")),
            "sector_tags": _string_list(row.get("sector_tags")),
            "market_tags": _string_list(row.get("market_tags")),
            "data_gaps": _string_list(row.get("data_gaps")),
            "symbols": symbols,
        }

    return [
        parsed_by_id.get(article_id)
        or _fallback_analysis_article(article_id, "AI khong tra ve ket qua cho article_id nay.")
        for article_id in expected
    ]


def _run_codex_cli_json(prompt: str, *, batch_number: int) -> str:
    with tempfile.TemporaryDirectory(prefix="news-mail-codex-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        schema_path = tmp_path / "codex-news-mail-output.schema.json"
        output_path = tmp_path / "codex-output.json"
        schema_path.write_text(json.dumps(_CODEX_NEWS_MAIL_OUTPUT_SCHEMA), encoding="utf-8")
        cmd = [
            "codex",
            "exec",
            "--cd",
            "/app",
            "--skip-git-repo-check",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(output_path),
            "--json",
            "-",
        ]
        try:
            completed = subprocess.run(
                cmd,
                input=prompt,
                text=True,
                capture_output=True,
                timeout=_NEWS_MAIL_CODEX_TIMEOUT_SECONDS,
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("codex_cli_not_found_in_backend_container") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"codex_cli_timeout_batch_{batch_number}") from exc
        if completed.returncode != 0:
            stderr = _compact_text(completed.stderr or completed.stdout or "", 1200)
            raise RuntimeError(f"codex_cli_failed_batch_{batch_number}: {stderr}")
        if not output_path.exists():
            stderr = _compact_text(completed.stderr or completed.stdout or "", 1200)
            raise RuntimeError(f"codex_cli_missing_output_batch_{batch_number}: {stderr}")
        return output_path.read_text(encoding="utf-8")


def _article_ids_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for row in rows:
        article_id = str(row.get("article_id") or row.get("id") or "").strip()
        if article_id:
            out.append(article_id)
    return out


def _article_analysis_is_done(row: dict[str, Any]) -> bool:
    status = str(row.get("codex_analysis_status") or "").strip().lower()
    summary = str(row.get("codex_summary") or "").strip()
    return status == "completed" or bool(summary)


def _news_mail_article_log_label(row: dict[str, Any]) -> str:
    article_id = str(row.get("article_id") or row.get("id") or "").strip() or "unknown"
    section_index = row.get("section_index")
    if section_index is None or section_index == "":
        section_index = "unknown"
    source_host = str(row.get("source_host") or "").strip()
    if not source_host:
        source_host = _host_from_url(str(row.get("url") or ""))
    if not source_host:
        source_host = "unknown"
    title = _compact_text(str(row.get("title") or row.get("section_title") or row.get("url") or ""), 180)
    if not title:
        title = "untitled"
    return "article_id=%s section_index=%s source_host=%s title=%r" % (
        article_id,
        section_index,
        source_host,
        title,
    )


def _log_news_mail_analysis_article(
    event: str,
    *,
    run_id: UUID | str,
    source: str,
    batch_number: int,
    article_position: int,
    total_articles: int,
    row: dict[str, Any],
    symbols_count: int | None = None,
    error: str | None = None,
) -> None:
    details = ""
    if symbols_count is not None:
        details += f" symbols_count={symbols_count}"
    if error:
        details += f" error={_compact_text(error, 300)!r}"
    logger.info(
        "%s run_id=%s source=%s batch=%s article_position=%s/%s %s%s",
        event,
        run_id,
        source,
        batch_number,
        article_position,
        total_articles,
        _news_mail_article_log_label(row),
        details,
    )


def _mark_articles_analysis_status(
    *,
    run_id: UUID | str,
    article_ids: list[str],
    status: str,
    error: str | None = None,
) -> int:
    normalized = [str(article_id).strip() for article_id in article_ids if str(article_id).strip()]
    if not normalized:
        return 0
    safe_status = str(status or "").strip().lower()
    if safe_status not in {"pending", "codex_running", "completed", "failed"}:
        safe_status = "pending"
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE news_mail_articles
                SET codex_analysis_status = %(status)s,
                    codex_analysis_started_at = CASE
                        WHEN %(status)s = 'codex_running' THEN NOW()
                        ELSE codex_analysis_started_at
                    END,
                    codex_analysis_finished_at = CASE
                        WHEN %(status)s IN ('completed', 'failed') THEN NOW()
                        ELSE codex_analysis_finished_at
                    END,
                    codex_analysis_error = CASE
                        WHEN %(status)s = 'failed' THEN %(error)s
                        WHEN %(status)s = 'completed' THEN NULL
                        ELSE codex_analysis_error
                    END,
                    updated_at = NOW()
                WHERE run_id = %(run_id)s
                  AND id = ANY(%(article_ids)s::uuid[])
                  AND (
                    %(status)s <> 'codex_running'
                    OR codex_analysis_status IS DISTINCT FROM 'completed'
                  )
                """,
                {
                    "run_id": run_id,
                    "article_ids": normalized,
                    "status": safe_status,
                    "error": str(error or "")[:4000] or None,
                },
            )
            count = int(cur.rowcount or 0)
        conn.commit()
    return count


def _run_news_mail_analysis_workflow(
    *,
    run_id: UUID | str,
    articles: list[dict[str, Any]],
    source: str,
) -> dict[str, Any]:
    if not articles:
        logger.info(
            "news_mail_analysis_workflow_skipped run_id=%s source=%s reason=no_missing_articles",
            run_id,
            source,
        )
        research = refresh_sentiment_return_research(run_id=run_id, days_back=365)
        metadata = {
            "analysis_source": source,
            "analysis_status": "skipped_no_missing_articles",
            "analysis_article_count": 0,
            "research": research,
        }
        _mark_run(run_id, "completed", metadata=metadata)
        return metadata

    batch_size = max(1, min(_NEWS_MAIL_ANALYSIS_BATCH_SIZE, 10))
    workflow_started_at = time.monotonic()
    batch_count = 0
    updated_articles = 0
    written_impacts = 0
    logger.info(
        "news_mail_analysis_workflow_started run_id=%s source=%s article_count=%s batch_size=%s",
        run_id,
        source,
        len(articles),
        batch_size,
    )
    for start in range(0, len(articles), batch_size):
        chunk = articles[start : start + batch_size]
        batch_with_positions = [
            (start + offset, row)
            for offset, row in enumerate(chunk, start=1)
            if not _article_analysis_is_done(row)
        ]
        batch = [row for _, row in batch_with_positions]
        if not batch:
            logger.info(
                "news_mail_analysis_batch_skipped run_id=%s source=%s batch_start=%s reason=already_completed",
                run_id,
                source,
                start,
            )
            continue
        batch_count += 1
        final_batch = start + batch_size >= len(articles)
        expected_ids = _article_ids_from_rows(batch)
        article_by_id = {
            str(row.get("article_id") or row.get("id") or "").strip(): row
            for row in batch
            if str(row.get("article_id") or row.get("id") or "").strip()
        }
        article_position_by_id = {
            str(row.get("article_id") or row.get("id") or "").strip(): position
            for position, row in batch_with_positions
            if str(row.get("article_id") or row.get("id") or "").strip()
        }
        claimed = _mark_articles_analysis_status(
            run_id=run_id,
            article_ids=expected_ids,
            status="codex_running",
        )
        if claimed <= 0:
            logger.info(
                "news_mail_analysis_batch_skipped run_id=%s source=%s batch=%s reason=no_articles_claimed article_count=%s",
                run_id,
                source,
                batch_count,
                len(batch),
            )
            continue
        batch_started_at = time.monotonic()
        logger.info(
            "news_mail_analysis_batch_started run_id=%s source=%s batch=%s article_count=%s claimed_articles=%s final_batch=%s",
            run_id,
            source,
            batch_count,
            len(batch),
            claimed,
            final_batch,
        )
        for article in batch:
            article_id = str(article.get("article_id") or article.get("id") or "").strip()
            _log_news_mail_analysis_article(
                "news_mail_analysis_article_started",
                run_id=run_id,
                source=source,
                batch_number=batch_count,
                article_position=article_position_by_id.get(article_id, start + 1),
                total_articles=len(articles),
                row=article,
            )
        try:
            logger.info(
                "news_mail_analysis_codex_started run_id=%s source=%s batch=%s article_count=%s timeout_seconds=%s",
                run_id,
                source,
                batch_count,
                len(batch),
                _NEWS_MAIL_CODEX_TIMEOUT_SECONDS,
            )
            raw = _run_codex_cli_json(build_codex_prompt(batch), batch_number=batch_count)
            logger.info(
                "news_mail_analysis_codex_finished run_id=%s source=%s batch=%s elapsed_seconds=%.1f",
                run_id,
                source,
                batch_count,
                time.monotonic() - batch_started_at,
            )
            parsed_articles = _parse_news_mail_analysis_output(raw, expected_ids)
            result = apply_codex_results(
                run_id=run_id,
                articles=parsed_articles,
                final_batch=final_batch,
                run_metadata={
                    "analysis_source": source,
                    "analysis_status": "completed" if final_batch else "running",
                    "batch_number": batch_count,
                    "batch_size": len(batch),
                    "analysis_article_count": len(articles),
                    "completed_at": _utc_now().isoformat() if final_batch else None,
                },
            )
        except Exception as exc:
            error_text = str(exc)
            _mark_articles_analysis_status(
                run_id=run_id,
                article_ids=expected_ids,
                status="failed",
                error=error_text,
            )
            for article in batch:
                article_id = str(article.get("article_id") or article.get("id") or "").strip()
                _log_news_mail_analysis_article(
                    "news_mail_analysis_article_failed",
                    run_id=run_id,
                    source=source,
                    batch_number=batch_count,
                    article_position=article_position_by_id.get(article_id, start + 1),
                    total_articles=len(articles),
                    row=article,
                    error=error_text,
                )
            logger.exception(
                "news_mail_analysis_batch_failed run_id=%s source=%s batch=%s article_count=%s elapsed_seconds=%.1f",
                run_id,
                source,
                batch_count,
                len(batch),
                time.monotonic() - batch_started_at,
            )
            raise
        for parsed_article in parsed_articles:
            article_id = str(parsed_article.get("article_id") or "").strip()
            article = article_by_id.get(article_id, parsed_article)
            symbols = parsed_article.get("symbols") or []
            _log_news_mail_analysis_article(
                "news_mail_analysis_article_completed",
                run_id=run_id,
                source=source,
                batch_number=batch_count,
                article_position=article_position_by_id.get(article_id, start + 1),
                total_articles=len(articles),
                row=article,
                symbols_count=len(symbols) if isinstance(symbols, list) else 0,
            )
        updated_articles += int(result.get("updated_articles") or 0)
        written_impacts += int(result.get("written_impacts") or 0)
        logger.info(
            "news_mail_analysis_batch_completed run_id=%s source=%s batch=%s updated_articles=%s written_impacts=%s elapsed_seconds=%.1f",
            run_id,
            source,
            batch_count,
            result.get("updated_articles") or 0,
            result.get("written_impacts") or 0,
            time.monotonic() - batch_started_at,
        )

    metadata = {
        "analysis_source": source,
        "analysis_status": "completed",
        "analysis_article_count": len(articles),
        "analysis_batch_count": batch_count,
        "updated_articles": updated_articles,
        "written_impacts": written_impacts,
    }
    logger.info(
        "news_mail_analysis_workflow_completed run_id=%s source=%s article_count=%s batch_count=%s updated_articles=%s written_impacts=%s elapsed_seconds=%.1f",
        run_id,
        source,
        len(articles),
        batch_count,
        updated_articles,
        written_impacts,
        time.monotonic() - workflow_started_at,
    )
    return metadata


def refresh_news_mail_workflow_from_gmail(
    *,
    query: str | None = None,
    max_results: int | None = None,
    article_fetch_limit: int | None = None,
) -> dict[str, Any]:
    logger.info(
        "news_mail_refresh_workflow_started max_results=%s article_fetch_limit=%s",
        max_results,
        article_fetch_limit,
    )
    refresh = refresh_news_mail_from_gmail(
        query=query,
        max_results=max_results,
        article_fetch_limit=article_fetch_limit,
    )
    run_id = str((refresh.get("run") or {}).get("id") or "").strip()
    if not run_id:
        raise RuntimeError("news_mail_refresh_missing_run_id")

    articles_for_analysis = _articles_for_codex(run_id, include_failed=True, only_missing_analysis=True)
    logger.info(
        "news_mail_refresh_workflow_scan_completed run_id=%s new_article_count=%s duplicate_skipped_count=%s empty_skipped_count=%s links_scanned=%s missing_analysis_count=%s",
        run_id,
        refresh.get("new_article_count") or 0,
        refresh.get("duplicate_skipped_count") or 0,
        refresh.get("empty_skipped_count") or 0,
        refresh.get("links_scanned") or 0,
        len(articles_for_analysis),
    )
    try:
        workflow = _run_news_mail_analysis_workflow(
            run_id=run_id,
            articles=articles_for_analysis,
            source="backend_refresh_codex_cli",
        )
    except Exception as exc:
        logger.exception(
            "news_mail_refresh_workflow_failed run_id=%s missing_analysis_count=%s",
            run_id,
            len(articles_for_analysis),
        )
        _mark_run(
            run_id,
            "failed",
            error=str(exc),
            metadata={
                "analysis_source": "backend_refresh_codex_cli",
                "analysis_status": "failed",
                "analysis_article_count": len(articles_for_analysis),
            },
        )
        raise

    return {
        **refresh,
        "run": _run_summary(run_id),
        "articles": _articles_for_codex(run_id, include_failed=True),
        "workflow": workflow,
    }


def _known_symbols(symbols: list[str]) -> set[str]:
    normalized = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not normalized:
        return set()
    try:
        ensure_market_symbol_tables()
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT symbol
                    FROM market_symbols
                    WHERE symbol = ANY(%(symbols)s)
                    """,
                    {"symbols": normalized},
                )
                rows = cur.fetchall()
        return {str(row.get("symbol") or "").upper() for row in rows}
    except Exception:
        logger.debug("news_mail_known_symbol_lookup_failed", exc_info=True)
        return set()


def apply_codex_results(
    *,
    run_id: UUID | str,
    articles: list[dict[str, Any]],
    final_batch: bool,
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_news_mail_tables()
    article_symbols: list[str] = []
    for article in articles:
        for symbol_row in article.get("symbols") or []:
            if isinstance(symbol_row, dict):
                symbol = str(symbol_row.get("symbol") or "").strip().upper()
                if symbol:
                    article_symbols.append(symbol)
    known = _known_symbols(article_symbols)
    symbol_categories = _news_mail_symbol_category_lookup(article_symbols, known)

    updated_articles = 0
    written_impacts = 0
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            for article in articles:
                article_id = str(article.get("article_id") or "").strip()
                if not article_id:
                    continue
                category, category_slug = _news_mail_article_category_from_symbols(
                    article.get("symbols") or [],
                    symbol_categories,
                )
                cur.execute(
                    """
                    UPDATE news_mail_articles
                    SET codex_summary = %(summary)s,
                        category = %(category)s,
                        category_slug = %(category_slug)s,
                        key_points = %(key_points)s,
                        sector_tags = %(sector_tags)s,
                        market_tags = %(market_tags)s,
                        data_gaps = %(data_gaps)s,
                        codex_analysis_status = 'completed',
                        codex_analysis_finished_at = NOW(),
                        codex_analysis_error = NULL,
                        updated_at = NOW()
                    WHERE id = %(article_id)s AND run_id = %(run_id)s
                    """,
                    {
                        "article_id": article_id,
                        "run_id": run_id,
                        "summary": str(article.get("summary") or "")[:4000],
                        "category": category,
                        "category_slug": category_slug,
                        "key_points": _as_json(article.get("key_points") or []),
                        "sector_tags": _as_json(article.get("sector_tags") or []),
                        "market_tags": _as_json(article.get("market_tags") or []),
                        "data_gaps": _as_json(article.get("data_gaps") or []),
                    },
                )
                if cur.rowcount:
                    updated_articles += 1
                cur.execute(
                    "DELETE FROM news_mail_symbol_impacts WHERE article_id = %(article_id)s",
                    {"article_id": article_id},
                )
                for symbol_row in article.get("symbols") or []:
                    if not isinstance(symbol_row, dict):
                        continue
                    symbol = str(symbol_row.get("symbol") or "").strip().upper()
                    if not symbol:
                        continue
                    cur.execute(
                        """
                        INSERT INTO news_mail_symbol_impacts (
                            id, run_id, article_id, symbol, company_name, known_symbol,
                            relevance_score, sentiment_label, sentiment_score,
                            impact_score, impact_horizon, confidence, rationale, metadata
                        )
                        VALUES (
                            %(id)s, %(run_id)s, %(article_id)s, %(symbol)s, %(company_name)s,
                            %(known_symbol)s, %(relevance_score)s, %(sentiment_label)s,
                            %(sentiment_score)s, %(impact_score)s, %(impact_horizon)s,
                            %(confidence)s, %(rationale)s, %(metadata)s
                        )
                        ON CONFLICT (article_id, symbol)
                        DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            known_symbol = EXCLUDED.known_symbol,
                            relevance_score = EXCLUDED.relevance_score,
                            sentiment_label = EXCLUDED.sentiment_label,
                            sentiment_score = EXCLUDED.sentiment_score,
                            impact_score = EXCLUDED.impact_score,
                            impact_horizon = EXCLUDED.impact_horizon,
                            confidence = EXCLUDED.confidence,
                            rationale = EXCLUDED.rationale,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW()
                        """,
                        {
                            "id": uuid4(),
                            "run_id": run_id,
                            "article_id": article_id,
                            "symbol": symbol[:20],
                            "company_name": str(symbol_row.get("company_name") or "")[:500] or None,
                            "known_symbol": symbol in known,
                            "relevance_score": max(0.0, min(100.0, _safe_float(symbol_row.get("relevance_score"), 0) or 0)),
                            "sentiment_label": _normalize_sentiment_label(symbol_row.get("sentiment_label")),
                            "sentiment_score": max(-100.0, min(100.0, _safe_float(symbol_row.get("sentiment_score"), 0) or 0)),
                            "impact_score": max(0.0, min(100.0, _safe_float(symbol_row.get("impact_score"), 0) or 0)),
                            "impact_horizon": str(symbol_row.get("impact_horizon") or "")[:16] or None,
                            "confidence": max(0.0, min(100.0, _safe_float(symbol_row.get("confidence"), 0) or 0)),
                            "rationale": str(symbol_row.get("rationale") or "")[:4000] or None,
                            "metadata": _as_json(symbol_row),
                        },
                    )
                    written_impacts += 1
            if final_batch:
                cur.execute(
                    """
                    UPDATE news_mail_runs
                    SET status = 'completed',
                        finished_at = NOW(),
                        metadata = %(metadata)s,
                        updated_at = NOW()
                    WHERE id = %(run_id)s
                    """,
                    {"run_id": run_id, "metadata": _as_json(run_metadata or {})},
                )
            else:
                cur.execute(
                    """
                    UPDATE news_mail_runs
                    SET status = 'codex_running', updated_at = NOW()
                    WHERE id = %(run_id)s
                    """,
                    {"run_id": run_id},
                )
        conn.commit()

    if final_batch:
        refresh_sentiment_return_research(run_id=run_id, days_back=365)
    return {
        "success": True,
        "run": _run_summary(run_id),
        "updated_articles": updated_articles,
        "written_impacts": written_impacts,
    }


def _normalize_sentiment_label(value: Any) -> str:
    label = str(value or "neutral").strip().lower()
    if label in {"positive", "negative", "neutral", "mixed"}:
        return label
    if label in {"bullish", "good", "pos"}:
        return "positive"
    if label in {"bearish", "bad", "neg"}:
        return "negative"
    return "neutral"


def fail_news_mail_run(run_id: UUID | str, error: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    _mark_run(run_id, "failed", error=error[:4000], metadata=metadata or {})
    return {"success": True, "run": _run_summary(run_id)}


def get_today_news_mail(limit: int = 100) -> dict[str, Any]:
    return get_news_mail_for_date(_local_today(), limit=limit)


def get_news_mail_for_date(target_date: date, limit: int = 100) -> dict[str, Any]:
    ensure_news_mail_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id,
                    r.run_date,
                    r.source_query,
                    r.status,
                    r.started_at,
                    r.finished_at,
                    r.error,
                    r.metadata,
                    COUNT(a.id)::INT AS article_count
                FROM news_mail_runs r
                LEFT JOIN news_mail_articles a ON a.run_id = r.id
                WHERE r.run_date = %(run_date)s
                GROUP BY r.id
                ORDER BY (COUNT(a.id) > 0) DESC, r.updated_at DESC
                LIMIT 1
                """,
                {"run_date": target_date},
            )
            run = cur.fetchone()
            if not run:
                return {"run": None, "articles": [], "count": 0}
            articles = _dashboard_articles(cur, run["id"], limit=limit)
    return {"run": _serialize_row(dict(run)), "articles": articles, "count": len(articles)}


def _normalize_news_mail_category_filter(category: str | None) -> str:
    return _ascii_slug(category or "")


def _normalize_news_mail_symbol_filter(symbol: str | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(symbol or "").strip().upper())[:20]


def list_news_mail_articles(
    *,
    limit: int = 20,
    offset: int = 0,
    category: str | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    ensure_news_mail_tables()
    safe_limit = max(1, min(int(limit), 100))
    safe_offset = max(0, min(int(offset), 10000))
    category_filter = _normalize_news_mail_category_filter(category)
    symbol_filter = _normalize_news_mail_symbol_filter(symbol)
    params: dict[str, Any] = {
        "limit": safe_limit,
        "offset": safe_offset,
    }
    where_parts: list[str] = []

    if category_filter in {"positive", "negative", "neutral", "mixed"}:
        params["category"] = category_filter
        where_parts.append(
            """
            EXISTS (
                SELECT 1
                FROM news_mail_symbol_impacts ci
                WHERE ci.article_id = a.id AND ci.sentiment_label = %(category)s
            )
            """
        )
    elif category_filter == "with_symbols":
        where_parts.append(
            """
            EXISTS (
                SELECT 1
                FROM news_mail_symbol_impacts ci
                WHERE ci.article_id = a.id
            )
            """
        )
    elif category_filter == "fetch_failed":
        where_parts.append("a.fetch_status <> 'fetched'")
    elif category_filter and category_filter != "all":
        params["category"] = category_filter
        where_parts.append(
            """
            a.category_slug = %(category)s
            """
        )

    if symbol_filter:
        params["symbol"] = symbol_filter
        where_parts.append(
            """
            EXISTS (
                SELECT 1
                FROM news_mail_symbol_impacts si
                WHERE si.article_id = a.id AND si.symbol = %(symbol)s
            )
            """
        )

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)::INT AS total_count
                FROM news_mail_articles a
                JOIN news_mail_runs r ON r.id = a.run_id
                {where_sql}
                """,
                params,
            )
            count_row = cur.fetchone() or {}
            total_count = int(count_row.get("total_count") or 0)
            cur.execute(
                f"""
                SELECT
                    a.id,
                    a.run_id,
                    r.run_date,
                    r.status AS run_status,
                    a.section_index,
                    a.section_title,
                    a.url,
                    a.source_host,
                    a.category,
                    a.category_slug,
                    a.fetch_status,
                    a.fetch_error,
                    a.title,
                    a.article_excerpt,
                    a.codex_summary,
                    a.key_points,
                    a.sector_tags,
                    a.market_tags,
                    a.data_gaps,
                    a.updated_at
                FROM news_mail_articles a
                JOIN news_mail_runs r ON r.id = a.run_id
                {where_sql}
                ORDER BY r.run_date DESC, a.section_index ASC, a.updated_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                params,
            )
            article_rows = [dict(row) for row in cur.fetchall()]
            articles = _attach_article_impacts(cur, article_rows)
    return {
        "items": articles,
        "count": len(articles),
        "total_count": total_count,
        "limit": safe_limit,
        "offset": safe_offset,
        "category": category_filter or "all",
        "symbol": symbol_filter or None,
        "has_previous": safe_offset > 0,
        "has_next": safe_offset + len(articles) < total_count,
    }


def _dashboard_articles(cur: Any, run_id: UUID | str, *, limit: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, section_index, section_title, url, source_host, fetch_status, fetch_error,
               category, category_slug, title, article_excerpt, codex_summary, key_points,
               sector_tags, market_tags, data_gaps, updated_at
        FROM news_mail_articles
        WHERE run_id = %(run_id)s
        ORDER BY section_index ASC, updated_at DESC
        LIMIT %(limit)s
        """,
        {"run_id": run_id, "limit": max(1, min(int(limit), 500))},
    )
    article_rows = [dict(row) for row in cur.fetchall()]
    return _attach_article_impacts(cur, article_rows)


def _attach_article_impacts(cur: Any, article_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not article_rows:
        return []
    ids = [row["id"] for row in article_rows]
    cur.execute(
        """
        SELECT id, article_id, symbol, company_name, known_symbol, relevance_score,
               sentiment_label, sentiment_score, impact_score, impact_horizon,
               confidence, rationale, created_at
        FROM news_mail_symbol_impacts
        WHERE article_id = ANY(%(article_ids)s)
        ORDER BY impact_score DESC, relevance_score DESC
        """,
        {"article_ids": ids},
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for impact in cur.fetchall():
        row = _serialize_row(dict(impact))
        grouped.setdefault(str(row["article_id"]), []).append(row)
    out: list[dict[str, Any]] = []
    for article in article_rows:
        serialized = _serialize_row(article)
        serialized["article_id"] = serialized.pop("id")
        serialized["impacts"] = grouped.get(str(serialized["article_id"]), [])
        out.append(serialized)
    return out


def recompute_news_mail_article_categories(run_id: UUID | str | None = None) -> dict[str, Any]:
    ensure_news_mail_tables()
    params: dict[str, Any] = {}
    where = ""
    if run_id:
        where = "WHERE run_id = %(run_id)s"
        params["run_id"] = run_id
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT id
                FROM news_mail_articles
                {where}
                """,
                params,
            )
            article_ids = [row["id"] for row in cur.fetchall()]
            if not article_ids:
                return {"article_count": 0, "updated_count": 0}
            cur.execute(
                """
                SELECT article_id, symbol, known_symbol, relevance_score, impact_score, confidence
                FROM news_mail_symbol_impacts
                WHERE article_id = ANY(%(article_ids)s)
                """,
                {"article_ids": article_ids},
            )
            impact_rows = [dict(row) for row in cur.fetchall()]
            all_symbols = [str(row.get("symbol") or "").strip().upper() for row in impact_rows]
            known = {str(row.get("symbol") or "").strip().upper() for row in impact_rows if row.get("known_symbol")}
            symbol_categories = _news_mail_symbol_category_lookup(all_symbols, known)
            impacts_by_article: dict[str, list[dict[str, Any]]] = {}
            for row in impact_rows:
                impacts_by_article.setdefault(str(row.get("article_id")), []).append(row)
            updates: list[dict[str, Any]] = []
            for article_id in article_ids:
                category, category_slug = _news_mail_article_category_from_symbols(
                    impacts_by_article.get(str(article_id), []),
                    symbol_categories,
                )
                updates.append(
                    {
                        "article_id": article_id,
                        "category": category,
                        "category_slug": category_slug,
                    }
                )
            cur.executemany(
                """
                UPDATE news_mail_articles
                SET category = %(category)s,
                    category_slug = %(category_slug)s,
                    updated_at = NOW()
                WHERE id = %(article_id)s
                """,
                updates,
            )
        conn.commit()
    return {"article_count": len(article_ids), "updated_count": len(updates)}


def get_news_by_symbol(symbol: str, limit: int = 50) -> dict[str, Any]:
    normalized = symbol.strip().upper()
    if not normalized:
        return {"symbol": "", "items": [], "count": 0}
    ensure_news_mail_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    i.id AS impact_id,
                    i.symbol,
                    i.company_name,
                    i.known_symbol,
                    i.relevance_score,
                    i.sentiment_label,
                    i.sentiment_score,
                    i.impact_score,
                    i.impact_horizon,
                    i.confidence,
                    i.rationale,
                    a.id AS article_id,
                    a.title,
                    a.codex_summary,
                    a.article_excerpt,
                    a.url,
                    a.source_host,
                    a.category,
                    a.category_slug,
                    a.section_title,
                    r.run_date,
                    r.status AS run_status
                FROM news_mail_symbol_impacts i
                JOIN news_mail_articles a ON a.id = i.article_id
                JOIN news_mail_runs r ON r.id = i.run_id
                WHERE i.symbol = %(symbol)s
                ORDER BY r.run_date DESC, i.impact_score DESC, i.updated_at DESC
                LIMIT %(limit)s
                """,
                {"symbol": normalized, "limit": max(1, min(int(limit), 200))},
            )
            rows = [_serialize_row(dict(row)) for row in cur.fetchall()]
    return {"symbol": normalized, "items": rows, "count": len(rows)}


def get_top_impact_news(limit: int = 20, sentiment: str | None = None) -> dict[str, Any]:
    ensure_news_mail_tables()
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 100))}
    where = ""
    label = (sentiment or "").strip().lower()
    if label in {"positive", "negative", "neutral", "mixed"}:
        where = "WHERE i.sentiment_label = %(sentiment)s"
        params["sentiment"] = label
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                    i.id AS impact_id,
                    i.symbol,
                    i.company_name,
                    i.known_symbol,
                    i.sentiment_label,
                    i.sentiment_score,
                    i.impact_score,
                    i.impact_horizon,
                    i.confidence,
                    i.rationale,
                    a.id AS article_id,
                    a.title,
                    a.codex_summary,
                    a.article_excerpt,
                    a.url,
                    a.source_host,
                    a.category,
                    a.category_slug,
                    r.run_date
                FROM news_mail_symbol_impacts i
                JOIN news_mail_articles a ON a.id = i.article_id
                JOIN news_mail_runs r ON r.id = i.run_id
                {where}
                ORDER BY i.impact_score DESC, ABS(i.sentiment_score) DESC, i.updated_at DESC
                LIMIT %(limit)s
                """,
                params,
            )
            rows = [_serialize_row(dict(row)) for row in cur.fetchall()]
    return {"items": rows, "count": len(rows), "sentiment": label or None}


def get_morning_brief(limit: int = 10) -> dict[str, Any]:
    today = get_today_news_mail(limit=200)
    articles = today.get("articles") if isinstance(today, dict) else []
    impacts: list[dict[str, Any]] = []
    for article in articles or []:
        for impact in article.get("impacts") or []:
            impacts.append(
                {
                    **impact,
                    "article_title": article.get("title") or article.get("section_title"),
                    "category": article.get("category"),
                    "category_slug": article.get("category_slug"),
                }
            )
    top = sorted(impacts, key=lambda row: float(row.get("impact_score") or 0), reverse=True)[:limit]
    positive = [row for row in top if row.get("sentiment_label") == "positive"]
    negative = [row for row in top if row.get("sentiment_label") == "negative"]
    return {
        "run": today.get("run"),
        "article_count": len(articles or []),
        "impact_count": len(impacts),
        "top_impacts": top,
        "positive_count": len([row for row in impacts if row.get("sentiment_label") == "positive"]),
        "negative_count": len([row for row in impacts if row.get("sentiment_label") == "negative"]),
        "brief": {
            "positive": positive[:5],
            "negative": negative[:5],
            "watch": [row for row in top if row.get("sentiment_label") in {"mixed", "neutral"}][:5],
        },
    }


def get_watchlist_alerts(watchlist_id: str, symbols: list[str], limit: int = 50) -> dict[str, Any]:
    normalized = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not normalized:
        return {"watchlist_id": watchlist_id, "symbols": [], "items": [], "count": 0}
    ensure_news_mail_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    i.symbol,
                    i.sentiment_label,
                    i.sentiment_score,
                    i.impact_score,
                    i.confidence,
                    i.rationale,
                    a.id AS article_id,
                    a.title,
                    a.codex_summary,
                    a.url,
                    a.source_host,
                    a.category,
                    a.category_slug,
                    r.run_date
                FROM news_mail_symbol_impacts i
                JOIN news_mail_articles a ON a.id = i.article_id
                JOIN news_mail_runs r ON r.id = i.run_id
                WHERE i.symbol = ANY(%(symbols)s)
                ORDER BY r.run_date DESC, i.impact_score DESC, i.updated_at DESC
                LIMIT %(limit)s
                """,
                {"symbols": normalized, "limit": max(1, min(int(limit), 200))},
            )
            rows = [_serialize_row(dict(row)) for row in cur.fetchall()]
    return {"watchlist_id": watchlist_id, "symbols": normalized, "items": rows, "count": len(rows)}


def _price_rows_for_symbol(symbol: str, event_date: date, horizon: int) -> list[dict[str, Any]]:
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT trading_date, close_price, volume
                FROM market_symbol_daily_volume
                WHERE symbol = %(symbol)s
                  AND trading_date >= %(event_date)s
                  AND close_price IS NOT NULL
                  AND close_price > 0
                ORDER BY trading_date ASC
                LIMIT %(limit)s
                """,
                {"symbol": symbol, "event_date": event_date, "limit": horizon + 1},
            )
            return [dict(row) for row in cur.fetchall()]


def _return_pct(base_close: float | None, future_close: float | None) -> float | None:
    if base_close is None or future_close is None or base_close <= 0 or future_close <= 0:
        return None
    return ((future_close - base_close) / base_close) * 100.0


def _volume_change_pct(base_volume: float | None, future_volume: float | None) -> float | None:
    if base_volume is None or future_volume is None or base_volume <= 0 or future_volume < 0:
        return None
    return ((future_volume - base_volume) / base_volume) * 100.0


def _news_event_outcome(
    *,
    sentiment_label: str,
    sentiment_score: float,
    return_pct: float | None,
    abnormal_return_pct: float | None,
    volume_change_pct: float | None,
) -> tuple[str | None, float | None]:
    directional = abnormal_return_pct if abnormal_return_pct is not None else return_pct
    if directional is None:
        return None, None
    label = str(sentiment_label or "neutral").strip().lower()
    score = 50.0
    if label == "positive":
        score = 50.0 + directional * 6.0
        if directional >= 0.75:
            outcome = "sentiment_confirmed"
        elif directional <= -0.75:
            outcome = "sentiment_failed"
        else:
            outcome = "flat_after_positive_news"
    elif label == "negative":
        score = 50.0 - directional * 6.0
        if directional <= -0.75:
            outcome = "avoidance_confirmed"
        elif directional >= 0.75:
            outcome = "negative_news_rebounded"
        else:
            outcome = "flat_after_negative_news"
    else:
        vol = abs(volume_change_pct or 0.0)
        if abs(directional) < 0.75:
            outcome = "neutral_confirmed"
            score = 62.0 - min(12.0, vol / 10.0)
        else:
            outcome = "mixed_directional_move"
            score = 45.0 + min(12.0, vol / 12.0)
    conviction = min(1.0, abs(float(sentiment_score)) / 100.0) if label in {"positive", "negative"} else 0.55
    adjusted_score = max(0.0, min(100.0, 50.0 + (score - 50.0) * max(0.35, conviction)))
    return outcome, round(adjusted_score, 4)


def refresh_sentiment_return_research(
    *,
    run_id: UUID | str | None = None,
    days_back: int = 90,
) -> dict[str, Any]:
    ensure_news_mail_tables()
    ensure_market_symbol_tables()
    params: dict[str, Any] = {"days_back": max(1, min(int(days_back), 2000))}
    where = "WHERE r.run_date >= CURRENT_DATE - (%(days_back)s::INT * INTERVAL '1 day')"
    if run_id:
        where += " AND r.id = %(run_id)s"
        params["run_id"] = run_id
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                    i.id AS symbol_impact_id,
                    i.run_id,
                    i.article_id,
                    i.symbol,
                    i.sentiment_label,
                    i.sentiment_score,
                    i.impact_score,
                    i.confidence,
                    r.run_date AS event_date
                FROM news_mail_symbol_impacts i
                JOIN news_mail_runs r ON r.id = i.run_id
                {where}
                ORDER BY r.run_date DESC
                """,
                params,
            )
            impacts = [dict(row) for row in cur.fetchall()]

    written = 0
    pending = 0
    for impact in impacts:
        symbol = str(impact.get("symbol") or "").upper()
        event_date = impact.get("event_date")
        if not symbol or not isinstance(event_date, date):
            continue
        for horizon in (1, 3, 5):
            prices = _price_rows_for_symbol(symbol, event_date, horizon)
            benchmark_prices = _price_rows_for_symbol("VNINDEX", event_date, horizon)
            status = "ready"
            base_date = base_close = future_date = future_close = return_pct = None
            base_volume = future_volume = volume_change = None
            benchmark_symbol = "VNINDEX"
            benchmark_base_date = benchmark_base_close = benchmark_future_date = benchmark_future_close = None
            benchmark_return_pct = None
            abnormal_return_pct = None
            outcome_label = None
            outcome_score = None
            metadata: dict[str, Any] = {
                "event_study_version": "1.0.0",
                "benchmark_symbol": benchmark_symbol,
                "benchmark_missing": True,
            }
            if not prices:
                status = "pending_base"
                pending += 1
            elif len(prices) <= horizon:
                status = "pending_future"
                base_date = prices[0]["trading_date"]
                base_close = _safe_float(prices[0]["close_price"])
                base_volume = _safe_float(prices[0].get("volume"))
                pending += 1
            else:
                base = prices[0]
                future = prices[horizon]
                base_date = base["trading_date"]
                future_date = future["trading_date"]
                base_close = _safe_float(base["close_price"])
                future_close = _safe_float(future["close_price"])
                base_volume = _safe_float(base.get("volume"))
                future_volume = _safe_float(future.get("volume"))
                return_pct = _return_pct(base_close, future_close)
                volume_change = _volume_change_pct(base_volume, future_volume)
                if len(benchmark_prices) > horizon:
                    benchmark_base = benchmark_prices[0]
                    benchmark_future = benchmark_prices[horizon]
                    benchmark_base_date = benchmark_base.get("trading_date")
                    benchmark_future_date = benchmark_future.get("trading_date")
                    benchmark_base_close = _safe_float(benchmark_base.get("close_price"))
                    benchmark_future_close = _safe_float(benchmark_future.get("close_price"))
                    benchmark_return_pct = _return_pct(benchmark_base_close, benchmark_future_close)
                    metadata["benchmark_missing"] = benchmark_return_pct is None
                if return_pct is not None:
                    abnormal_return_pct = return_pct - benchmark_return_pct if benchmark_return_pct is not None else return_pct
                    outcome_label, outcome_score = _news_event_outcome(
                        sentiment_label=str(impact.get("sentiment_label") or "neutral"),
                        sentiment_score=float(_safe_float(impact.get("sentiment_score"), 0.0) or 0.0),
                        return_pct=return_pct,
                        abnormal_return_pct=abnormal_return_pct,
                        volume_change_pct=volume_change,
                    )
                else:
                    status = "pending_base"
                    pending += 1
            with connect(settings.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO news_mail_return_research (
                            id, run_id, article_id, symbol_impact_id, symbol, event_date,
                            horizon_days, base_trading_date, base_close, future_trading_date,
                            future_close, return_pct, benchmark_symbol, benchmark_base_trading_date,
                            benchmark_base_close, benchmark_future_trading_date, benchmark_future_close,
                            benchmark_return_pct, abnormal_return_pct, base_volume, future_volume,
                            volume_change_pct, outcome_label, outcome_score, status, metadata
                        )
                        VALUES (
                            %(id)s, %(run_id)s, %(article_id)s, %(symbol_impact_id)s, %(symbol)s,
                            %(event_date)s, %(horizon_days)s, %(base_trading_date)s, %(base_close)s,
                            %(future_trading_date)s, %(future_close)s, %(return_pct)s, %(benchmark_symbol)s,
                            %(benchmark_base_trading_date)s, %(benchmark_base_close)s,
                            %(benchmark_future_trading_date)s, %(benchmark_future_close)s,
                            %(benchmark_return_pct)s, %(abnormal_return_pct)s, %(base_volume)s,
                            %(future_volume)s, %(volume_change_pct)s, %(outcome_label)s,
                            %(outcome_score)s, %(status)s, %(metadata)s
                        )
                        ON CONFLICT (symbol_impact_id, horizon_days)
                        DO UPDATE SET
                            base_trading_date = EXCLUDED.base_trading_date,
                            base_close = EXCLUDED.base_close,
                            future_trading_date = EXCLUDED.future_trading_date,
                            future_close = EXCLUDED.future_close,
                            return_pct = EXCLUDED.return_pct,
                            benchmark_symbol = EXCLUDED.benchmark_symbol,
                            benchmark_base_trading_date = EXCLUDED.benchmark_base_trading_date,
                            benchmark_base_close = EXCLUDED.benchmark_base_close,
                            benchmark_future_trading_date = EXCLUDED.benchmark_future_trading_date,
                            benchmark_future_close = EXCLUDED.benchmark_future_close,
                            benchmark_return_pct = EXCLUDED.benchmark_return_pct,
                            abnormal_return_pct = EXCLUDED.abnormal_return_pct,
                            base_volume = EXCLUDED.base_volume,
                            future_volume = EXCLUDED.future_volume,
                            volume_change_pct = EXCLUDED.volume_change_pct,
                            outcome_label = EXCLUDED.outcome_label,
                            outcome_score = EXCLUDED.outcome_score,
                            status = EXCLUDED.status,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW()
                        """,
                        {
                            "id": uuid4(),
                            "run_id": impact["run_id"],
                            "article_id": impact["article_id"],
                            "symbol_impact_id": impact["symbol_impact_id"],
                            "symbol": symbol,
                            "event_date": event_date,
                            "horizon_days": horizon,
                            "base_trading_date": base_date,
                            "base_close": base_close,
                            "future_trading_date": future_date,
                            "future_close": future_close,
                            "return_pct": return_pct,
                            "benchmark_symbol": benchmark_symbol,
                            "benchmark_base_trading_date": benchmark_base_date,
                            "benchmark_base_close": benchmark_base_close,
                            "benchmark_future_trading_date": benchmark_future_date,
                            "benchmark_future_close": benchmark_future_close,
                            "benchmark_return_pct": benchmark_return_pct,
                            "abnormal_return_pct": abnormal_return_pct,
                            "base_volume": base_volume,
                            "future_volume": future_volume,
                            "volume_change_pct": volume_change,
                            "outcome_label": outcome_label,
                            "outcome_score": outcome_score,
                            "status": status,
                            "metadata": _as_json(metadata),
                        },
                    )
                conn.commit()
            written += 1
    experience_refresh: dict[str, Any] | None = None
    try:
        from app.services.news_impact_experience_service import refresh_news_impact_experience

        experience_refresh = refresh_news_impact_experience(days_back=max(365, int(days_back)))
    except Exception as exc:
        logger.warning("news_impact_experience_refresh_failed", extra={"error": str(exc)})
    return {
        "impacts_scanned": len(impacts),
        "rows_written": written,
        "pending": pending,
        "experience_refresh": experience_refresh,
    }


def get_sentiment_return_research(
    *,
    refresh: bool = False,
    days_back: int = 90,
    limit: int = 200,
) -> dict[str, Any]:
    ensure_news_mail_tables()
    refresh_result = refresh_sentiment_return_research(days_back=days_back) if refresh else None
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    rr.symbol,
                    rr.event_date,
                    rr.horizon_days,
                    rr.base_trading_date,
                    rr.base_close,
                    rr.future_trading_date,
                    rr.future_close,
                    rr.return_pct,
                    rr.benchmark_symbol,
                    rr.benchmark_return_pct,
                    rr.abnormal_return_pct,
                    rr.base_volume,
                    rr.future_volume,
                    rr.volume_change_pct,
                    rr.outcome_label,
                    rr.outcome_score,
                    rr.metadata,
                    rr.status,
                    i.sentiment_label,
                    i.sentiment_score,
                    i.impact_score,
                    i.confidence,
                    a.title,
                    a.url
                FROM news_mail_return_research rr
                JOIN news_mail_symbol_impacts i ON i.id = rr.symbol_impact_id
                JOIN news_mail_articles a ON a.id = rr.article_id
                WHERE rr.event_date >= CURRENT_DATE - (%(days_back)s::INT * INTERVAL '1 day')
                ORDER BY rr.event_date DESC, rr.symbol ASC, rr.horizon_days ASC
                LIMIT %(limit)s
                """,
                {
                    "days_back": max(1, min(int(days_back), 2000)),
                    "limit": max(1, min(int(limit), 2000)),
                },
            )
            rows = [_serialize_row(dict(row)) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT i.sentiment_label, rr.horizon_days, COUNT(*)::INT AS count,
                       AVG(rr.return_pct) AS avg_return_pct,
                       AVG(rr.abnormal_return_pct) AS avg_abnormal_return_pct,
                       AVG(rr.volume_change_pct) AS avg_volume_change_pct,
                       AVG(rr.outcome_score) AS avg_outcome_score
                FROM news_mail_return_research rr
                JOIN news_mail_symbol_impacts i ON i.id = rr.symbol_impact_id
                WHERE rr.status = 'ready'
                  AND rr.event_date >= CURRENT_DATE - (%(days_back)s::INT * INTERVAL '1 day')
                GROUP BY i.sentiment_label, rr.horizon_days
                ORDER BY rr.horizon_days ASC, i.sentiment_label ASC
                """,
                {"days_back": max(1, min(int(days_back), 2000))},
            )
            summary = [_serialize_row(dict(row)) for row in cur.fetchall()]
    return {"rows": rows, "summary": summary, "count": len(rows), "refresh": refresh_result}
