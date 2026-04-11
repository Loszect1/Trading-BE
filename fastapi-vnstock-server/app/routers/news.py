from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import sha256
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from app.core.config import settings
from app.core.firecrawl_discovery import FIRECRAWL_DISCOVERY_QUERIES, DiscoveryPreset
from app.core.firecrawl_press_config import PressScope
from app.core.news_feed_registry import (
    NEWS_API_HINTS,
    NEWS_FEED_SOURCES,
    NEWS_SOURCES_PENDING_INTEGRATION,
    NewsCategory,
    NewsFeedSource,
)
from app.services.firecrawl_search_service import (
    FirecrawlSearchService,
    normalize_firecrawl_search_response,
)
from app.services.news_aggregator_service import NewsAggregatorService
from app.services.redis_cache import RedisCacheService

router = APIRouter(prefix="/news", tags=["news"])

_news_service = NewsAggregatorService()
_redis_cache = RedisCacheService()
_firecrawl_service = FirecrawlSearchService()

NEWS_SOURCES_BY_ID: dict[str, NewsFeedSource] = {src.id: src for src in NEWS_FEED_SOURCES}

NewsCategoryParam = Literal["all", "domestic", "world", "social"]


def _category_set(category: NewsCategoryParam) -> set[NewsCategory] | None:
    if category == "all":
        return None
    return {category}


def _build_news_cache_key(
    category: NewsCategoryParam,
    per_feed_limit: int,
    response_limit: int,
    use_firecrawl: bool,
    use_firecrawl_rss_fallback: bool,
) -> str:
    fc_on = "1" if use_firecrawl else "0"
    fc_cfg = "1" if _firecrawl_service.is_configured() else "0"
    fc_max = max(1, min(int(settings.news_firecrawl_today_max), 20))
    fb_on = "1" if (use_firecrawl and use_firecrawl_rss_fallback) else "0"
    fb_pf = int(settings.news_firecrawl_fallback_per_feed)
    fb_tot = int(settings.news_firecrawl_fallback_max_total)
    raw = f"{category}:{per_feed_limit}:{response_limit}:{fc_on}:{fc_cfg}:{fc_max}:{fb_on}:{fb_pf}:{fb_tot}:v5"
    digest = sha256(raw.encode("utf-8")).hexdigest()
    return f"news:aggregate:{digest}"


def _press_scope_for_category(category: NewsCategoryParam) -> PressScope | None:
    if category == "social":
        return None
    if category == "domestic":
        return "domestic"
    if category == "world":
        return "world"
    return "all"


def _normalize_item_link(link: str) -> str:
    return (link or "").strip().lower().rstrip("/")


def _merge_rss_and_firecrawl(
    rss_items: list[dict[str, Any]],
    fc_items: list[dict[str, Any]],
    response_limit: int,
) -> list[dict[str, Any]]:
    """Gộp RSS + Firecrawl; trùng URL thì ưu tiên bản Firecrawl. Sắp xếp theo published_at (chuỗi)."""
    bucket: dict[str, dict[str, Any]] = {}
    seq = 0

    for item in rss_items:
        row = dict(item)
        row.setdefault("item_origin", "rss")
        link_key = _normalize_item_link(str(row.get("link") or ""))
        key = link_key if link_key else f"__rss_{seq}"
        seq += 1
        bucket[key] = row

    for item in fc_items:
        row = dict(item)
        link_key = _normalize_item_link(str(row.get("link") or ""))
        key = link_key if link_key else f"__fc_{seq}"
        seq += 1
        bucket[key] = row

    merged = list(bucket.values())

    def _sort_key(row: dict[str, Any]) -> str:
        published = row.get("published_at")
        if isinstance(published, str) and published.strip():
            return published.strip()
        return ""

    merged.sort(key=_sort_key, reverse=True)
    return merged[:response_limit]


def _run_firecrawl_rss_fallback(
    feed_errors: dict[str, str],
    *,
    use_firecrawl: bool,
    use_firecrawl_rss_fallback: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Với mỗi feed RSS lỗi (có trong registry), gọi Firecrawl Search theo site:domain, tin trong ngày."""
    meta: dict[str, Any] = {
        "active": False,
        "attempted_source_ids": [],
        "recovered_source_ids": [],
        "items_count": 0,
        "per_feed_cap": int(settings.news_firecrawl_fallback_per_feed),
        "max_total_cap": int(settings.news_firecrawl_fallback_max_total),
        "sub_errors": {},
        "skipped_reason": None,
    }

    if not use_firecrawl:
        meta["skipped_reason"] = "use_firecrawl_false"
        return [], meta
    if not use_firecrawl_rss_fallback:
        meta["skipped_reason"] = "use_firecrawl_rss_fallback_false"
        return [], meta
    if not _firecrawl_service.is_configured():
        meta["skipped_reason"] = "firecrawl_api_key_missing"
        return [], meta

    max_total = int(settings.news_firecrawl_fallback_max_total)
    if max_total <= 0:
        meta["skipped_reason"] = "fallback_disabled_max_total_zero"
        return [], meta

    if not feed_errors:
        meta["skipped_reason"] = "no_feed_errors"
        return [], meta

    failed_ids = sorted(
        sid for sid in feed_errors if sid != "unknown" and sid in NEWS_SOURCES_BY_ID
    )
    if not failed_ids:
        meta["skipped_reason"] = "no_mapped_failed_sources"
        return [], meta

    meta["active"] = True
    meta["attempted_source_ids"] = failed_ids

    per_feed = max(1, min(int(settings.news_firecrawl_fallback_per_feed), 25))
    workers = max(1, min(int(settings.news_firecrawl_fallback_max_workers), len(failed_ids)))
    timeout_ms = int(settings.firecrawl_search_timeout_ms)

    def work(source_id: str) -> tuple[str, list[dict[str, Any]], str | None]:
        src = NEWS_SOURCES_BY_ID[source_id]
        rows, err = _firecrawl_service.fetch_rss_fallback_for_source(
            feed_url=src.feed_url,
            display_name=src.display_name,
            source_id=src.id,
            category=src.category,
            max_items=per_feed,
            timeout_ms=timeout_ms,
        )
        return source_id, rows, err

    collected: list[dict[str, Any]] = []
    recovered: set[str] = set()
    sub_errors: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(work, sid): sid for sid in failed_ids}
        for future in as_completed(future_map):
            sid = future_map[future]
            try:
                source_id, rows, err = future.result()
            except Exception as exc:
                sub_errors[sid] = f"{exc.__class__.__name__}: {exc}"
                continue
            if err:
                sub_errors[source_id] = err
                continue
            if rows:
                recovered.add(source_id)
            collected.extend(rows)

    meta["sub_errors"] = sub_errors
    meta["recovered_source_ids"] = sorted(recovered)
    collected = collected[:max_total]
    meta["items_count"] = len(collected)
    return collected, meta


@router.get("")
def list_news(
    category: NewsCategoryParam = Query(
        "all",
        description="Lọc theo nhóm nguồn: domestic, world, social hoặc all.",
    ),
    per_feed_limit: int = Query(
        12,
        ge=3,
        le=40,
        description="Số bài tối đa lấy từ mỗi feed RSS.",
    ),
    limit: int = Query(
        120,
        ge=10,
        le=400,
        description="Số bài tối đa trả về sau khi gộp RSS + Firecrawl và loại trùng theo URL.",
    ),
    use_firecrawl: bool = Query(
        True,
        description="Gọi Firecrawl Search (tin trong ngày, tối đa 20) từ các domain báo đã cấu hình; cần FIRECRAWL_API_KEY.",
    ),
    use_firecrawl_rss_fallback: bool = Query(
        True,
        description="Nếu một feed RSS lỗi, tự Firecrawl Search theo site:domain (tin trong ngày); cần FIRECRAWL_API_KEY.",
    ),
    force_refresh: bool = Query(False, description="Bỏ qua cache Redis nếu có."),
) -> dict[str, Any]:
    """
    Tổng hợp tin từ RSS công khai và (nếu bật) Firecrawl: tin trong ngày tối đa 20 từ các báo
    (VN + quốc tế theo `category`), lọc `tbs=qdr:d`. Feed RSS lỗi có thể bù bằng Firecrawl theo domain.
    Reddit vẫn chỉ qua RSS khi `category=social`.
    """
    cache_key = _build_news_cache_key(
        category,
        per_feed_limit,
        limit,
        use_firecrawl,
        use_firecrawl_rss_fallback,
    )
    if not force_refresh:
        cached = _redis_cache.get_json(cache_key)
        if cached is not None:
            return cached

    fc_max = max(1, min(int(settings.news_firecrawl_today_max), 20))
    press_scope: PressScope | None = None
    if use_firecrawl and _firecrawl_service.is_configured():
        press_scope = _press_scope_for_category(category)

    fc_items: list[dict[str, Any]] = []
    fc_meta: dict[str, Any] = {
        "active": press_scope is not None,
        "count": 0,
        "max": fc_max,
        "query": None,
        "country": None,
        "credits_used": None,
        "warning": None,
        "error": None,
        "skipped_reason": None,
    }

    if category == "social":
        fc_meta["skipped_reason"] = "category_social"
    elif not use_firecrawl:
        fc_meta["skipped_reason"] = "use_firecrawl_false"
    elif not _firecrawl_service.is_configured():
        fc_meta["skipped_reason"] = "firecrawl_api_key_missing"

    workers = 2 if press_scope else 1
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_rss = executor.submit(
                _news_service.aggregate,
                _category_set(category),
                per_feed_limit,
                8,
                12.0,
            )
            future_fc = None
            if press_scope is not None:
                future_fc = executor.submit(
                    _firecrawl_service.fetch_press_today,
                    scope=press_scope,
                    max_items=fc_max,
                    timeout_ms=int(settings.firecrawl_search_timeout_ms),
                )

            try:
                payload = future_rss.result()
            except Exception as exc:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to aggregate news feeds: {exc}",
                ) from exc

            if future_fc is not None:
                try:
                    fc_items, fetch_meta = future_fc.result()
                except Exception as exc:
                    fc_items = []
                    fc_meta["error"] = f"{exc.__class__.__name__}: {exc}"
                else:
                    fc_meta.update(
                        {
                            "query": fetch_meta.get("query"),
                            "country": fetch_meta.get("country"),
                            "credits_used": fetch_meta.get("credits_used"),
                            "warning": fetch_meta.get("warning"),
                            "error": fetch_meta.get("error"),
                        }
                    )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load news: {exc}",
        ) from exc

    rss_list = payload.get("items") if isinstance(payload.get("items"), list) else []

    feed_errors = payload.get("feed_errors") or {}
    if not isinstance(feed_errors, dict):
        feed_errors = {}

    fallback_items, fallback_meta = _run_firecrawl_rss_fallback(
        feed_errors,
        use_firecrawl=use_firecrawl,
        use_firecrawl_rss_fallback=use_firecrawl_rss_fallback,
    )

    merged = _merge_rss_and_firecrawl(rss_list, fallback_items + fc_items, limit)
    fc_meta["count"] = len(fc_items)

    rss_fetch_failure_recovery: dict[str, Any] | None = None
    if feed_errors:
        auto_fb_note = ""
        if fallback_meta.get("active") and fallback_meta.get("items_count", 0) > 0:
            auto_fb_note = (
                " Hệ thống đã thử bổ sung tin qua Firecrawl cho từng domain lỗi (xem firecrawl_rss_fallback)."
            )
        rss_fetch_failure_recovery = {
            "message": (
                "Các feed RSS sau không tải hoặc parse được."
                + auto_fb_note
                + " Có thể tìm URL feed thay thế bằng Firecrawl (cần FIRECRAWL_API_KEY). "
                "Chi tiết lỗi từng nguồn nằm ở feed_errors."
            ),
            "failed_source_ids": sorted(str(k) for k in feed_errors.keys()),
            "suggested_steps": [
                {
                    "action": "firecrawl_discover",
                    "endpoint": "GET /news/firecrawl/discover",
                    "query_example": "?preset=domestic hoặc ?preset=world hoặc ?preset=all",
                    "detail": NEWS_API_HINTS["firecrawl_discover_rss"],
                },
                {
                    "action": "firecrawl_search",
                    "endpoint": "GET /news/firecrawl/search",
                    "query_example": "?q=site:example.com rss",
                    "detail": NEWS_API_HINTS["firecrawl_search_custom"],
                },
            ],
        }

    response: dict[str, Any] = {
        "category": category,
        "per_feed_limit": per_feed_limit,
        "limit": limit,
        "use_firecrawl": use_firecrawl,
        "use_firecrawl_rss_fallback": use_firecrawl_rss_fallback,
        "count": len(merged),
        "items": merged,
        "feed_errors": feed_errors,
        "rss_fetch_failure_recovery": rss_fetch_failure_recovery,
        "fetched_at": payload.get("fetched_at"),
        "firecrawl_today": fc_meta,
        "firecrawl_rss_fallback": fallback_meta,
        "sources_pending_integration": list(NEWS_SOURCES_PENDING_INTEGRATION),
    }
    ttl = max(30, int(settings.news_cache_ttl_seconds))
    _redis_cache.set_json(cache_key, response, ttl_seconds=ttl)
    return response


@router.get("/sources")
def list_news_sources() -> dict[str, Any]:
    """Danh sách feed đã cấu hình (metadata, không gọi mạng)."""
    feeds = [
        {
            "id": src.id,
            "display_name": src.display_name,
            "category": src.category,
            "feed_url": src.feed_url,
        }
        for src in NEWS_FEED_SOURCES
    ]
    return {
        "feeds": feeds,
        "sources_pending_integration": list(NEWS_SOURCES_PENDING_INTEGRATION),
        "hints": dict(NEWS_API_HINTS),
    }


def _require_firecrawl() -> None:
    if not _firecrawl_service.is_configured():
        raise HTTPException(
            status_code=503,
            detail="Firecrawl chưa cấu hình. Đặt biến môi trường FIRECRAWL_API_KEY (Bearer token từ firecrawl.dev).",
        )


def _discovery_queries_for_preset(preset: DiscoveryPreset | Literal["all"]) -> list:
    if preset == "all":
        return list(FIRECRAWL_DISCOVERY_QUERIES)
    return [q for q in FIRECRAWL_DISCOVERY_QUERIES if q.preset == preset]


def _run_single_firecrawl_query(
    query_id: str,
    query_text: str,
    country: str,
    per_query_limit: int,
) -> dict[str, Any]:
    raw = _firecrawl_service.search(
        query_text,
        limit=per_query_limit,
        sources=[{"type": "web"}],
        country=country or None,
        timeout_ms=int(settings.firecrawl_search_timeout_ms),
    )
    normalized = normalize_firecrawl_search_response(raw)
    rss_candidates = []
    for bucket_rows in normalized.get("buckets", {}).values():
        for row in bucket_rows:
            if row.get("rss_hint") and row.get("url"):
                rss_candidates.append(row["url"])
    return {
        "id": query_id,
        "query": query_text,
        "country": country or None,
        "firecrawl": normalized,
        "rss_url_candidates": sorted(set(rss_candidates)),
    }


@router.get("/firecrawl/search")
def firecrawl_search_news_sources(
    q: str = Query(..., min_length=2, max_length=500, description="Câu truy vấn Firecrawl Search."),
    limit: int = Query(8, ge=1, le=25, description="Số kết quả tối đa."),
    country: str = Query(
        "",
        max_length=8,
        description="Mã quốc gia (VN, US, ...); để trống dùng mặc định API.",
    ),
    tbs: str = Query(
        "",
        max_length=32,
        description="Lọc thời gian Google-style, ví dụ qdr:d (trong ngày). Để trống = không lọc.",
    ),
) -> dict[str, Any]:
    """
    Tìm kiếm tuỳ chỉnh qua Firecrawl (web). Dùng để rà soát URL RSS/feed hoặc trang nguồn tin.
    Tốn credit theo gói Firecrawl; không cache mặc định.
    """
    _require_firecrawl()
    tbs_val = tbs.strip() or None
    try:
        raw = _firecrawl_service.search(
            q.strip(),
            limit=limit,
            sources=[{"type": "web"}],
            country=country.strip() or None,
            timeout_ms=int(settings.firecrawl_search_timeout_ms),
            tbs=tbs_val,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    normalized = normalize_firecrawl_search_response(raw)
    rss_candidates = []
    for bucket_rows in normalized.get("buckets", {}).values():
        for row in bucket_rows:
            if row.get("rss_hint") and row.get("url"):
                rss_candidates.append(row["url"])
    return {
        "query": q.strip(),
        "limit": limit,
        "country": country.strip() or None,
        "tbs": tbs_val,
        "firecrawl": normalized,
        "rss_url_candidates": sorted(set(rss_candidates)),
    }


@router.get("/firecrawl/discover")
def firecrawl_discover_feed_candidates(
    preset: DiscoveryPreset | Literal["all"] = Query(
        "all",
        description="domestic: nguồn VN; world: nguồn quốc tế; all: chạy cả hai nhóm.",
    ),
    per_query_limit: int = Query(
        6,
        ge=1,
        le=15,
        description="Số kết quả Firecrawl cho mỗi truy vấn discovery.",
    ),
    max_workers: int = Query(4, ge=1, le=8, description="Số worker song song (mỗi worker một truy vấn)."),
) -> dict[str, Any]:
    """
    Chạy bộ truy vấn discovery định sẵn (CafeF, Baomoi, TNCK, Fireant, v.v.) qua Firecrawl Search.
    Trả về `rss_url_candidates` gợi ý (theo heuristic URL); cần xác minh thủ công trước khi thêm vào registry.
    """
    _require_firecrawl()
    queries = _discovery_queries_for_preset(preset)
    if not queries:
        raise HTTPException(status_code=400, detail="No discovery queries for this preset.")

    results: list[dict[str, Any]] = []
    errors: dict[str, str] = {}

    workers = max(1, min(max_workers, len(queries)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                _run_single_firecrawl_query,
                item.id,
                item.query,
                item.country,
                per_query_limit,
            ): item.id
            for item in queries
        }
        for future in as_completed(future_map):
            qid = future_map[future]
            try:
                results.append(future.result())
            except Exception as exc:
                errors[qid] = f"{exc.__class__.__name__}: {exc}"

    results.sort(key=lambda row: row.get("id", ""))
    merged_rss: list[str] = []
    for row in results:
        for url in row.get("rss_url_candidates") or []:
            merged_rss.append(url)

    return {
        "preset": preset,
        "per_query_limit": per_query_limit,
        "queries_run": len(queries),
        "results": results,
        "errors": errors,
        "rss_url_candidates_merged": sorted(set(merged_rss)),
        "note": "Kết quả phụ thuộc chỉ mục tìm kiếm; luôn kiểm tra URL (HTTP 200, XML hợp lệ) trước khi đưa vào NEWS_FEED_SOURCES.",
    }
