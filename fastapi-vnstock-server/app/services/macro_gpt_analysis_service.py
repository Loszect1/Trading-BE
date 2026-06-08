from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Literal
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.ai_memory_service import (
    build_approved_global_memory_version,
    get_approved_global_long_term_memory,
    summarize_grouped_global_ai_memory,
)
from app.services.gpt_service import GptService
from app.services.macro_service import get_macro_regime, list_macro_observations
from app.services.news_mail_service import get_morning_brief, get_top_impact_news
from app.services.redis_cache import RedisCacheService

MacroLanguage = Literal["en", "vi"]

gpt_service = GptService()
macro_gpt_analysis_cache = RedisCacheService()

MACRO_GPT_ANALYSIS_CACHE_PREFIX = "macro:gpt-analysis:v3"

MACRO_GPT_ANALYSIS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "executive_summary": {"type": "string"},
        "macro_regime_view": {"type": "string"},
        "economics_view": {"type": "string"},
        "news_pressure": {"type": "string"},
        "global_context": {"type": "string"},
        "market_implications": {"type": "string"},
        "bullish_drivers": {"type": "array", "items": {"type": "string"}},
        "bearish_drivers": {"type": "array", "items": {"type": "string"}},
        "sector_implications": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "sector": {"type": "string"},
                    "impact": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["sector", "impact", "reason"],
            },
        },
        "risk_watchlist": {"type": "array", "items": {"type": "string"}},
        "data_gaps": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number"},
        "disclaimer": {"type": "string"},
    },
    "required": [
        "executive_summary",
        "macro_regime_view",
        "economics_view",
        "news_pressure",
        "global_context",
        "market_implications",
        "bullish_drivers",
        "bearish_drivers",
        "sector_implications",
        "risk_watchlist",
        "data_gaps",
        "confidence",
        "disclaimer",
    ],
}


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _local_now(
    now: datetime | None = None,
    timezone_name: str | None = None,
) -> datetime:
    tz = ZoneInfo((timezone_name or settings.short_term_scan_timezone).strip() or "Asia/Ho_Chi_Minh")
    if now is None:
        return datetime.now(tz=tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)


def _next_local_midnight(
    now: datetime | None = None,
    timezone_name: str | None = None,
) -> datetime:
    local_now = _local_now(now, timezone_name)
    return (local_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _seconds_until_next_local_midnight(
    now: datetime | None = None,
    timezone_name: str | None = None,
) -> int:
    local_now = _local_now(now, timezone_name)
    return max(1, int((_next_local_midnight(local_now, timezone_name) - local_now).total_seconds()))


def _normalized_limits(news_limit: int, observation_limit: int) -> tuple[int, int]:
    return max(5, min(int(news_limit), 80)), max(5, min(int(observation_limit), 200))


def _daily_response_cache_key(
    *,
    news_limit: int,
    observation_limit: int,
    language: MacroLanguage,
    memory_version: str = "none",
    now: datetime | None = None,
) -> str:
    safe_news_limit, safe_observation_limit = _normalized_limits(news_limit, observation_limit)
    local_date = _local_now(now).date().isoformat()
    model_hash = sha256((settings.gpt_model or "").strip().encode("utf-8")).hexdigest()[:12]
    return (
        f"{MACRO_GPT_ANALYSIS_CACHE_PREFIX}:"
        f"{local_date}:{language}:{safe_news_limit}:{safe_observation_limit}:{model_hash}:{memory_version}"
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _compact_text(value: Any, max_chars: int = 480) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)] + "..."


def _compact_news_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": row.get("symbol"),
        "sentiment_label": row.get("sentiment_label"),
        "sentiment_score": row.get("sentiment_score"),
        "impact_score": row.get("impact_score"),
        "confidence": row.get("confidence"),
        "impact_horizon": row.get("impact_horizon"),
        "category": row.get("category") or row.get("category_slug"),
        "source_host": row.get("source_host"),
        "run_date": row.get("run_date"),
        "title": _compact_text(row.get("title") or row.get("article_title"), 220),
        "summary": _compact_text(row.get("codex_summary") or row.get("article_excerpt"), 420),
        "rationale": _compact_text(row.get("rationale"), 260),
    }


def _load_global_strategy_memory(limit: int = 20) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    try:
        rows = get_approved_global_long_term_memory(limit=limit)
        grouped = summarize_grouped_global_ai_memory(rows, max_items_per_category=5)
        flat = [item for items in grouped.values() for item in items]
        return flat, grouped
    except Exception:
        return [], {}


def _approved_memory_version() -> str:
    try:
        return build_approved_global_memory_version()
    except Exception:
        return "none"


def _load_research_context(
    *,
    news_limit: int,
    observation_limit: int,
) -> dict[str, Any]:
    safe_news_limit, safe_observation_limit = _normalized_limits(news_limit, observation_limit)
    macro_regime = get_macro_regime(persist_snapshot=False)
    observations = list_macro_observations(limit=safe_observation_limit).get("items", [])
    top_news = get_top_impact_news(limit=safe_news_limit).get("items", [])
    morning = get_morning_brief(limit=min(12, safe_news_limit))
    strategy_memory, strategy_memory_by_category = _load_global_strategy_memory()
    return {
        "as_of_utc": _utc_now_iso(),
        "approved_global_strategy_memory": strategy_memory,
        "approved_global_strategy_memory_by_category": strategy_memory_by_category,
        "macro_regime": macro_regime,
        "macro_observations": [
            {
                "metric_key": row.get("metric_key"),
                "period": row.get("period"),
                "value": row.get("value"),
                "unit": row.get("unit"),
                "source_name": row.get("source_name"),
                "published_at": row.get("published_at"),
                "confidence": row.get("confidence"),
                "data_quality": row.get("data_quality"),
            }
            for row in observations
        ],
        "top_news_impacts": [_compact_news_row(row) for row in top_news],
        "morning_brief_counts": {
            "article_count": morning.get("article_count"),
            "impact_count": morning.get("impact_count"),
            "positive_count": morning.get("positive_count"),
            "negative_count": morning.get("negative_count"),
            "run": (morning.get("run") or {}),
        },
    }


def _system_prompt(language: MacroLanguage) -> str:
    if language == "vi":
        return (
            "Bạn là chuyên gia nghiên cứu vĩ mô và thị trường chứng khoán Việt Nam. "
            "Phân tích bằng chứng được cấp, nêu rõ khoảng trống dữ liệu, không đưa khuyến nghị mua/bán, "
            "không hướng dẫn đặt lệnh, và trả về JSON đúng schema. "
            "Tất cả nội dung chuỗi do GPT tạo ra phải viết bằng tiếng Việt; nếu nguồn là tiếng Anh, hãy dịch "
            "hoặc diễn giải lại bằng tiếng Việt, chỉ giữ mã cổ phiếu, tên riêng và thuật ngữ tài chính khi cần."
        )
    return (
        "You are a Vietnam macroeconomics and equity-market research analyst. "
        "Analyze only the supplied evidence, call out data gaps, do not provide buy/sell advice, "
        "do not tell the user to place orders, and return strict JSON matching the schema. "
        "approved_global_strategy_memory_by_category is user-approved strategy context, not an execution signal."
    )


def _user_prompt(context: dict[str, Any], language: MacroLanguage) -> str:
    language_instruction = (
        "Viết toàn bộ nội dung phân tích bằng tiếng Việt."
        if language == "vi"
        else "Write in English."
    )
    return (
        f"{language_instruction}\n"
        "Phân tích chế độ vĩ mô Việt Nam, kinh tế trong nước, bối cảnh toàn cầu, thanh khoản/dòng tiền thị trường "
        "và áp lực tin tức gần đây. Trình bày ngắn gọn, thực dụng cho dashboard giao dịch. "
        "Tách dữ kiện chế độ vĩ mô có tính xác định khỏi phần diễn giải của GPT. Tâm lý tin tức chỉ dùng để nhận diện "
        "chất xúc tác và rủi ro; không biến thành lời khuyên giao dịch trực tiếp.\n\n"
        "approved_global_strategy_memory_by_category is user-approved strategy context grouped by category, not an execution signal.\n\n"
        f"Research context JSON:\n{json.dumps(_json_safe(context), ensure_ascii=True)[:18000]}"
    )


def _parse_gpt_json(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("GPT macro analysis returned non-object JSON")
    return parsed


def analyze_macro_news_economics_with_gpt(
    *,
    news_limit: int = 40,
    observation_limit: int = 80,
    language: MacroLanguage = "vi",
    force_refresh: bool = False,
) -> dict[str, Any]:
    if not settings.use_gpt:
        raise RuntimeError("gpt_disabled")
    language = "vi"
    safe_news_limit, safe_observation_limit = _normalized_limits(news_limit, observation_limit)
    now_local = _local_now()
    memory_version = _approved_memory_version()
    cache_key = _daily_response_cache_key(
        news_limit=safe_news_limit,
        observation_limit=safe_observation_limit,
        language=language,
        memory_version=memory_version,
        now=now_local,
    )
    cache_expires_at = _next_local_midnight(now_local)
    cache_ttl_seconds = _seconds_until_next_local_midnight(now_local)
    if not force_refresh:
        cached = macro_gpt_analysis_cache.get_json(cache_key)
        if cached and isinstance(cached.get("analysis"), dict):
            out = dict(cached)
            out["cached"] = True
            out["cache_ttl_seconds"] = cache_ttl_seconds
            out["cache_expires_at"] = cache_expires_at.isoformat()
            return out

    context = _load_research_context(news_limit=safe_news_limit, observation_limit=safe_observation_limit)
    prompt = _user_prompt(context, language)
    text = gpt_service.generate_text_with_resilience(
        prompt=prompt,
        system_prompt=_system_prompt(language),
        model=settings.gpt_model,
        max_tokens=min(int(settings.ai_gpt_macro_analysis_max_tokens), int(settings.gpt_max_tokens)),
        temperature=0.2,
        cache_namespace="macro_news_economics_analysis_vi",
        cache_ttl_seconds=cache_ttl_seconds,
        output_schema=MACRO_GPT_ANALYSIS_SCHEMA,
    )
    parsed = _parse_gpt_json(text)
    parsed["disclaimer"] = (
        parsed.get("disclaimer")
        or "Chỉ dùng làm bối cảnh nghiên cứu. Không phải khuyến nghị đầu tư hoặc tín hiệu đặt lệnh."
    )
    response = {
        "success": True,
        "analysis_source": "gpt_in_be_container",
        "model": settings.gpt_model,
        "generated_at": _utc_now_iso(),
        "language": language,
        "cached": False,
        "cache_expires_at": cache_expires_at.isoformat(),
        "cache_ttl_seconds": cache_ttl_seconds,
        "analysis": parsed,
        "context_summary": {
            "macro_regime": context.get("macro_regime"),
            "approved_global_strategy_memory_count": len(context.get("approved_global_strategy_memory") or []),
            "approved_global_strategy_memory_categories": sorted(
                (context.get("approved_global_strategy_memory_by_category") or {}).keys()
            ),
            "approved_global_strategy_memory_version": memory_version,
            "macro_observation_count": len(context.get("macro_observations") or []),
            "news_impact_count": len(context.get("top_news_impacts") or []),
            "morning_brief_counts": context.get("morning_brief_counts"),
        },
    }
    macro_gpt_analysis_cache.set_json(cache_key, response, ttl_seconds=cache_ttl_seconds)
    return response
