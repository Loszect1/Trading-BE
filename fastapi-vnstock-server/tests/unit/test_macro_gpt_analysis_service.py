from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from app.services import macro_gpt_analysis_service as service


def _gpt_payload() -> dict:
    return {
        "executive_summary": "Bối cảnh vĩ mô Việt Nam đang pha trộn nhưng vẫn có thể nghiên cứu.",
        "macro_regime_view": "Chế độ phục hồi nhưng còn khoảng trống dữ liệu.",
        "economics_view": "Tăng trưởng cải thiện trong khi lãi suất vẫn là ràng buộc.",
        "news_pressure": "Áp lực tin tức cân bằng.",
        "global_context": "USD và lãi suất Mỹ là điểm theo dõi bên ngoài chính.",
        "market_implications": "Chỉ dùng làm bối cảnh nghiên cứu; theo dõi thanh khoản và dòng tiền.",
        "bullish_drivers": ["Phục hồi tín dụng", "Đầu tư công"],
        "bearish_drivers": ["Áp lực tỷ giá"],
        "sector_implications": [{"sector": "Ngân hàng", "impact": "trung tính", "reason": "Tín dụng hỗ trợ nhưng NIM còn rủi ro."}],
        "risk_watchlist": ["Biến động tỷ giá"],
        "data_gaps": ["Độ mới của dòng vốn ngoại"],
        "confidence": 68,
        "disclaimer": "Chỉ dùng làm bối cảnh nghiên cứu. Không phải khuyến nghị đầu tư hoặc tín hiệu đặt lệnh.",
    }


class FakeCache:
    def __init__(self, cached: dict | None = None) -> None:
        self.cached = cached
        self.writes: list[tuple[str, dict, int]] = []

    def get_json(self, key: str) -> dict | None:
        return self.cached

    def set_json(self, key: str, value: dict, ttl_seconds: int) -> None:
        self.writes.append((key, value, ttl_seconds))


def test_macro_gpt_analysis_uses_container_gpt(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    fake_cache = FakeCache()
    monkeypatch.setattr(service.settings, "use_gpt", True)
    monkeypatch.setattr(service.settings, "gpt_model", "gpt-test")
    monkeypatch.setattr(service.settings, "gpt_max_tokens", 2048)
    monkeypatch.setattr(service.settings, "ai_gpt_macro_analysis_max_tokens", 1400)
    monkeypatch.setattr(service.settings, "short_term_scan_timezone", "Asia/Ho_Chi_Minh")
    monkeypatch.setattr(service, "macro_gpt_analysis_cache", fake_cache)
    monkeypatch.setattr(
        service,
        "_load_research_context",
        lambda **_kwargs: {
            "macro_regime": {"regime": "Recovery", "regime_score": 55},
            "approved_global_strategy_memory": [{"memory": {"title": "Balanced strategy", "allocation": {"cash": "25-30"}}}],
            "macro_observations": [{"metric_key": "gdp_growth_yoy", "value": 6.2}],
            "top_news_impacts": [{"title": "Credit growth improves", "impact_score": 80}],
            "morning_brief_counts": {"article_count": 1, "impact_count": 1},
        },
    )

    def fake_generate_text_with_resilience(**kwargs):  # noqa: ANN003
        calls.append(kwargs)
        return json.dumps(_gpt_payload())

    monkeypatch.setattr(service.gpt_service, "generate_text_with_resilience", fake_generate_text_with_resilience)

    out = service.analyze_macro_news_economics_with_gpt(language="en")

    assert out["success"] is True
    assert out["analysis_source"] == "gpt_in_be_container"
    assert out["model"] == "gpt-test"
    assert out["language"] == "vi"
    assert out["cached"] is False
    assert out["cache_expires_at"]
    assert out["analysis"]["executive_summary"]
    assert calls
    assert calls[0]["output_schema"] == service.MACRO_GPT_ANALYSIS_SCHEMA
    assert calls[0]["cache_ttl_seconds"] == fake_cache.writes[0][2]
    assert "Viết toàn bộ nội dung phân tích bằng tiếng Việt" in calls[0]["prompt"]
    assert "approved_global_strategy_memory" in calls[0]["prompt"]
    assert "not an execution signal" in calls[0]["prompt"]
    assert "Research context JSON" in calls[0]["prompt"]
    assert out["context_summary"]["approved_global_strategy_memory_count"] == 1
    assert fake_cache.writes


def test_macro_gpt_analysis_reuses_daily_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service.settings, "use_gpt", True)
    monkeypatch.setattr(service.settings, "gpt_model", "gpt-test")
    monkeypatch.setattr(service.settings, "short_term_scan_timezone", "Asia/Ho_Chi_Minh")
    monkeypatch.setattr(
        service,
        "macro_gpt_analysis_cache",
        FakeCache(
            {
                "success": True,
                "analysis_source": "gpt_in_be_container",
                "model": "gpt-test",
                "generated_at": "2026-06-04T00:30:00+00:00",
                "language": "vi",
                "cached": False,
                "analysis": _gpt_payload(),
                "context_summary": {},
            }
        ),
    )
    monkeypatch.setattr(service, "_load_research_context", lambda **_kwargs: pytest.fail("cache should avoid reload"))

    out = service.analyze_macro_news_economics_with_gpt()

    assert out["success"] is True
    assert out["language"] == "vi"
    assert out["cached"] is True
    assert out["cache_expires_at"]


def test_seconds_until_next_local_midnight() -> None:
    now = datetime(2026, 6, 4, 14, 30, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))

    assert service._seconds_until_next_local_midnight(now, "Asia/Ho_Chi_Minh") == 9 * 60 * 60 + 30 * 60


def test_macro_gpt_analysis_disabled_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service.settings, "use_gpt", False)

    with pytest.raises(RuntimeError, match="gpt_disabled"):
        service.analyze_macro_news_economics_with_gpt()
