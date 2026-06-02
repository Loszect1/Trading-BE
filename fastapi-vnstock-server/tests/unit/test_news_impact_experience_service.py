from __future__ import annotations

from datetime import date

from app.services.news_impact_experience_service import (
    aggregate_news_impact_rows,
    apply_adjustment_to_signal_quality,
    impact_bucket_from_news_score,
    sentiment_bucket_from_news_score,
)


def _row(symbol: str, abnormal: float, *, label: str = "positive", impact: float = 82.0) -> dict:
    return {
        "symbol": symbol,
        "horizon_days": 3,
        "event_date": date(2026, 6, 1),
        "return_pct": abnormal,
        "abnormal_return_pct": abnormal,
        "volume_change_pct": 25.0,
        "sentiment_label": label,
        "sentiment_score": 35.0 if label == "positive" else -35.0,
        "impact_score": impact,
        "confidence": 80.0,
        "category_slug": "banking",
    }


def test_aggregate_news_impact_rows_positive_edge_increases_adjustment() -> None:
    rows = [_row("TSTNWS", v) for v in [1.2, 1.8, 2.1, 0.9, 1.5]]

    out = aggregate_news_impact_rows(rows)

    assert len(out) == 1
    assert out[0]["sample_count"] == 5
    assert out[0]["sentiment_bucket"] == "positive"
    assert out[0]["impact_bucket"] == "high"
    assert out[0]["score_adjustment"] > 0
    assert out[0]["score_adjustment"] <= 8


def test_aggregate_news_impact_rows_negative_edge_reduces_buy_adjustment() -> None:
    rows = [_row("TSTNWS", v, label="negative") for v in [-2.4, -1.8, -1.2, -0.9, -1.5]]

    out = aggregate_news_impact_rows(rows)

    assert len(out) == 1
    assert out[0]["sentiment_bucket"] == "negative"
    assert out[0]["loss_count"] == 5
    assert out[0]["score_adjustment"] < 0
    assert out[0]["score_adjustment"] >= -8


def test_apply_adjustment_to_signal_quality_caps_composite() -> None:
    metadata = {"signal_quality": {"composite_score_0_100": 97.0}}
    adjustment = {"applied": True, "adjustment": 8.0, "sample_count": 5}

    out = apply_adjustment_to_signal_quality(metadata, adjustment)

    assert out["signal_quality"]["composite_score_before_news_impact_experience"] == 97.0
    assert out["signal_quality"]["composite_score_0_100"] == 100.0
    assert out["news_impact_experience"]["adjustment"] == 8.0


def test_news_score_bucket_mapping_for_scanner() -> None:
    assert sentiment_bucket_from_news_score(71.0) == "positive"
    assert sentiment_bucket_from_news_score(34.0) == "negative"
    assert sentiment_bucket_from_news_score(50.0) == "neutral"
    assert impact_bucket_from_news_score(80.0) == "high"
    assert impact_bucket_from_news_score(62.0) == "medium"
    assert impact_bucket_from_news_score(53.0) == "low"
