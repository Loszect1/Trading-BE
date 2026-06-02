from __future__ import annotations

from app.services.mail_signal_scheduler_service import _apply_negative_news_adjustment_to_qty
from app.services.news_mail_service import _news_event_outcome


def test_news_event_outcome_confirms_positive_sentiment_with_positive_abnormal_return() -> None:
    label, score = _news_event_outcome(
        sentiment_label="positive",
        sentiment_score=40.0,
        return_pct=2.5,
        abnormal_return_pct=1.8,
        volume_change_pct=30.0,
    )

    assert label == "sentiment_confirmed"
    assert score is not None and score > 50.0


def test_news_event_outcome_confirms_negative_news_as_avoidance_signal() -> None:
    label, score = _news_event_outcome(
        sentiment_label="negative",
        sentiment_score=-45.0,
        return_pct=-2.5,
        abnormal_return_pct=-1.6,
        volume_change_pct=20.0,
    )

    assert label == "avoidance_confirmed"
    assert score is not None and score > 50.0


def test_mail_signal_negative_news_adjustment_reduces_board_lot_quantity() -> None:
    qty = _apply_negative_news_adjustment_to_qty(
        500,
        {"applied": True, "adjustment": -8.0},
    )

    assert qty == 300
    assert qty % 100 == 0


def test_mail_signal_positive_news_adjustment_does_not_exceed_risk_quantity() -> None:
    qty = _apply_negative_news_adjustment_to_qty(
        500,
        {"applied": True, "adjustment": 8.0},
    )

    assert qty == 500
