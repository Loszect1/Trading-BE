from __future__ import annotations

import json


def test_gpt_scoring_analysis_uses_schema_and_clamps_adjustment(monkeypatch) -> None:
    import app.services.signal_engine_service as svc

    captured: dict[str, object] = {}
    monkeypatch.setattr(svc.settings, "use_gpt", True)

    def fake_generate_text_with_resilience(**kwargs):
        captured.update(kwargs)
        return json.dumps(
            {
                "score_commentary": "Setup tot nhung can quan tri rui ro.",
                "risk_notes": ["Thanh khoan can xac nhan."],
                "confidence_adjustment": 99,
            }
        )

    monkeypatch.setattr(svc._gpt_service, "generate_text_with_resilience", fake_generate_text_with_resilience)

    out = svc._gpt_scoring_analysis(
        strategy_type="SHORT_TERM",
        symbol="AAA",
        action="BUY",
        reason="test",
        metadata={"signal_quality": {"composite_score_0_100": 80}},
    )

    assert out == {
        "score_commentary": "Setup tot nhung can quan tri rui ro.",
        "risk_notes": ["Thanh khoan can xac nhan."],
        "confidence_adjustment": 15.0,
    }
    assert captured["output_schema"] == svc._GPT_SIGNAL_SCORING_OUTPUT_SCHEMA
    assert captured["cache_namespace"] == "signal-scoring:SHORT_TERM"


def test_gpt_scoring_analysis_disabled_returns_none(monkeypatch) -> None:
    import app.services.signal_engine_service as svc

    monkeypatch.setattr(svc.settings, "use_gpt", False)
    out = svc._gpt_scoring_analysis(
        strategy_type="SHORT_TERM",
        symbol="AAA",
        action="BUY",
        reason="test",
        metadata={},
    )

    assert out is None
