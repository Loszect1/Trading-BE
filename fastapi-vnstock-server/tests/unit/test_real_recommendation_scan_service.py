from __future__ import annotations

from app.services import real_recommendation_scan_service as service


def test_run_real_recommendations_scan_uses_resilient_scan_and_returns_watch(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(service, "run_mail_signal_pipeline", lambda: {"success": True, "mail_count": 0, "items": []})
    monkeypatch.setattr(
        service,
        "build_recommendation_from_signal",
        lambda _signal: {
            "symbol": "AAA",
            "entry": 100.0,
            "take_profit": 108.0,
            "stop_loss": 96.0,
            "confidence": 75.0,
            "reason": "fixture",
        },
    )
    def fake_resilient_scan(**kwargs):  # noqa: ANN003
        calls.append(kwargs)
        return {
            "scan_finished_at": "2026-05-18T10:15:00+07:00",
            "scanned": 2,
            "signals": [{"symbol": "AAA", "action": "BUY"}],
            "rejected_candidates": [
                {
                    "symbol": "ACB",
                    "exchange": "HOSE",
                    "reason": "entry_gate_failed",
                    "detail": {"rsi14": 55.5, "momentum_5d_pct": 1.2},
                }
            ],
        }

    monkeypatch.setattr(service, "run_short_term_scan_batch_resilient", fake_resilient_scan)

    payload = service.run_real_recommendations_scan(
        exchange_scope="ALL",
        limit_symbols=0,
        available_cash_vnd=None,
        persist=False,
    )

    assert calls
    assert payload["short_term_count"] == 1
    assert payload["short_term_recommendations"][0]["risk_status"] == "RECOMMENDATION_ONLY"
    assert payload["short_term_recommendations"][0]["risk_reason"] == "cash_preflight_skipped"
    assert payload["short_term_recommendations"][0]["suggested_quantity"] == 0
    assert payload["short_term_recommendations"][0]["reward_risk"] == 2.0
    assert payload["watch_count"] == 1
    assert payload["watch_candidates"][0]["symbol"] == "ACB"
    assert payload["watch_candidates"][0]["scan_reason"] == "entry_gate_failed"
