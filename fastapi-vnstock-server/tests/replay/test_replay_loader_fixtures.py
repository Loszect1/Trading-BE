from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.replay.replay_loader import load_replay_timeline, normalize_sanitized_trade_log_v1
from tests.replay.replay_runner import run_trading_timeline_steps

_FIX = Path(__file__).resolve().parent.parent / "fixtures" / "replay"


def test_load_minimal_ndjson_step_stream():
    tl = load_replay_timeline(_FIX / "minimal_timeline.ndjson")
    assert tl["symbol"] == "TSTMIN"
    assert len(tl["steps"]) == 1
    assert tl["steps"][0]["op"] == "set_clock"


def test_sanitized_log_normalization_matches_legacy_ops_shape():
    legacy = load_replay_timeline(_FIX / "t_plus_2_settlement_timeline.json")
    sanitized = load_replay_timeline(_FIX / "sanitized_trade_log_v1.json")
    assert [s["op"] for s in legacy["steps"]] == [s["op"] for s in sanitized["steps"]]


def test_normalize_rejects_unknown_kind():
    with pytest.raises(ValueError, match="unknown sanitized log kind"):
        normalize_sanitized_trade_log_v1([{"kind": "not_a_real_kind"}])


def test_normalize_sanitized_trade_log_extensions_ops_shape() -> None:
    tl = normalize_sanitized_trade_log_v1(
        [
            {"kind": "meta", "symbol": "TSTNORM", "account_mode": "DEMO"},
            {"kind": "clock", "ts": "2026-01-05T10:00:00Z"},
            {
                "kind": "cancel_order",
                "account_mode": "DEMO",
                "idempotency_key": "x",
                "reason": "r",
                "expect": {"cancelled": False},
            },
            {"kind": "reconcile_order", "account_mode": "DEMO", "order_id": "00000000-0000-0000-0000-000000000001", "expect": {"reconciled": False}},
            {
                "kind": "risk_event",
                "account_mode": "DEMO",
                "event_type": "T",
                "payload": {},
                "expect": {"event_type": "T"},
            },
            {"kind": "broker_snapshot", "account_mode": "DEMO", "expect": {"contains_keys": ["portfolio"]}},
        ]
    )
    assert tl["symbol"] == "TSTNORM"
    assert [s["op"] for s in tl["steps"]] == [
        "set_clock",
        "cancel_order",
        "reconcile_order",
        "risk_event",
        "broker_snapshot",
    ]


def test_load_raw_extra_capture_v1_and_v2_same_ops() -> None:
    tl1 = load_replay_timeline(_FIX / "raw_production_trade_log_v1_extra_capture.ndjson")
    tl2 = load_replay_timeline(_FIX / "raw_production_trade_log_v2_extra_capture.ndjson")
    assert [s["op"] for s in tl1["steps"]] == [s["op"] for s in tl2["steps"]]
    b1 = load_replay_timeline(_FIX / "raw_production_trade_log_v1_extra_capture.bundle.json")
    b2 = load_replay_timeline(_FIX / "raw_production_trade_log_v2_extra_capture.bundle.json")
    assert [s["op"] for s in b1["steps"]] == [s["op"] for s in b2["steps"]]


def test_load_replay_timeline_rejects_unsupported_json_document(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"note": "not a replay document"}), encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported replay document"):
        load_replay_timeline(p)


@pytest.mark.postgres
def test_replay_from_sanitized_trade_log_v1(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "sanitized_trade_log_v1.json")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)
