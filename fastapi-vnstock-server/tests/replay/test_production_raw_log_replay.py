from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.replay.production_log_redact import redact_sensitive_fields
from tests.replay.production_raw_log import normalize_raw_production_trade_log_v1
from tests.replay.replay_loader import load_replay_timeline
from tests.replay.replay_runner import load_ndjson_steps, run_trading_timeline_steps

_FIX = Path(__file__).resolve().parent.parent / "fixtures" / "replay"


def test_raw_ndjson_ops_match_legacy_timeline():
    legacy = load_replay_timeline(_FIX / "t_plus_2_settlement_timeline.json")
    raw_tl = load_replay_timeline(_FIX / "raw_production_trade_log_v1.ndjson")
    assert [s["op"] for s in legacy["steps"]] == [s["op"] for s in raw_tl["steps"]]


def test_raw_bundle_json_loads_via_loader():
    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v1.bundle.json")
    assert [s["op"] for s in tl["steps"]][:4] == [
        "set_clock",
        "place_order",
        "check_settlement",
    ]


def test_raw_v2_ndjson_normalizes_same_ops_as_v1():
    tl_v1 = load_replay_timeline(_FIX / "raw_production_trade_log_v1.ndjson")
    tl_v2 = load_replay_timeline(_FIX / "raw_production_trade_log_v2.ndjson")
    assert [s["op"] for s in tl_v1["steps"]] == [s["op"] for s in tl_v2["steps"]]


def test_raw_v2_bundle_json_loads_via_loader():
    tl_v1 = load_replay_timeline(_FIX / "raw_production_trade_log_v1.bundle.json")
    tl_v2 = load_replay_timeline(_FIX / "raw_production_trade_log_v2.bundle.json")
    assert [s["op"] for s in tl_v1["steps"]] == [s["op"] for s in tl_v2["steps"]]


def test_normalize_raw_empty_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        normalize_raw_production_trade_log_v1([])


def test_normalize_raw_missing_record_type_raises() -> None:
    with pytest.raises(ValueError, match="missing record_type"):
        normalize_raw_production_trade_log_v1([{"expect": {}, "order": {}}])


def test_normalize_raw_unknown_record_type_raises() -> None:
    with pytest.raises(ValueError, match="unknown raw production record_type"):
        normalize_raw_production_trade_log_v1(
            [{"record_type": "weird", "expect": {}, "order": {"symbol": "X", "side": "BUY"}}]
        )


def test_normalize_raw_place_order_missing_expect_raises() -> None:
    with pytest.raises(ValueError, match="requires expect"):
        normalize_raw_production_trade_log_v1(
            [
                {
                    "format_version": "raw_production_trade_log_v1",
                    "role": "header",
                    "default_symbol": "X",
                },
                {
                    "record_type": "place_order",
                    "order": {"symbol": "X", "side": "BUY", "quantity": 1},
                },
            ]
        )


def test_normalize_raw_check_settlement_missing_quantity_raises() -> None:
    with pytest.raises(ValueError, match="requires quantity"):
        normalize_raw_production_trade_log_v1(
            [
                {
                    "format_version": "raw_production_trade_log_v1",
                    "role": "header",
                    "default_symbol": "X",
                },
                {"record_type": "check_settlement", "expect": {"pass": True, "available_qty": 1}},
            ]
        )


def test_normalize_raw_redacts_before_mapping():
    lines = load_ndjson_steps(_FIX / "raw_production_trade_log_v1.ndjson")
    scrubbed = redact_sensitive_fields(lines)
    blob = json.dumps(scrubbed)
    assert "live-secret-token" not in blob
    assert "ultra-secret" not in blob
    assert "nested-secret" not in blob
    assert "supersecret" not in blob
    tl = normalize_raw_production_trade_log_v1(lines)
    assert "TSTRAW" in json.dumps(tl)


@pytest.mark.postgres
def test_replay_parse_normalize_run_raw_ndjson(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v1.ndjson")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)


@pytest.mark.postgres
def test_replay_parse_normalize_run_raw_v2_ndjson(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v2.ndjson")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)


@pytest.mark.postgres
def test_replay_raw_bundle_json(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v1.bundle.json")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)


@pytest.mark.postgres
def test_replay_raw_extra_capture_ndjson(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v1_extra_capture.ndjson")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)


@pytest.mark.postgres
def test_replay_raw_extra_capture_v2_ndjson(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v2_extra_capture.ndjson")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)


@pytest.mark.postgres
def test_replay_raw_extra_capture_bundle_json(monkeypatch: pytest.MonkeyPatch, trading_db: str):
    import app.services.trading_core_service as tcs

    tl = load_replay_timeline(_FIX / "raw_production_trade_log_v1_extra_capture.bundle.json")
    run_trading_timeline_steps(monkeypatch=monkeypatch, timeline=tl, tcs_module=tcs)
