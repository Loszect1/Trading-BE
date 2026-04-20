from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.replay.replay_loader import load_replay_timeline
from tests.replay.replay_runner import load_ndjson_steps
from tests.replay.replay_schema_registry import (
    RAW_PRODUCTION_TRADE_LOG_V0_COMPAT,
    RAW_PRODUCTION_TRADE_LOG_V1,
    RAW_PRODUCTION_TRADE_LOG_V2,
    canonical_raw_body_record_type,
    canonicalize_raw_lines,
    migrate_raw_v0_compat_to_v1,
    migrate_raw_v2_to_v1,
    validate_canonical_raw_v1_record_contracts,
    validate_raw_bundle_document,
)

_FIX = Path(__file__).resolve().parent.parent / "fixtures" / "replay"


def test_canonical_raw_body_record_type_aliases() -> None:
    assert canonical_raw_body_record_type({"record_type": "order.cancel"}) == "cancel_order"
    assert canonical_raw_body_record_type({"record_type": "order.reconcile"}) == "reconcile_order"
    assert canonical_raw_body_record_type({"record_type": "risk.log"}) == "risk_event"
    assert canonical_raw_body_record_type({"record_type": "monitoring.snapshot"}) == "broker_snapshot"


def test_validate_contracts_cancel_order_requires_target() -> None:
    lines = [
        {"format_version": RAW_PRODUCTION_TRADE_LOG_V1, "role": "header", "default_symbol": "Z"},
        {"record_type": "cancel_order", "expect": {"cancelled": False}},
    ]
    with pytest.raises(ValueError, match="order_id or idempotency_key"):
        validate_canonical_raw_v1_record_contracts(lines, context="test")


def test_validate_contracts_risk_event_requires_event_type() -> None:
    lines = [
        {"format_version": RAW_PRODUCTION_TRADE_LOG_V1, "role": "header", "default_symbol": "Z"},
        {"record_type": "risk_event", "expect": {}, "account_mode": "DEMO"},
    ]
    with pytest.raises(ValueError, match="event_type"):
        validate_canonical_raw_v1_record_contracts(lines, context="test")


def test_migrate_v2_strips_annotations_and_event_alias() -> None:
    lines = migrate_raw_v2_to_v1(
        [
            {
                "format_version": RAW_PRODUCTION_TRADE_LOG_V2,
                "role": "header",
                "default_symbol": "MV2",
                "deploy_schema": "x",
                "schema_minor": 2,
            },
            {"record_type": "capture_marker", "tag": "t"},
            {"event": "clock", "ts": "2026-01-01T00:00:00Z"},
            {"record_type": "trace_context", "trace_id": "z"},
        ]
    )
    assert lines[0]["format_version"] == RAW_PRODUCTION_TRADE_LOG_V1
    assert "deploy_schema" not in lines[0]
    assert len(lines) == 2
    assert lines[1]["record_type"] == "clock"


def test_canonicalize_v2_fixture_matches_v1_ops_shape() -> None:
    v1 = load_ndjson_steps(_FIX / "raw_production_trade_log_v1.ndjson")
    v2 = load_ndjson_steps(_FIX / "raw_production_trade_log_v2.ndjson")
    c1 = canonicalize_raw_lines(v1, context="v1")
    c2 = canonicalize_raw_lines(v2, context="v2")
    assert c1 == c2


def test_migrate_v2_extra_capture_aliases_to_canonical_v1_rows() -> None:
    lines = migrate_raw_v2_to_v1(load_ndjson_steps(_FIX / "raw_production_trade_log_v2_extra_capture.ndjson"))
    assert lines[0]["format_version"] == RAW_PRODUCTION_TRADE_LOG_V1
    kinds = [canonical_raw_body_record_type(r) for r in lines[1:]]
    assert kinds == [
        "clock",
        "place_order",
        "cancel_order",
        "place_order",
        "reconcile_order",
        "cancel_order",
        "risk_event",
        "broker_snapshot",
    ]


def test_validate_contracts_rejects_unknown_body_record_type() -> None:
    lines = [
        {"format_version": RAW_PRODUCTION_TRADE_LOG_V1, "role": "header", "default_symbol": "Z"},
        {"record_type": "weird", "expect": {}, "order": {"symbol": "Z", "side": "BUY"}},
    ]
    with pytest.raises(ValueError, match="unsupported record_type"):
        validate_canonical_raw_v1_record_contracts(lines, context="test")


def test_validate_contracts_rejects_clock_without_ts() -> None:
    lines = [
        {"format_version": RAW_PRODUCTION_TRADE_LOG_V1, "role": "header", "default_symbol": "Z"},
        {"record_type": "clock"},
    ]
    with pytest.raises(ValueError, match="iso_utc"):
        validate_canonical_raw_v1_record_contracts(lines, context="test")


def test_migrate_v0_compat_rewrites_type_to_record_type() -> None:
    lines = migrate_raw_v0_compat_to_v1(
        [
            {
                "format_version": RAW_PRODUCTION_TRADE_LOG_V0_COMPAT,
                "role": "header",
                "default_symbol": "MIG",
                "account_mode": "DEMO",
            },
            {"type": "clock", "ts": "2026-01-01T00:00:00Z"},
        ]
    )
    assert lines[0]["format_version"] == RAW_PRODUCTION_TRADE_LOG_V1
    assert lines[1]["record_type"] == "clock"
    assert "type" not in lines[1]


def test_canonicalize_accepts_v1_fixture_shape() -> None:
    lines = load_ndjson_steps(_FIX / "raw_production_trade_log_v1.ndjson")
    out = canonicalize_raw_lines(lines, context="fixture")
    assert out[0]["format_version"] == RAW_PRODUCTION_TRADE_LOG_V1


def test_canonicalize_unknown_version_raises() -> None:
    with pytest.raises(ValueError, match="unsupported raw production replay format_version"):
        canonicalize_raw_lines(
            [{"format_version": "raw_production_trade_log_v99", "role": "header"}],
            context="test",
        )


def test_validate_bundle_inner_version_mismatch_raises(tmp_path: Path) -> None:
    doc = {
        "format_version": RAW_PRODUCTION_TRADE_LOG_V1,
        "records": [
            {
                "format_version": RAW_PRODUCTION_TRADE_LOG_V0_COMPAT,
                "role": "header",
                "default_symbol": "X",
            },
        ],
    }
    with pytest.raises(ValueError, match="inner header format_version"):
        validate_raw_bundle_document(doc, path_hint="synthetic")


def test_load_replay_unknown_raw_bundle_version(tmp_path: Path) -> None:
    p = tmp_path / "bad_raw.json"
    p.write_text(
        json.dumps(
            {
                "format_version": "raw_production_trade_log_v42",
                "records": [{"role": "header", "default_symbol": "Z"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsupported raw production replay bundle"):
        load_replay_timeline(p)


def test_load_replay_raw_bundle_requires_non_empty_records(tmp_path: Path) -> None:
    p = tmp_path / "empty_raw.json"
    p.write_text(
        json.dumps({"format_version": RAW_PRODUCTION_TRADE_LOG_V1, "records": []}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="empty"):
        load_replay_timeline(p)


def test_load_replay_ndjson_unknown_raw_header(tmp_path: Path) -> None:
    p = tmp_path / "bad.ndjson"
    p.write_text(
        json.dumps({"format_version": "raw_production_trade_log_v77", "role": "header"})
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsupported raw production NDJSON format_version"):
        load_replay_timeline(p)


def test_ndjson_raw_v1_requires_header_first_line(tmp_path: Path) -> None:
    p = tmp_path / "no_header.ndjson"
    p.write_text(
        json.dumps(
            {
                "format_version": RAW_PRODUCTION_TRADE_LOG_V1,
                "record_type": "clock",
                "ts": "2026-01-01T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="first record to be a header"):
        load_replay_timeline(p)


def test_migration_end_to_end_v0_ndjson(tmp_path: Path) -> None:
    p = tmp_path / "v0.ndjson"
    p.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "format_version": RAW_PRODUCTION_TRADE_LOG_V0_COMPAT,
                        "role": "header",
                        "default_symbol": "MV0",
                        "account_mode": "DEMO",
                    }
                ),
                json.dumps(
                    {
                        "type": "clock",
                        "ts": "2026-01-02T10:00:00Z",
                    }
                ),
                json.dumps(
                    {
                        "type": "place_order",
                        "order": {
                            "account_mode": "DEMO",
                            "symbol": "MV0",
                            "side": "BUY",
                            "quantity": 1,
                            "price": 1000.0,
                            "idempotency_key": "mig-v0-buy",
                            "auto_process": True,
                        },
                        "expect": {"status": "FILLED"},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    tl = load_replay_timeline(p)
    assert [s["op"] for s in tl["steps"]][:2] == ["set_clock", "place_order"]
