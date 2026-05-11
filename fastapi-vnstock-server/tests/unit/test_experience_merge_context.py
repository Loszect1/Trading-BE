from datetime import datetime, timezone
from uuid import uuid4

from app.services.experience_service import (
    ExperienceRecord,
    _cmt_stoploss_playbook,
    _guess_root_cause,
    _heuristic_market_adaptation,
    _merge_experience_context,
)


def test_merge_experience_context_rollup() -> None:
    merged_ctx, total_pnl_value, avg_pnl_percent = _merge_experience_context(
        current_market_context={"trigger": "stoploss_hit"},
        previous_rows=[
            {
                "pnl_value": -1000.0,
                "pnl_percent": -1.5,
                "root_cause": "weak_volume_confirmation",
                "mistake_tags": ["stoploss_hit", "low_rr"],
                "market_context": {"trigger": "stoploss_hit"},
            },
            {
                "pnl_value": 500.0,
                "pnl_percent": 0.8,
                "root_cause": "execution_noise",
                "mistake_tags": ["unclassified"],
                "market_context": {"trigger": "take_profit_hit"},
            },
        ],
        current_pnl_value=-300.0,
        current_pnl_percent=-0.6,
    )
    assert round(total_pnl_value, 4) == -800.0
    assert round(avg_pnl_percent, 4) == -0.4333
    rollup = merged_ctx.get("experience_rollup") or {}
    assert rollup.get("samples") == 3
    assert rollup.get("stoploss_hits") == 2


def test_guess_root_cause_flags_late_overextended_entry() -> None:
    record = ExperienceRecord(
        id=uuid4(),
        trade_id="TST-1",
        account_mode="DEMO",
        symbol="AAA",
        strategy_type="SHORT_TERM",
        entry_time=datetime.now(tz=timezone.utc),
        exit_time=datetime.now(tz=timezone.utc),
        pnl_value=-1_000_000.0,
        pnl_percent=-3.0,
        market_context={
            "rsi14": 82,
            "distance_from_ema20_pct": 12,
            "volume_spike": 2.0,
            "rr_realized": 0.5,
        },
        root_cause="pending",
        mistake_tags=[],
        improvement_action="pending",
        confidence_after_review=70.0,
    )
    root_cause, tags, action = _guess_root_cause(record)
    assert root_cause in {"overextended_entry", "risk_reward_unfavorable", "late_entry_after_momentum"}
    assert "overbought_chase" in tags
    assert "overextended_from_ema20" in tags
    assert "stoploss_hit" in tags
    assert action


def test_heuristic_market_adaptation_tightens_after_stoploss_rollup() -> None:
    adaptation = _heuristic_market_adaptation(
        ["stoploss_hit", "weak_volume_confirmation", "overextended_from_ema20"],
        {"experience_rollup": {"samples": 4, "stoploss_hits": 3}},
    )
    assert adaptation["neutral"]["min_spike_ratio"] >= 2.2
    assert adaptation["neutral"]["max_distance_from_ema20_pct"] <= 5.0
    assert adaptation["risk_off"]["min_momentum_5d_pct"] >= adaptation["neutral"]["min_momentum_5d_pct"]


def test_cmt_stoploss_playbook_adds_next_buy_filters() -> None:
    record = ExperienceRecord(
        id=uuid4(),
        trade_id="TST-2",
        account_mode="DEMO",
        symbol="AAA",
        strategy_type="MAIL_SIGNAL",
        entry_time=datetime.now(tz=timezone.utc),
        exit_time=datetime.now(tz=timezone.utc),
        pnl_value=-1_000_000.0,
        pnl_percent=-5.5,
        market_context={
            "trigger": "stoploss_hit",
            "entry_price": 10_000,
            "exit_price": 9_400,
            "stoploss_price": 9_500,
            "take_profit_price": 11_500,
            "volume_spike": 1.0,
            "distance_from_ema20_pct": 9.0,
        },
        root_cause="pending",
        mistake_tags=[],
        improvement_action="pending",
        confidence_after_review=70.0,
    )
    playbook = _cmt_stoploss_playbook(record, ["stoploss_hit", "weak_volume_confirmation", "overextended_from_ema20"])
    filters = playbook["next_buy_filters"]
    assert playbook["framework"] == "CMT_price_volume_risk_review"
    assert filters["cooldown_days"] >= 5
    assert filters["min_volume_spike_ratio"] >= 2.2
    assert filters["max_distance_from_ema20_pct"] <= 5.0
    assert filters["position_size_multiplier"] <= 0.5
