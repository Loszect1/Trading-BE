from app.services.experience_service import _merge_experience_context


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
