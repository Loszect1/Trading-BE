from __future__ import annotations

from app.services.macro_service import compute_macro_regime_from_observations


def _obs(metric_key: str, value: float, source_name: str = "SBV") -> dict:
    return {
        "metric_key": metric_key,
        "period": "2026Q1",
        "value": value,
        "unit": "%",
        "source_name": source_name,
        "confidence": 90,
        "data_quality": "official",
    }


def test_macro_regime_expansion_fixture() -> None:
    out = compute_macro_regime_from_observations(
        [
            _obs("gdp_growth_yoy", 7.2, "NSO/GSO"),
            _obs("cpi_yoy", 3.4, "NSO/GSO"),
            _obs("policy_rate", 4.5, "SBV"),
            _obs("credit_growth_yoy", 14.5, "SBV"),
            _obs("foreign_net_flow_vnd", 4200, "HOSE"),
            _obs("margin_pressure", 25, "SSC"),
        ]
    )

    assert out["regime"] == "Expansion"
    assert out["components"]["growth"] >= 65
    assert out["components"]["stress_risk"] < 50


def test_macro_regime_recovery_fixture() -> None:
    out = compute_macro_regime_from_observations(
        [
            _obs("gdp_growth_yoy", 4.5, "NSO/GSO"),
            _obs("cpi_yoy", 3.8, "NSO/GSO"),
            _obs("credit_growth_yoy", 9.0, "SBV"),
            _obs("foreign_net_flow_vnd", -500, "HOSE"),
            _obs("margin_pressure", 38, "SSC"),
        ]
    )

    assert out["regime"] == "Recovery"
    assert 35 <= out["regime_score"] <= 70


def test_macro_regime_overheated_fixture() -> None:
    out = compute_macro_regime_from_observations(
        [
            _obs("gdp_growth_yoy", 7.5, "NSO/GSO"),
            _obs("cpi_yoy", 6.2, "NSO/GSO"),
            _obs("policy_rate", 7.5, "SBV"),
            _obs("credit_growth_yoy", 16.0, "SBV"),
            _obs("vnindex_pe", 28.0, "HOSE"),
            _obs("margin_pressure", 58.0, "SSC"),
        ]
    )

    assert out["regime"] == "Overheated"
    assert out["components"]["growth"] >= 62
    assert out["components"]["stress_risk"] >= 55


def test_macro_regime_stress_fixture() -> None:
    out = compute_macro_regime_from_observations(
        [
            _obs("gdp_growth_yoy", 2.2, "NSO/GSO"),
            _obs("cpi_yoy", 7.5, "NSO/GSO"),
            _obs("usd_vnd_yoy", 7.0, "SBV"),
            _obs("foreign_net_flow_vnd", -8000, "HOSE"),
            _obs("margin_pressure", 85.0, "SSC"),
            _obs("default_stress", 72.0, "SSC"),
        ]
    )

    assert out["regime"] == "Stress"
    assert out["components"]["stress_risk"] >= 70
    assert "data_gaps" in out
