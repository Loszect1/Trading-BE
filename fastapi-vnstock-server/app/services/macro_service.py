from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings

OFFICIAL_SOURCE_REGISTRY: list[dict[str, str]] = [
    {"name": "NSO/GSO", "scope": "Vietnam macro", "url": "https://www.nso.gov.vn/"},
    {"name": "SBV", "scope": "Rates, credit, FX", "url": "https://www.sbv.gov.vn/"},
    {"name": "MOF", "scope": "Fiscal policy, public investment", "url": "https://mof.gov.vn/"},
    {"name": "SSC", "scope": "Market supervision", "url": "https://ssc.gov.vn/"},
    {"name": "HOSE", "scope": "Market liquidity and exchange data", "url": "https://www.hsx.vn/"},
    {"name": "HNX", "scope": "Market liquidity and exchange data", "url": "https://www.hnx.vn/"},
    {"name": "VNX", "scope": "Exchange group data", "url": "https://vnx.com.vn/"},
    {"name": "World Bank", "scope": "Global macro", "url": "https://www.worldbank.org/"},
    {"name": "IMF", "scope": "Global macro", "url": "https://www.imf.org/"},
    {"name": "ADB", "scope": "Asia macro", "url": "https://www.adb.org/"},
    {"name": "FTSE Russell", "scope": "Market classification", "url": "https://www.lseg.com/en/ftse-russell"},
    {"name": "MSCI", "scope": "Market classification", "url": "https://www.msci.com/"},
]

MACRO_DISCLAIMER = "Research context only. This is not financial advice or an execution signal."

_TABLES_READY = False


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _clean_key(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")[:96]


def _clean_source(value: Any) -> str:
    return str(value or "").strip()[:240] or "Manual"


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def ensure_macro_research_tables() -> None:
    global _TABLES_READY
    if _TABLES_READY:
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS macro_observations (
        id UUID PRIMARY KEY,
        metric_key VARCHAR(96) NOT NULL,
        period VARCHAR(32) NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        unit VARCHAR(32) NOT NULL DEFAULT '',
        source_name TEXT NOT NULL,
        source_url TEXT NULL,
        published_at TIMESTAMPTZ NULL,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 75 CHECK (confidence >= 0 AND confidence <= 100),
        data_quality VARCHAR(32) NOT NULL DEFAULT 'manual',
        raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (metric_key, period, source_name)
    );
    CREATE INDEX IF NOT EXISTS idx_macro_observations_metric_period
    ON macro_observations(metric_key, period DESC);
    CREATE INDEX IF NOT EXISTS idx_macro_observations_published
    ON macro_observations(published_at DESC NULLS LAST);
    CREATE TABLE IF NOT EXISTS macro_regime_snapshots (
        id UUID PRIMARY KEY,
        as_of DATE NOT NULL,
        regime VARCHAR(32) NOT NULL,
        regime_score DOUBLE PRECISION NOT NULL CHECK (regime_score >= 0 AND regime_score <= 100),
        components JSONB NOT NULL DEFAULT '{}'::jsonb,
        drivers JSONB NOT NULL DEFAULT '[]'::jsonb,
        warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
        source_coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
        data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_macro_regime_snapshots_as_of
    ON macro_regime_snapshots(as_of DESC, created_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    _TABLES_READY = True


def upsert_macro_observation(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_macro_research_tables()
    metric_key = _clean_key(payload.get("metric_key"))
    if not metric_key:
        raise ValueError("metric_key is required")
    value = _to_float(payload.get("value"))
    if value is None:
        raise ValueError("value must be numeric")
    period = str(payload.get("period") or date.today().isoformat()).strip()[:32]
    confidence = _to_float(payload.get("confidence"))
    confidence = max(0.0, min(100.0, confidence if confidence is not None else 75.0))
    row_id = uuid4()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO macro_observations (
                    id, metric_key, period, value, unit, source_name, source_url,
                    published_at, confidence, data_quality, raw_metadata, created_at, updated_at
                ) VALUES (
                    %(id)s, %(metric_key)s, %(period)s, %(value)s, %(unit)s, %(source_name)s,
                    %(source_url)s, %(published_at)s, %(confidence)s, %(data_quality)s,
                    %(raw_metadata)s, NOW(), NOW()
                )
                ON CONFLICT (metric_key, period, source_name) DO UPDATE SET
                    value = EXCLUDED.value,
                    unit = EXCLUDED.unit,
                    source_url = EXCLUDED.source_url,
                    published_at = EXCLUDED.published_at,
                    confidence = EXCLUDED.confidence,
                    data_quality = EXCLUDED.data_quality,
                    raw_metadata = EXCLUDED.raw_metadata,
                    updated_at = NOW()
                RETURNING id::text AS id, metric_key, period, value, unit, source_name,
                          source_url, published_at, confidence, data_quality, raw_metadata,
                          created_at, updated_at
                """,
                {
                    "id": row_id,
                    "metric_key": metric_key,
                    "period": period,
                    "value": value,
                    "unit": str(payload.get("unit") or "").strip()[:32],
                    "source_name": _clean_source(payload.get("source_name")),
                    "source_url": str(payload.get("source_url") or "").strip()[:1000] or None,
                    "published_at": payload.get("published_at"),
                    "confidence": confidence,
                    "data_quality": str(payload.get("data_quality") or "manual").strip().lower()[:32],
                    "raw_metadata": Json(_json_safe(payload.get("raw_metadata") or {})),
                },
            )
            row = cur.fetchone()
        conn.commit()
    return _json_safe(dict(row or {}))


def list_macro_observations(
    *,
    metric_key: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    ensure_macro_research_tables()
    safe_limit = max(1, min(int(limit), 500))
    safe_offset = max(0, min(int(offset), 10000))
    params: dict[str, Any] = {"limit": safe_limit, "offset": safe_offset}
    where = ""
    key = _clean_key(metric_key)
    if key:
        where = "WHERE metric_key = %(metric_key)s"
        params["metric_key"] = key
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT COUNT(*)::INT AS c FROM macro_observations {where}", params)
            total = int((cur.fetchone() or {}).get("c") or 0)
            cur.execute(
                f"""
                SELECT id::text AS id, metric_key, period, value, unit, source_name,
                       source_url, published_at, confidence, data_quality, raw_metadata,
                       created_at, updated_at
                FROM macro_observations
                {where}
                ORDER BY period DESC, published_at DESC NULLS LAST, updated_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                params,
            )
            rows = [dict(row) for row in cur.fetchall()]
    return {
        "items": _json_safe(rows),
        "count": len(rows),
        "total_count": total,
        "limit": safe_limit,
        "offset": safe_offset,
        "source_registry": OFFICIAL_SOURCE_REGISTRY,
    }


_ALIASES: dict[str, str] = {
    "gdp": "gdp_growth_yoy",
    "gdp_yoy": "gdp_growth_yoy",
    "cpi": "cpi_yoy",
    "policy_rate": "policy_rate",
    "refinancing_rate": "policy_rate",
    "usd_vnd": "usd_vnd_yoy",
    "fx": "usd_vnd_yoy",
    "credit": "credit_growth_yoy",
    "liquidity": "market_liquidity_value",
    "foreign_flow": "foreign_net_flow_vnd",
    "pe": "vnindex_pe",
    "margin": "margin_pressure",
}

# component, direction, low, high, display label
_RULES: dict[str, tuple[str, str, float, float, str]] = {
    "gdp_growth_yoy": ("growth", "high_good", 2.0, 8.0, "GDP growth"),
    "industrial_production_yoy": ("growth", "high_good", 0.0, 12.0, "Industrial production"),
    "fdi_disbursement_yoy": ("growth", "high_good", -5.0, 15.0, "FDI disbursement"),
    "exports_yoy": ("growth", "high_good", -8.0, 15.0, "Exports"),
    "public_investment_yoy": ("growth", "high_good", 0.0, 20.0, "Public investment"),
    "cpi_yoy": ("inflation_rates", "low_good", 2.5, 6.5, "CPI inflation"),
    "policy_rate": ("inflation_rates", "low_good", 4.0, 8.0, "Policy rate"),
    "deposit_rate": ("inflation_rates", "low_good", 4.5, 9.0, "Deposit rate"),
    "usd_vnd_yoy": ("fx_external", "low_good", 0.0, 6.0, "USD/VND pressure"),
    "dxy_yoy": ("fx_external", "low_good", -2.0, 8.0, "USD index"),
    "china_pmi": ("fx_external", "high_good", 48.0, 53.0, "China demand"),
    "credit_growth_yoy": ("credit_liquidity", "high_good", 6.0, 16.0, "Credit growth"),
    "market_liquidity_value": ("credit_liquidity", "high_good", 8_000.0, 25_000.0, "Market liquidity"),
    "margin_balance_yoy": ("credit_liquidity", "balanced", -10.0, 25.0, "Margin balance growth"),
    "foreign_net_flow_vnd": ("valuation_flows", "high_good", -5_000.0, 5_000.0, "Foreign net flow"),
    "vnindex_pe": ("valuation_flows", "valuation", 10.0, 22.0, "VNIndex P/E"),
    "market_pb": ("valuation_flows", "valuation", 1.2, 2.8, "Market P/B"),
    "margin_pressure": ("stress_risk", "risk", 20.0, 80.0, "Margin pressure"),
    "geopolitical_risk": ("stress_risk", "risk", 20.0, 80.0, "Geopolitical risk"),
    "default_stress": ("stress_risk", "risk", 15.0, 65.0, "Default stress"),
    "vnindex_volatility": ("stress_risk", "risk", 10.0, 35.0, "VNIndex volatility"),
}


def _canonical_metric_key(metric_key: Any) -> str:
    key = _clean_key(metric_key)
    return _ALIASES.get(key, key)


def _quality_weight(row: dict[str, Any]) -> float:
    q = str(row.get("data_quality") or "unknown").strip().lower()
    q_weight = {
        "official": 1.0,
        "verified": 0.95,
        "good": 0.9,
        "manual": 0.85,
        "estimated": 0.65,
        "stale": 0.45,
        "low": 0.4,
    }.get(q, 0.75)
    confidence = _to_float(row.get("confidence"))
    c_weight = max(0.2, min(1.0, (confidence if confidence is not None else 75.0) / 100.0))
    return q_weight * c_weight


def _linear(value: float, low: float, high: float) -> float:
    if high <= low:
        return 50.0
    return max(0.0, min(100.0, ((value - low) / (high - low)) * 100.0))


def _score_metric(value: float, direction: str, low: float, high: float) -> float:
    if direction == "high_good":
        return _linear(value, low, high)
    if direction == "low_good":
        return 100.0 - _linear(value, low, high)
    if direction == "risk":
        return _linear(value, low, high)
    if direction == "balanced":
        if value < low:
            return max(35.0, 70.0 + value)
        if value <= high:
            return 85.0
        return max(0.0, 85.0 - (value - high) * 3.0)
    if direction == "valuation":
        midpoint = (low + high) / 2.0
        distance = abs(value - midpoint)
        return max(0.0, 100.0 - (distance / max(1.0, midpoint - low)) * 55.0)
    return 50.0


def _latest_by_metric(observations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in observations:
        key = _canonical_metric_key(row.get("metric_key"))
        if not key:
            continue
        previous = latest.get(key)
        if previous is None:
            latest[key] = row
            continue
        old_period = str(previous.get("period") or "")
        new_period = str(row.get("period") or "")
        if new_period >= old_period:
            latest[key] = row
    return latest


def compute_macro_regime_from_observations(
    observations: list[dict[str, Any]],
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    snapshot_date = as_of or date.today()
    latest = _latest_by_metric(observations)
    component_values: dict[str, list[tuple[float, float, dict[str, Any], str]]] = {
        "growth": [],
        "inflation_rates": [],
        "fx_external": [],
        "credit_liquidity": [],
        "valuation_flows": [],
        "stress_risk": [],
    }
    drivers: list[dict[str, Any]] = []
    warnings: list[str] = []
    data_gaps: list[str] = []

    for metric_key, rule in _RULES.items():
        row = latest.get(metric_key)
        if not row:
            data_gaps.append(metric_key)
            continue
        value = _to_float(row.get("value"))
        if value is None:
            data_gaps.append(metric_key)
            continue
        component, direction, low, high, label = rule
        score = _score_metric(value, direction, low, high)
        weight = _quality_weight(row)
        component_values[component].append((score, weight, row, label))
        if score >= 72 or score <= 35 or (component == "stress_risk" and score >= 55):
            drivers.append(
                {
                    "metric_key": metric_key,
                    "label": label,
                    "component": component,
                    "value": value,
                    "unit": row.get("unit") or "",
                    "score": round(score, 2),
                    "source_name": row.get("source_name"),
                    "direction": "risk" if component == "stress_risk" else ("positive" if score >= 60 else "negative"),
                }
            )

    components: dict[str, float] = {}
    for component, rows in component_values.items():
        if not rows:
            components[component] = 50.0 if component != "stress_risk" else 35.0
            continue
        weight_sum = sum(max(0.01, weight) for _, weight, _, _ in rows)
        components[component] = round(
            sum(score * max(0.01, weight) for score, weight, _, _ in rows) / weight_sum,
            2,
        )

    healthy_components = [
        components["growth"],
        components["inflation_rates"],
        components["fx_external"],
        components["credit_liquidity"],
        components["valuation_flows"],
    ]
    health_score = sum(healthy_components) / len(healthy_components)
    stress_risk = components["stress_risk"]
    regime_score = round(max(0.0, min(100.0, health_score - stress_risk * 0.22)), 2)

    if stress_risk >= 70 or regime_score < 35:
        regime = "Stress"
    elif components["growth"] >= 65 and components["credit_liquidity"] >= 55 and components["inflation_rates"] >= 45 and stress_risk < 50:
        regime = "Expansion"
    elif components["growth"] >= 62 and (components["inflation_rates"] < 42 or components["valuation_flows"] < 40 or stress_risk >= 55):
        regime = "Overheated"
    else:
        regime = "Recovery"

    source_names = {str(row.get("source_name") or "").strip() for row in latest.values() if row.get("source_name")}
    official_names = {src["name"].lower() for src in OFFICIAL_SOURCE_REGISTRY}
    observed_official = sorted(name for name in source_names if name.lower() in official_names)
    if len(observed_official) < 3:
        warnings.append("Low official-source coverage; regime relies on sparse or manual observations.")
    if len(data_gaps) > 10:
        warnings.append("Large macro data gaps; treat regime as provisional.")

    return {
        "regime": regime,
        "regime_score": regime_score,
        "as_of": snapshot_date.isoformat(),
        "components": components,
        "drivers": drivers[:10],
        "warnings": warnings,
        "source_coverage": {
            "observed_sources": sorted(source_names),
            "official_sources_observed": observed_official,
            "official_sources_total": len(OFFICIAL_SOURCE_REGISTRY),
            "source_registry": OFFICIAL_SOURCE_REGISTRY,
        },
        "data_gaps": data_gaps,
        "disclaimer": MACRO_DISCLAIMER,
    }


def _load_latest_macro_observations(limit: int = 500) -> list[dict[str, Any]]:
    ensure_macro_research_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id::text AS id, metric_key, period, value, unit, source_name,
                       source_url, published_at, confidence, data_quality, raw_metadata,
                       created_at, updated_at
                FROM macro_observations
                ORDER BY period DESC, published_at DESC NULLS LAST, updated_at DESC
                LIMIT %(limit)s
                """,
                {"limit": max(1, min(int(limit), 1000))},
            )
            return [dict(row) for row in cur.fetchall()]


def persist_macro_regime_snapshot(payload: dict[str, Any]) -> None:
    ensure_macro_research_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO macro_regime_snapshots (
                    id, as_of, regime, regime_score, components, drivers,
                    warnings, source_coverage, data_gaps, created_at
                ) VALUES (
                    %(id)s, %(as_of)s, %(regime)s, %(regime_score)s, %(components)s,
                    %(drivers)s, %(warnings)s, %(source_coverage)s, %(data_gaps)s, NOW()
                )
                """,
                {
                    "id": uuid4(),
                    "as_of": payload["as_of"],
                    "regime": payload["regime"],
                    "regime_score": payload["regime_score"],
                    "components": Json(_json_safe(payload.get("components") or {})),
                    "drivers": Json(_json_safe(payload.get("drivers") or [])),
                    "warnings": Json(_json_safe(payload.get("warnings") or [])),
                    "source_coverage": Json(_json_safe(payload.get("source_coverage") or {})),
                    "data_gaps": Json(_json_safe(payload.get("data_gaps") or [])),
                },
            )
        conn.commit()


def get_macro_regime(*, persist_snapshot: bool = True) -> dict[str, Any]:
    observations = _load_latest_macro_observations()
    result = compute_macro_regime_from_observations(observations)
    if persist_snapshot:
        persist_macro_regime_snapshot(result)
    return result
