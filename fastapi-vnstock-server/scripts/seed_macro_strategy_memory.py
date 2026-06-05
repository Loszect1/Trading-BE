from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.ai_decision_event_service import record_ai_decision_event  # noqa: E402

SOURCE_ID = "balanced-vn-macro-strategy-2026-06-05"
IDEMPOTENCY_KEY = f"macro-strategy-memory:{SOURCE_ID}"


def build_memory_payload() -> dict:
    return {
        "title": "Balanced Vietnam macro strategy, 3-5 year horizon",
        "summary": (
            "Use the 2026 Vietnam growth thesis as a macro filter, not a certainty. "
            "Deploy capital in staged tranches, keep no leverage, and require evidence from "
            "growth, inflation, FDI, credit, public investment, VNIndex liquidity, foreign flow, "
            "and real-estate transaction quality before adding risk."
        ),
        "allocation": {
            "cash_or_deposits_pct": "25-30",
            "vn_equities_pct": "35-40",
            "real_estate_reserve_pct": "20-25",
            "gold_usd_hedge_pct": "5-10",
            "skills_business_cashflow_pct": "5-10",
        },
        "rules": [
            "Do not use leverage while inflation is elevated and real-estate credit is controlled.",
            "Treat 10%+ Vietnam GDP growth as upside case, not base case.",
            "Buy VN equities selectively, not broad VNIndex FOMO.",
            "Prefer banks with quality credit, industrial parks, logistics, infrastructure, consumption, tourism, and FDI/export-linked businesses.",
            "Use real estate only when title/legal status is clean, price has margin of safety, rental or use value is clear, and jobs/infrastructure are already credible.",
            "Use gold/USD as hedge only, not the main wealth engine.",
        ],
        "monitoring_metrics": [
            "CPI and core CPI",
            "credit growth and lending/deposit rates",
            "FDI registration and disbursement",
            "public investment disbursement",
            "VNIndex breadth, liquidity, valuation, and foreign flow",
            "USD/VND pressure",
            "real-estate transaction volume, inventory, legal status, and rental yields",
        ],
        "invalidation_triggers": [
            "Inflation accelerates or stays above policy comfort range.",
            "USD/VND breaks higher with persistent FX stress.",
            "Credit growth stalls or asset-quality stress rises.",
            "Public investment disbursement misses plan materially.",
            "FDI disbursement weakens for multiple months.",
            "VNIndex rises on narrowing breadth and weak liquidity.",
            "Foreign selling expands after market-upgrade catalyst.",
            "Real-estate liquidity remains poor despite headline price increases.",
        ],
        "assumptions": [
            "User is mostly cash.",
            "Strategy horizon is 3-5 years.",
            "Risk profile is moderate.",
            "No leverage.",
            "No individual ticker or property recommendation without current price, liquidity, valuation, and risk checks.",
        ],
    }


def seed() -> dict:
    payload = build_memory_payload()
    prompt_text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return record_ai_decision_event(
        workflow_type="MACRO_STRATEGY_MEMORY",
        account_mode=None,
        symbol=None,
        strategy_type="LONG_TERM",
        source_type="manual_user_strategy_plan",
        source_id=SOURCE_ID,
        idempotency_key=IDEMPOTENCY_KEY,
        model="manual-codex",
        schema_version="macro-strategy-memory-v1",
        prompt_text=prompt_text,
        confidence=75,
        reuse_status="APPROVED",
        input_snapshot={
            "origin": "user_requested_backend_gpt_memory",
            "as_of": "2026-06-05",
            "source_note": "Adjusted balanced macro strategy based on local Telegram CSV analysis and current macro verification.",
        },
        llm_recommendation=payload,
        final_system_decision={
            "scope": ["macro_gpt_analysis", "long_term_research"],
            "automatic_execution": False,
            "deterministic_scoring_change": False,
        },
        guardrail_result={
            "status": "APPROVED_FOR_CONTEXT_ONLY",
            "reason": "Long-term macro memory must not be treated as an order, signal, or guarantee.",
        },
    )


if __name__ == "__main__":
    print(json.dumps(seed(), ensure_ascii=False, default=str))
