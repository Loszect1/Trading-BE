from __future__ import annotations

import json
from typing import Any


def test_contribution_schema_requires_all_properties_for_strict_structured_output() -> None:
    from app.services.strategy_memory_contribution_service import CONTRIBUTION_ANALYSIS_SCHEMA

    def walk(schema: dict[str, Any], path: str = "$") -> None:
        if schema.get("type") == "object" or (
            isinstance(schema.get("type"), list) and "object" in schema.get("type", [])
        ):
            properties = schema.get("properties")
            if schema.get("additionalProperties") is False and isinstance(properties, dict):
                assert set(schema.get("required") or []) == set(properties), path
                for key, child in properties.items():
                    if isinstance(child, dict):
                        walk(child, f"{path}.{key}")
        items = schema.get("items")
        if isinstance(items, dict):
            walk(items, f"{path}[]")

    walk(CONTRIBUTION_ANALYSIS_SCHEMA)


def test_memory_contribution_records_new_candidate_with_evidence(monkeypatch) -> None:
    from app.services import strategy_memory_contribution_service as svc

    calls: dict[str, Any] = {}
    monkeypatch.setattr(svc.settings, "use_gpt", True)
    monkeypatch.setattr(svc.settings, "gpt_model", "gpt-test")
    monkeypatch.setattr(svc.settings, "gpt_max_tokens", 2200)
    monkeypatch.setattr(svc, "_gather_research_context", lambda: {"macro_regime": {"regime": "Recovery"}})

    def fake_generate(**kwargs):  # noqa: ANN003
        calls["prompt"] = kwargs["prompt"]
        return json.dumps(
            {
                "analysis_report": {
                    "verified_facts": ["Input is internally coherent."],
                    "contradictions_or_issues": [],
                    "gaps_or_missing": ["Needs source date."],
                    "suggested_workflow_type": "RISK_MANAGEMENT_MEMORY",
                    "overall_assessment": "Usable with review.",
                    "research_notes": "Checked against context.",
                },
                "proposed_llm_recommendation": {
                    "title": "Risk buffer",
                    "summary": "Keep a cash buffer.",
                    "allocation": {
                        "cash": "25-30%",
                        "equity": "70-75%",
                        "position_size": "Normal size only in liquid regimes.",
                        "sector_limits": None,
                        "notes": "Context memory only.",
                    },
                    "rules": ["No leverage"],
                    "monitoring_metrics": ["VNINDEX liquidity"],
                    "invalidation_triggers": ["Drawdown stress"],
                    "assumptions": ["Context only"],
                },
                "proposed_final_system_decision": {
                    "scope": ["macro_gpt_analysis", "long_term_research"],
                    "automatic_execution": False,
                    "deterministic_scoring_change": False,
                },
                "proposed_guardrail": {
                    "status": "APPROVED_FOR_CONTEXT_ONLY",
                    "reason": "Context only.",
                },
                "confidence": 82,
            }
        )

    def fake_record_ai_decision_event(**kwargs):  # noqa: ANN003
        calls["record"] = kwargs
        return {
            "id": "00000000-0000-0000-0000-000000000001",
            "workflow_type": kwargs["workflow_type"],
            "reuse_status": kwargs["reuse_status"],
        }

    def fake_persist(event_id, sources):  # noqa: ANN001
        calls["persist"] = {"event_id": event_id, "sources": sources}
        return [{"id": "src-1", "event_id": event_id, **sources[0]}]

    monkeypatch.setattr(svc.gpt_service, "generate_text_with_resilience", fake_generate)
    monkeypatch.setattr(svc, "record_ai_decision_event", fake_record_ai_decision_event)
    monkeypatch.setattr(svc, "persist_ai_memory_evidence_sources", fake_persist)

    out = svc.analyze_and_propose_memory_contribution(
        user_text="Keep risk low when liquidity fades.",
        evidence_sources=[
            {
                "filename": "risk.md",
                "kind": "text/markdown",
                "file_sha256": "abc",
                "file_size_bytes": 42,
                "extraction_method": "text_decode",
                "excerpt": "Liquidity fades",
                "warnings": [],
            }
        ],
    )

    assert "Attached evidence excerpts" in calls["prompt"]
    assert out["workflow_type_used"] == "RISK_MANAGEMENT_MEMORY"
    assert calls["record"]["workflow_type"] == "RISK_MANAGEMENT_MEMORY"
    assert calls["record"]["reuse_status"] == "NEW"
    assert calls["record"]["input_snapshot"]["evidence_count"] == 1
    assert calls["persist"]["event_id"] == "00000000-0000-0000-0000-000000000001"
    assert out["evidence_sources"][0]["filename"] == "risk.md"
