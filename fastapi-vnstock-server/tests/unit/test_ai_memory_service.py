from __future__ import annotations

from app.services import ai_memory_service as service


def test_extract_text_memory_evidence_source() -> None:
    source = service.extract_memory_evidence_source(
        filename="notes.md",
        content_type="text/markdown",
        raw=b"Macro view\nCredit growth improving",
    )

    assert source["extraction_method"] == "text_decode"
    assert source["file_size_bytes"] > 0
    assert source["file_sha256"]
    assert "Credit growth" in source["excerpt"]
    assert source["warnings"] == []


def test_normalize_memory_workflow_type_falls_back_to_other() -> None:
    assert service.normalize_memory_workflow_type("risk_management_memory") == "RISK_MANAGEMENT_MEMORY"
    assert service.normalize_memory_workflow_type("made_up") == "OTHER_STRATEGY_CONTEXT"


def test_summarize_grouped_global_ai_memory_keeps_categories() -> None:
    rows = [
        {
            "workflow_type": "RISK_MANAGEMENT_MEMORY",
            "strategy_type": "LONG_TERM",
            "source_type": "user_contribution",
            "confidence": 82,
            "reuse_status": "APPROVED",
            "llm_recommendation": {"title": "Risk cap", "summary": "Keep cash buffer", "rules": ["No leverage"]},
            "final_system_decision": {"scope": ["macro_gpt_analysis", "long_term_research"]},
            "guardrail_result": {"status": "APPROVED_FOR_CONTEXT_ONLY"},
        },
        {
            "workflow_type": "TECHNICAL_PATTERN_MEMORY",
            "strategy_type": "LONG_TERM",
            "source_type": "user_contribution",
            "confidence": 75,
            "reuse_status": "APPROVED",
            "llm_recommendation": {"title": "Base breakout", "summary": "Confirm with volume"},
            "final_system_decision": {"scope": ["long_term_research"]},
            "guardrail_result": {"status": "APPROVED_FOR_CONTEXT_ONLY"},
        },
    ]

    grouped = service.summarize_grouped_global_ai_memory(rows)

    assert set(grouped) == {"RISK_MANAGEMENT_MEMORY", "TECHNICAL_PATTERN_MEMORY"}
    assert grouped["RISK_MANAGEMENT_MEMORY"][0]["memory"]["title"] == "Risk cap"
    assert grouped["TECHNICAL_PATTERN_MEMORY"][0]["memory"]["summary"] == "Confirm with volume"
