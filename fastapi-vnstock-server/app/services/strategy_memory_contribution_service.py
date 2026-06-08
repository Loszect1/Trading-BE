from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.services.ai_decision_event_service import record_ai_decision_event
from app.services.ai_memory_service import (
    normalize_memory_workflow_type,
    persist_ai_memory_evidence_sources,
)
from app.services.gpt_service import GptService
from app.services.macro_service import get_macro_regime, list_macro_observations
from app.services.news_mail_service import get_morning_brief, get_top_impact_news

gpt_service = GptService()

CONTRIBUTION_ANALYSIS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "analysis_report": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "verified_facts": {"type": "array", "items": {"type": "string"}},
                "contradictions_or_issues": {"type": "array", "items": {"type": "string"}},
                "gaps_or_missing": {"type": "array", "items": {"type": "string"}},
                "suggested_workflow_type": {"type": "string"},
                "overall_assessment": {"type": "string"},
                "research_notes": {"type": "string"},
            },
            "required": [
                "verified_facts",
                "contradictions_or_issues",
                "gaps_or_missing",
                "suggested_workflow_type",
                "overall_assessment",
                "research_notes",
            ],
        },
        "proposed_llm_recommendation": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "title": {"type": "string"},
                "summary": {"type": "string"},
                "allocation": {
                    "type": ["object", "null"],
                    "additionalProperties": False,
                    "properties": {
                        "cash": {"type": ["string", "number", "null"]},
                        "equity": {"type": ["string", "number", "null"]},
                        "position_size": {"type": ["string", "number", "null"]},
                        "sector_limits": {"type": ["string", "null"]},
                        "notes": {"type": ["string", "null"]},
                    },
                    "required": ["cash", "equity", "position_size", "sector_limits", "notes"],
                },
                "rules": {"type": "array", "items": {"type": "string"}},
                "monitoring_metrics": {"type": "array", "items": {"type": "string"}},
                "invalidation_triggers": {"type": "array", "items": {"type": "string"}},
                "assumptions": {"type": "array", "items": {"type": "string"}},
            },
            "required": [
                "title",
                "summary",
                "allocation",
                "rules",
                "monitoring_metrics",
                "invalidation_triggers",
                "assumptions",
            ],
        },
        "proposed_final_system_decision": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "scope": {"type": "array", "items": {"type": "string"}},
                "automatic_execution": {"type": "boolean"},
                "deterministic_scoring_change": {"type": "boolean"},
            },
            "required": ["scope", "automatic_execution", "deterministic_scoring_change"],
        },
        "proposed_guardrail": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "status": {"type": "string"},
                "reason": {"type": "string"},
            },
            "required": ["status", "reason"],
        },
        "confidence": {"type": "number"},
    },
    "required": [
        "analysis_report",
        "proposed_llm_recommendation",
        "proposed_final_system_decision",
        "proposed_guardrail",
        "confidence",
    ],
}


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _compact_text(value: Any, max_chars: int = 6000) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)] + "..."


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        return _compact_text(value, 400)
    if isinstance(value, dict):
        return {str(k): _json_safe(v, depth=depth + 1) for k, v in list(value.items())[:60]}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v, depth=depth + 1) for v in value[:40]]
    if isinstance(value, str):
        return _compact_text(value, 2000)
    return value


def _gather_research_context(max_items: int = 30) -> dict[str, Any]:
    """Lightweight research grounding for fact-checking user contributions (reuses existing data sources)."""
    try:
        regime = get_macro_regime(persist_snapshot=False)
    except Exception:
        regime = None

    try:
        observations = list_macro_observations(limit=30).get("items", [])
    except Exception:
        observations = []

    try:
        top_news = get_top_impact_news(limit=15).get("items", [])
    except Exception:
        top_news = []

    try:
        morning = get_morning_brief(limit=8)
    except Exception:
        morning = {}

    # We intentionally do NOT force approved memory here; the fact-check is about the new input.
    return {
        "as_of": _utc_now_iso(),
        "macro_regime": regime,
        "macro_observations": [
            {
                "metric_key": o.get("metric_key"),
                "period": o.get("period"),
                "value": o.get("value"),
                "unit": o.get("unit"),
                "source_name": o.get("source_name"),
            }
            for o in observations[:max_items]
        ],
        "top_news_impacts": [
            {
                "title": _compact_text(n.get("title") or n.get("article_title"), 180),
                "category": n.get("category") or n.get("category_slug"),
                "sentiment_label": n.get("sentiment_label"),
                "impact_score": n.get("impact_score"),
            }
            for n in top_news[:max_items]
        ],
        "morning_brief": {
            "positive_count": (morning or {}).get("positive_count"),
            "negative_count": (morning or {}).get("negative_count"),
        },
    }


def _build_system_prompt() -> str:
    return (
        "You are a rigorous, evidence-based fact-checker and long-term strategy memory curator for the Vietnamese stock market. "
        "Your job is to deeply analyze the user-provided information against available macro data, news, and market observations. "
        "Be precise. Clearly separate verified facts from contradictions, uncertainties, and gaps. "
        "Propose a clean, reusable structured memory entry suitable for injection into future GPT prompts for macro analysis and long-term research. "
        "Never invent data. When information is missing or uncertain, explicitly call it out. "
        "Output strictly valid JSON matching the provided schema. All prose in Vietnamese where appropriate for the domain (keep English financial terms and codes)."
    )


def _build_user_prompt(user_text: str, evidence_sources: list[dict], research: dict, category_hint: str, notes: str) -> str:
    excerpts_block = ""
    if evidence_sources:
        excerpts_block = "\n\nAttached evidence excerpts (text/PDF extraction or image OCR):\n"
        for ex in evidence_sources[:6]:
            warning_text = f" warnings={ex.get('warnings')}" if ex.get("warnings") else ""
            excerpts_block += (
                f"- {ex.get('filename')} ({ex.get('extraction_method') or ex.get('kind')}{warning_text}): "
                f"{_compact_text(ex.get('excerpt') or ex.get('extracted_text'), 1400)}\n"
            )

    notes_block = f"\nAdditional user notes/context: {notes}\n" if notes else ""
    hint_line = f"User-provided category preference (use only as a very soft hint if it makes sense; you decide the best one): {category_hint}\n\n" if category_hint else ""

    return (
        f"User-submitted information to analyze, fact-check, and turn into approved long-term strategy memory:\n\n"
        f"{user_text}\n"
        f"{excerpts_block}"
        f"{notes_block}\n"
        f"{hint_line}"
        "Research grounding context (macro regime, observations, recent high-impact news, brief):\n"
        f"{json.dumps(_json_safe(research), ensure_ascii=True)[:14000]}\n\n"
        "Instructions:\n"
        "1. Perform deep cross-check of every substantive claim in the user input against the research context and general knowledge of VN markets.\n"
        "2. Produce analysis_report with:\n"
        "   - verified_facts (list of claims that check out)\n"
        "   - contradictions_or_issues (list)\n"
        "   - gaps_or_missing (list)\n"
        "   - suggested_workflow_type: **You must choose the single best category** from this list: MACRO_STRATEGY_MEMORY, RISK_MANAGEMENT_MEMORY, TECHNICAL_PATTERN_MEMORY, MARKET_STRUCTURE_MEMORY, FUNDAMENTAL_THESIS_MEMORY, OTHER_STRATEGY_CONTEXT. Base the choice purely on the content of the information (macro/vĩ mô → MACRO_STRATEGY_MEMORY, risk rules/guardrails → RISK_..., chart patterns/technical setups → TECHNICAL_..., etc.).\n"
        "   - overall_assessment (short paragraph)\n"
        "   - research_notes\n"
        "3. Propose a high-quality, concise structured memory in proposed_llm_recommendation (title, summary, allocation object or null, rules[], monitoring_metrics[], invalidation_triggers[], assumptions[]).\n"
        "4. Propose proposed_final_system_decision with scope (array, e.g. [\"macro_gpt_analysis\", \"long_term_research\"]) and flags.\n"
        "5. Propose proposed_guardrail (status e.g. \"APPROVED_FOR_CONTEXT_ONLY\", reason).\n"
        "6. Give an overall confidence (0-100) for the proposed memory entry.\n"
        "Return only the JSON object."
    )


def _parse_gpt_json(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("GPT contribution analysis returned non-object JSON")
    return parsed


def analyze_and_propose_memory_contribution(
    *,
    user_text: str,
    file_excerpts: list[dict[str, Any]] | None = None,
    evidence_sources: list[dict[str, Any]] | None = None,
    category_hint: str | None = None,
    notes: str = "",
) -> dict[str, Any]:
    """
    Deep analysis + research + fact-check of user-provided information.
    GPT fully decides the best strategy memory category (workflow_type).
    Produces a proposed structured memory and records it as NEW (for human review/approval).
    The recorded entry can later be set to APPROVED and will then participate in get_global_ai_memory.
    """
    if not settings.use_gpt:
        raise RuntimeError("gpt_disabled")

    sources = evidence_sources if evidence_sources is not None else file_excerpts
    if not (user_text or "").strip() and not (sources or []):
        raise ValueError("user_text or at least one file is required")

    evidence_sources = sources or []
    research = _gather_research_context()

    prompt = _build_user_prompt(user_text, evidence_sources, research, category_hint or "", notes)
    system_prompt = _build_system_prompt()

    # Use resilience + schema for structured output (same pattern as macro GPT analysis)
    raw_text = gpt_service.generate_text_with_resilience(
        prompt=prompt,
        system_prompt=system_prompt,
        model=settings.gpt_model,
        max_tokens=min(2200, int(settings.gpt_max_tokens)),
        temperature=0.15,
        cache_namespace="memory_contribution_factcheck_v1",
        cache_ttl_seconds=60 * 60 * 6,  # 6h cache for identical contribs
        output_schema=CONTRIBUTION_ANALYSIS_SCHEMA,
    )

    parsed = _parse_gpt_json(raw_text)

    analysis_report = parsed.get("analysis_report") or {}
    proposed_reco = parsed.get("proposed_llm_recommendation") or {}
    proposed_final = parsed.get("proposed_final_system_decision") or {
        "scope": ["macro_gpt_analysis", "long_term_research"],
        "automatic_execution": False,
        "deterministic_scoring_change": False,
    }
    proposed_guard = parsed.get("proposed_guardrail") or {
        "status": "APPROVED_FOR_CONTEXT_ONLY",
        "reason": "User-contributed long-term memory. Must be reviewed before use as strategy context.",
    }
    confidence = parsed.get("confidence") or 60

    # GPT fully decides the category via suggested_workflow_type.
    # We only fall back to a sensible default if the model fails to suggest one.
    workflow_type = normalize_memory_workflow_type(str(analysis_report.get("suggested_workflow_type") or "MACRO_STRATEGY_MEMORY"))

    # Record as NEW so it goes through human approval before affecting global approved memory
    recorded = record_ai_decision_event(
        workflow_type=workflow_type,
        llm_recommendation=proposed_reco,
        final_system_decision=proposed_final,
        guardrail_result=proposed_guard,
        input_snapshot={
            "source": "user_contribution",
            "user_text": _compact_text(user_text, 8000),
            "evidence_sources": [
                {
                    "filename": ex.get("filename"),
                    "kind": ex.get("kind") or ex.get("content_type"),
                    "file_sha256": ex.get("file_sha256"),
                    "file_size_bytes": ex.get("file_size_bytes"),
                    "extraction_method": ex.get("extraction_method"),
                    "excerpt_preview": _compact_text(ex.get("excerpt") or ex.get("extracted_text"), 600),
                    "warnings": ex.get("warnings") or [],
                }
                for ex in evidence_sources
            ],
            "evidence_count": len(evidence_sources),
            "notes": _compact_text(notes, 2000),
            "analysis_report": analysis_report,
            "research_context_summary": {
                "macro_regime": (research.get("macro_regime") or {}).get("regime") if isinstance(research.get("macro_regime"), dict) else research.get("macro_regime"),
                "obs_count": len(research.get("macro_observations") or []),
                "news_count": len(research.get("top_news_impacts") or []),
            },
            "category_hint": category_hint,
        },
        symbol=None,
        strategy_type="LONG_TERM",
        source_type="user_contribution",
        source_id=None,  # let service derive good idempotency
        model=settings.gpt_model,
        schema_version="memory-contribution-v1",
        prompt_text=prompt[:3000],
        confidence=confidence,
        reuse_status="NEW",
    )
    persisted_evidence = persist_ai_memory_evidence_sources(recorded["id"], evidence_sources)

    return {
        "analysis_report": analysis_report,
        "proposed_llm_recommendation": proposed_reco,
        "proposed_final_system_decision": proposed_final,
        "proposed_guardrail": proposed_guard,
        "confidence": confidence,
        "suggested_workflow_type": analysis_report.get("suggested_workflow_type"),
        "recorded_event": recorded,
        "evidence_sources": persisted_evidence,
        "workflow_type_used": workflow_type,
    }
