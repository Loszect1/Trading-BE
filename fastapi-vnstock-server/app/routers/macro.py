from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.macro_service import (
    get_macro_regime,
    list_macro_observations,
    upsert_macro_observation,
)
from app.services.macro_gpt_analysis_service import analyze_macro_news_economics_with_gpt

router = APIRouter(prefix="/macro", tags=["macro"])


class MacroObservationRequest(BaseModel):
    metric_key: str = Field(..., min_length=1, max_length=96)
    period: str = Field(..., min_length=1, max_length=32)
    value: float
    unit: str = ""
    source_name: str = Field(..., min_length=1, max_length=240)
    source_url: str | None = None
    published_at: datetime | None = None
    confidence: float = Field(default=75, ge=0, le=100)
    data_quality: str = Field(default="manual", max_length=32)
    raw_metadata: dict[str, Any] = Field(default_factory=dict)


class MacroGptAnalysisRequest(BaseModel):
    news_limit: int = Field(default=40, ge=5, le=80)
    observation_limit: int = Field(default=80, ge=5, le=200)
    language: Literal["en", "vi"] = "vi"
    force_refresh: bool = False


@router.post("/observations")
def post_macro_observation(body: MacroObservationRequest) -> dict[str, Any]:
    try:
        return {"success": True, "data": upsert_macro_observation(body.model_dump())}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save macro observation: {exc}") from exc


@router.get("/observations")
def get_macro_observations(
    metric_key: str | None = Query(default=None, max_length=96),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    try:
        return {"success": True, "data": list_macro_observations(metric_key=metric_key, limit=limit, offset=offset)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read macro observations: {exc}") from exc


@router.get("/regime")
def get_macro_regime_alias() -> dict[str, Any]:
    try:
        return get_macro_regime()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to compute macro regime: {exc}") from exc


@router.post("/gpt-analysis")
def post_macro_gpt_analysis(body: MacroGptAnalysisRequest = MacroGptAnalysisRequest()) -> dict[str, Any]:
    try:
        return analyze_macro_news_economics_with_gpt(
            news_limit=body.news_limit,
            observation_limit=body.observation_limit,
            language=body.language,
            force_refresh=body.force_refresh,
        )
    except RuntimeError as exc:
        if str(exc) == "gpt_disabled":
            raise HTTPException(
                status_code=503,
                detail="GPT is disabled in the backend container. Set USE_GPT=true and restart BE.",
            ) from exc
        raise HTTPException(status_code=500, detail=f"GPT macro analysis failed: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"Invalid GPT macro analysis response: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"GPT macro analysis failed: {exc}") from exc
