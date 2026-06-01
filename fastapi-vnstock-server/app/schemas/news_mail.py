from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class NewsMailPrepareRequest(BaseModel):
    query: str | None = Field(default=None, max_length=500)
    max_results: int | None = Field(default=None, ge=1, le=100)
    force_refresh: bool = False
    article_fetch_limit: int | None = Field(default=None, ge=1, le=200)


class NewsMailSymbolImpactResult(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    company_name: str | None = Field(default=None, max_length=500)
    relevance_score: float = Field(default=0, ge=0, le=100)
    sentiment_label: str = Field(default="neutral", max_length=16)
    sentiment_score: float = Field(default=0, ge=-100, le=100)
    impact_score: float = Field(default=0, ge=0, le=100)
    impact_horizon: str | None = Field(default=None, max_length=16)
    confidence: float = Field(default=0, ge=0, le=100)
    rationale: str | None = Field(default=None, max_length=4000)


class NewsMailArticleCodexResult(BaseModel):
    article_id: UUID
    summary: str = Field(default="", max_length=4000)
    key_points: list[str] = Field(default_factory=list)
    sector_tags: list[str] = Field(default_factory=list)
    market_tags: list[str] = Field(default_factory=list)
    data_gaps: list[str] = Field(default_factory=list)
    symbols: list[NewsMailSymbolImpactResult] = Field(default_factory=list)


class NewsMailCodexResultRequest(BaseModel):
    articles: list[NewsMailArticleCodexResult] = Field(default_factory=list)
    final_batch: bool = True
    run_metadata: dict[str, Any] = Field(default_factory=dict)


class NewsMailRunFailRequest(BaseModel):
    error: str = Field(min_length=1, max_length=4000)
    metadata: dict[str, Any] = Field(default_factory=dict)
