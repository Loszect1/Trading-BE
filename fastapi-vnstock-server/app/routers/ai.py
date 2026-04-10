from __future__ import annotations

import json
import re
from hashlib import sha256
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from app.core.config import settings
from app.services.claude_service import ClaudeService
from app.services.redis_cache import RedisCacheService
from app.services.vnstock_api_service import VNStockApiService

router = APIRouter(prefix="/ai", tags=["ai"])
claude_service = ClaudeService()
vnstock_api_service = VNStockApiService()
redis_cache_service = RedisCacheService()


class ClaudeGenerateRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    system_prompt: Optional[str] = None
    model: Optional[str] = Field(
        default=None,
        description="Optional Claude model override. Leave empty to use server default.",
        examples=["claude-sonnet-4-6"],
    )
    max_tokens: Optional[int] = Field(default=None, ge=1, le=8192)
    temperature: float = Field(default=0.2, ge=0, le=1)

    @field_validator("model", mode="before")
    @classmethod
    def normalize_model(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        if normalized.lower() in {"", "string", "none", "null", "undefined"}:
            return None
        return normalized


class AnalyzeSymbolRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10)
    interval: str = Field(default="1D", min_length=1, max_length=5)
    lookback_days: int = Field(default=90, ge=7, le=365)
    source: str = Field(default="VCI", min_length=1, max_length=10)
    model: Optional[str] = Field(
        default=None,
        description="Optional Claude model override. Leave empty to use server default.",
        examples=["claude-sonnet-4-6"],
    )
    max_tokens: Optional[int] = Field(default=700, ge=100, le=5000)
    temperature: float = Field(default=0.2, ge=0, le=1)

    @field_validator("model", mode="before")
    @classmethod
    def normalize_model(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        if normalized.lower() in {"", "string", "none", "null", "undefined"}:
            return None
        return normalized


class AnalyzeSymbolShortRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10)
    interval: str = Field(default="1D", min_length=1, max_length=5)
    lookback_days: int = Field(default=90, ge=7, le=365)
    source: str = Field(default="VCI", min_length=1, max_length=10)
    model: Optional[str] = Field(default=None, examples=["claude-sonnet-4-6"])
    max_tokens: int = Field(default=1200, ge=100, le=1200)
    temperature: float = Field(default=0.2, ge=0, le=1)

    @field_validator("model", mode="before")
    @classmethod
    def normalize_model(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        if normalized.lower() in {"", "string", "none", "null", "undefined"}:
            return None
        return normalized


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_tail(items: List[Dict[str, Any]], size: int) -> List[Dict[str, Any]]:
    if size <= 0:
        return []
    return items[-size:] if len(items) > size else items


def _simple_moving_average(values: List[float], period: int) -> Optional[float]:
    if period <= 0 or len(values) < period:
        return None
    window = values[-period:]
    return sum(window) / period if window else None


def _compute_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if period <= 0 or len(closes) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for idx in range(-period, 0):
        delta = closes[idx] - closes[idx - 1]
        if delta >= 0:
            gains.append(delta)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(delta))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _compute_atr(candles: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    if period <= 0 or len(candles) < period + 1:
        return None
    true_ranges: List[float] = []
    for i in range(1, len(candles)):
        high = _to_float(candles[i].get("high"))
        low = _to_float(candles[i].get("low"))
        prev_close = _to_float(candles[i - 1].get("close"))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    if len(true_ranges) < period:
        return None
    return sum(true_ranges[-period:]) / period


def _compute_ema(values: List[float], period: int) -> Optional[float]:
    if period <= 0 or len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for value in values[period:]:
        ema = (value * k) + (ema * (1 - k))
    return ema


def _build_technical_summary(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [_to_float(item.get("close")) for item in candles]
    volumes = [_to_int(item.get("volume")) for item in candles]
    sma_20 = _simple_moving_average(closes, 20)
    sma_50 = _simple_moving_average(closes, 50)
    ema_10 = _compute_ema(closes, 10)
    ema_12 = _compute_ema(closes, 12)
    ema_26 = _compute_ema(closes, 26)
    rsi_14 = _compute_rsi(closes, 14)
    atr_14 = _compute_atr(candles, 14)

    latest_close = closes[-1] if closes else 0.0
    latest_volume = volumes[-1] if volumes else 0
    volume_avg_20 = int(sum(volumes[-20:]) / min(len(volumes), 20)) if volumes else 0

    trend_signal = "unknown"
    if sma_20 is not None and sma_50 is not None:
        if sma_20 > sma_50 and latest_close >= sma_20:
            trend_signal = "uptrend"
        elif sma_20 < sma_50 and latest_close <= sma_20:
            trend_signal = "downtrend"
        else:
            trend_signal = "sideways"

    momentum_signal = "neutral"
    if rsi_14 is not None:
        if rsi_14 >= 70:
            momentum_signal = "overbought"
        elif rsi_14 <= 30:
            momentum_signal = "oversold"
        elif rsi_14 > 55:
            momentum_signal = "bullish"
        elif rsi_14 < 45:
            momentum_signal = "bearish"

    volume_signal = "normal"
    if volume_avg_20 > 0:
        ratio = latest_volume / volume_avg_20
        if ratio >= 1.5:
            volume_signal = "high"
        elif ratio <= 0.7:
            volume_signal = "low"

    boll_mid = sma_20
    boll_ub = None
    boll_lb = None
    if len(closes) >= 20 and boll_mid is not None:
        recent = closes[-20:]
        variance = sum((x - boll_mid) ** 2 for x in recent) / len(recent)
        std_dev = variance ** 0.5
        boll_ub = boll_mid + (2 * std_dev)
        boll_lb = boll_mid - (2 * std_dev)

    vwma = None
    if len(closes) >= 20 and len(volumes) >= 20:
        recent_close = closes[-20:]
        recent_vol = volumes[-20:]
        vol_sum = sum(recent_vol)
        if vol_sum > 0:
            vwma = sum(c * v for c, v in zip(recent_close, recent_vol)) / vol_sum

    macd = None
    macds = None
    macdh = None
    if ema_12 is not None and ema_26 is not None:
        macd = ema_12 - ema_26
        macd_series_like = []
        for idx in range(26, len(closes)):
            sub = closes[: idx + 1]
            e12 = _compute_ema(sub, 12)
            e26 = _compute_ema(sub, 26)
            if e12 is None or e26 is None:
                continue
            macd_series_like.append(e12 - e26)
        macds = _compute_ema(macd_series_like, 9) if len(macd_series_like) >= 9 else None
        if macds is not None:
            macdh = macd - macds

    return {
        "close_latest": round(latest_close, 4),
        "close_10_ema": round(ema_10, 4) if ema_10 is not None else None,
        "sma_20": round(sma_20, 4) if sma_20 is not None else None,
        "sma_50": round(sma_50, 4) if sma_50 is not None else None,
        "macd": round(macd, 4) if macd is not None else None,
        "macds": round(macds, 4) if macds is not None else None,
        "macdh": round(macdh, 4) if macdh is not None else None,
        "rsi_14": round(rsi_14, 2) if rsi_14 is not None else None,
        "boll": round(boll_mid, 4) if boll_mid is not None else None,
        "boll_ub": round(boll_ub, 4) if boll_ub is not None else None,
        "boll_lb": round(boll_lb, 4) if boll_lb is not None else None,
        "atr_14": round(atr_14, 4) if atr_14 is not None else None,
        "vwma": round(vwma, 4) if vwma is not None else None,
        "latest_volume": latest_volume,
        "avg_volume_20": volume_avg_20,
        "trend_signal": trend_signal,
        "momentum_signal": momentum_signal,
        "volume_signal": volume_signal,
    }


def _build_data_completeness(context: Dict[str, Any]) -> Dict[str, Any]:
    company = context.get("company", {})
    recent_candles = context.get("recent_candles", [])
    market_score = 100 if isinstance(recent_candles, list) and len(recent_candles) >= 20 else 60
    fundamentals_fields = ["name", "industry", "exchange", "market_cap", "issue_share", "website"]
    fundamentals_hits = sum(1 for field in fundamentals_fields if company.get(field))
    fundamentals_score = int((fundamentals_hits / len(fundamentals_fields)) * 100)
    # News and social are not collected directly in this endpoint yet.
    news_score = 35
    social_score = 25
    total_score = int((market_score + fundamentals_score + news_score + social_score) / 4)
    return {
        "market": market_score,
        "fundamentals": fundamentals_score,
        "news": news_score,
        "social_sentiment": social_score,
        "overall": total_score,
    }


def _extract_json_block(text: str) -> Optional[Dict[str, Any]]:
    pattern = r"```json\s*(\{[\s\S]*?\})\s*```"
    match = re.search(pattern, text)
    candidates: List[str] = []
    if match:
        candidates.append(match.group(1).strip())

    # Fallback: capture a likely raw JSON object even when markdown fences are missing.
    fallback_match = re.search(
        r"(\{[\s\S]*\"executive_summary\"[\s\S]*\"confidence\"[\s\S]*\})",
        text,
    )
    if fallback_match:
        candidates.append(fallback_match.group(1).strip())

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _parse_best_effort_json(text: str) -> Optional[Dict[str, Any]]:
    stripped = text.strip()
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    first = stripped.find("{")
    last = stripped.rfind("}")
    if first == -1 or last == -1 or first >= last:
        return None
    candidate = stripped[first : last + 1]
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _validate_analysis_quality(text: str, structured: Optional[Dict[str, Any]]) -> bool:
    has_bias = bool(re.search(r"Bias\s*:\s*(Bullish|Neutral|Bearish)", text, flags=re.IGNORECASE))
    has_confidence = bool(re.search(r"Confidence\s*:\s*(\d+(\.\d+)?)\s*/\s*10", text, flags=re.IGNORECASE))
    has_risk_matrix = "Risk matrix" in text or "Risk Matrix" in text
    if not (has_bias and has_confidence and has_risk_matrix):
        return False
    if not structured:
        return False
    required_keys = {
        "executive_summary",
        "market_analysis",
        "fundamentals_analysis",
        "news_analysis",
        "social_sentiment_analysis",
        "risk_matrix",
        "trading_watchlist_plan",
        "bias",
        "confidence",
    }
    return required_keys.issubset(set(structured.keys()))


def _extract_section(text: str, heading: str, fallback: str = "") -> str:
    pattern = rf"##\s*.*{re.escape(heading)}.*\n([\s\S]*?)(\n##\s|$)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return fallback
    return match.group(1).strip()


def _build_min_structured_from_text(report_text: str) -> Dict[str, Any]:
    bias_match = re.search(r"Bias\s*:\s*(Bullish|Neutral|Bearish)", report_text, flags=re.IGNORECASE)
    confidence_match = re.search(
        r"Confidence\s*:\s*(\d+(\.\d+)?)\s*/\s*10", report_text, flags=re.IGNORECASE
    )
    bias = (bias_match.group(1).capitalize() if bias_match else "Neutral")
    confidence = float(confidence_match.group(1)) if confidence_match else 5.0

    return {
        "executive_summary": _extract_section(report_text, "Executive Summary", report_text[:600]),
        "market_analysis": _extract_section(report_text, "Market Analysis"),
        "fundamentals_analysis": _extract_section(report_text, "Fundamentals Analysis"),
        "news_analysis": _extract_section(report_text, "News Analysis"),
        "social_sentiment_analysis": _extract_section(report_text, "Social sentiment analysis"),
        "risk_matrix": [],
        "trading_watchlist_plan": {"bull": "", "base": "", "bear": ""},
        "bias": bias,
        "confidence": confidence,
        "data_gaps": ["Structured JSON generated by deterministic fallback parser."],
    }


def _build_structured_from_report(
    report_text: str,
    model: Optional[str],
    temperature: float,
    max_tokens: Optional[int],
) -> Optional[Dict[str, Any]]:
    extraction_prompt = (
        "Chuyen report sau thanh JSON hop le, khong them giai thich, khong markdown, chi tra ve JSON object.\n"
        "Schema bat buoc:\n"
        "{\n"
        '  "executive_summary": "string",\n'
        '  "market_analysis": "string",\n'
        '  "fundamentals_analysis": "string",\n'
        '  "news_analysis": "string",\n'
        '  "social_sentiment_analysis": "string",\n'
        '  "risk_matrix": [{"risk":"string","probability":"Low|Medium|High","impact":"Low|Medium|High","monitoring":"string"}],\n'
        '  "trading_watchlist_plan": {"bull":"string","base":"string","bear":"string"},\n'
        '  "bias": "Bullish|Neutral|Bearish",\n'
        '  "confidence": number,\n'
        '  "data_gaps": ["string"]\n'
        "}\n\n"
        "Report:\n"
        f"{report_text}"
    )
    extracted_text = claude_service.generate_text(
        prompt=extraction_prompt,
        system_prompt="Ban la bo chuyen doi du lieu sang JSON. Uu tien dung schema va JSON hop le.",
        model=model,
        max_tokens=max_tokens or 1200,
        temperature=max(0.0, min(0.3, temperature)),
    )
    direct = _extract_json_block(extracted_text)
    if direct:
        return direct
    return _parse_best_effort_json(extracted_text)


def _build_symbol_context(
    symbol: str,
    source: str,
    interval: str,
    lookback_days: int,
) -> Dict[str, Any]:
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)
    history_raw = vnstock_api_service.call_quote(
        "history",
        source=source,
        symbol=symbol,
        show_log=False,
        method_kwargs={
            "interval": interval,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        },
    )
    overview_raw = vnstock_api_service.call_company(
        "overview",
        source=source,
        symbol=symbol,
        show_log=False,
        method_kwargs={},
    )

    history = history_raw if isinstance(history_raw, list) else []
    candles = [item for item in history if isinstance(item, dict)]
    if not candles:
        raise ValueError("No price history found for this symbol/time range.")

    latest = candles[-1]
    first = candles[0]
    latest_close = _to_float(latest.get("close"))
    first_close = _to_float(first.get("close"))
    abs_change = latest_close - first_close
    pct_change = (abs_change / first_close * 100) if first_close else 0.0

    total_volume = sum(_to_int(item.get("volume")) for item in candles)
    avg_volume = int(total_volume / len(candles)) if candles else 0

    overview_item: Dict[str, Any] = {}
    if isinstance(overview_raw, list) and overview_raw:
        first_overview = overview_raw[0]
        if isinstance(first_overview, dict):
            overview_item = first_overview
    elif isinstance(overview_raw, dict):
        overview_item = overview_raw

    technical_summary = _build_technical_summary(candles)

    return {
        "symbol": symbol,
        "source": source,
        "interval": interval,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "company": {
            "name": overview_item.get("company_name"),
            "industry": overview_item.get("industry"),
            "exchange": overview_item.get("exchange"),
            "market_cap": overview_item.get("market_cap"),
            "issue_share": overview_item.get("issue_share"),
            "website": overview_item.get("website"),
        },
        "stats": {
            "candles": len(candles),
            "first_close": round(first_close, 2),
            "latest_close": round(latest_close, 2),
            "abs_change": round(abs_change, 2),
            "pct_change": round(pct_change, 2),
            "avg_volume": avg_volume,
        },
        "technical_summary": technical_summary,
        "recent_candles": _safe_tail(candles, 25),
    }


def _build_analyze_cache_key(payload: AnalyzeSymbolRequest, normalized_symbol: str, normalized_source: str, normalized_interval: str) -> str:
    raw_key_payload = {
        "symbol": normalized_symbol,
        "source": normalized_source,
        "interval": normalized_interval,
        "lookback_days": payload.lookback_days,
        "model": payload.model or "",
        "max_tokens": payload.max_tokens,
        "temperature": payload.temperature,
        "prompt_version": "v3_structured_analyst",
    }
    digest = sha256(json.dumps(raw_key_payload, sort_keys=True).encode("utf-8")).hexdigest()
    return f"ai:analyze-symbol:{digest}"


def _build_short_analyze_cache_key(payload: AnalyzeSymbolShortRequest, symbol: str, source: str, interval: str) -> str:
    raw_key_payload = {
        "symbol": symbol,
        "source": source,
        "interval": interval,
        "lookback_days": payload.lookback_days,
        "model": payload.model or "",
        "max_tokens": payload.max_tokens,
        "temperature": payload.temperature,
        "prompt_version": "v1_short_technical_entry_exit",
    }
    digest = sha256(json.dumps(raw_key_payload, sort_keys=True).encode("utf-8")).hexdigest()
    return f"ai:analyze-symbol-short:{digest}"


@router.post("/generate")
def generate_text(payload: ClaudeGenerateRequest) -> dict:
    try:
        result = claude_service.generate_text(
            prompt=payload.prompt,
            system_prompt=payload.system_prompt,
            model=payload.model,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
        )
        return {"success": True, "data": {"text": result}}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/analyze-symbol")
def analyze_symbol(payload: AnalyzeSymbolRequest) -> dict:
    symbol = payload.symbol.strip().upper()
    source = payload.source.strip().upper()
    interval = payload.interval.strip().upper()
    try:
        cache_key = _build_analyze_cache_key(
            payload=payload,
            normalized_symbol=symbol,
            normalized_source=source,
            normalized_interval=interval,
        )
        cached_data = redis_cache_service.get_json(cache_key)
        if cached_data:
            return {"success": True, "data": cached_data, "cached": True}

        context = _build_symbol_context(
            symbol=symbol,
            source=source,
            interval=interval,
            lookback_days=payload.lookback_days,
        )
        completeness = _build_data_completeness(context)

        system_prompt = (
            "Ban la chuyen gia phan tich co phieu Viet Nam theo framework 4 goc nhin: "
            "Market analyst, Fundamentals analyst, News analyst, Social sentiment analyst. "
            "Tra loi bang tieng Viet, logic, ro rang, canh bao rui ro, khong khang dinh chac chan. "
            "Neu thieu du lieu cho mot muc, phai ghi ro gia dinh va muc do tin cay. "
            "Khong dua ra khuyen nghi dau tu bat chap rui ro."
        )

        prompt = (
            "Phan tich ma co phieu theo framework 4 analyst voi output markdown.\n"
            "Bat buoc co dung thu tu cac phan sau:\n"
            "1) Tong quan nhanh (executive summary)\n"
            "2) Market analysis (gia, xu huong, momentum, bien dong, thanh khoan)\n"
            "3) Fundamentals analysis (suc khoe doanh nghiep, chat luong tai chinh, diem manh/yeu)\n"
            "4) News analysis (tin doanh nghiep + boi canh vi mo, su kien co the tac dong gia)\n"
            "5) Social sentiment analysis (tam ly thi truong, dong thuan/phan ky quan diem)\n"
            "6) Risk matrix (cac rui ro chinh, xac suat, muc do anh huong, cach theo doi)\n"
            "7) Trading watchlist plan (kich ban Bull/Base/Bear, moc gia hoac dieu kien can theo doi)\n"
            "\n"
            "Yeu cau chat luong:\n"
            "- Moi nhan dinh quan trong can co bang chung tu du lieu dau vao.\n"
            "- Neu du lieu chua du cho fundamentals/news/social, van phai dua ra nhan dinh co dieu kien va noi ro han che.\n"
            "- Cuoi bai phai co 1 bang markdown tong hop cac muc: Goc nhin | Tin hieu chinh | Muc do tin cay | Tac dong du kien.\n"
            "- Cuoi cung phai co ket luan chuan hoa theo dung mau:\n"
            "  Bias: Bullish | Neutral | Bearish\n"
            "  Confidence: x/10\n"
            "  Luu y: confidence phai phu hop chat luong va do day cua du lieu.\n"
            "- Sau phan markdown, bat buoc tra ve them mot khoi JSON hop le trong ```json ... ``` theo schema:\n"
            "  {\n"
            '    "executive_summary": "string",\n'
            '    "market_analysis": "string",\n'
            '    "fundamentals_analysis": "string",\n'
            '    "news_analysis": "string",\n'
            '    "social_sentiment_analysis": "string",\n'
            '    "risk_matrix": [{"risk":"string","probability":"Low|Medium|High","impact":"Low|Medium|High","monitoring":"string"}],\n'
            '    "trading_watchlist_plan": {"bull":"string","base":"string","bear":"string"},\n'
            '    "bias": "Bullish|Neutral|Bearish",\n'
            '    "confidence": number,\n'
            '    "data_gaps": ["string"]\n'
            "  }\n"
            "- Giu van phong chuyen nghiep, de doc, uu tien tinh ung dung cho theo doi giao dich.\n"
            "\n"
            "Data completeness score (0-100), bat buoc dung de calibrate confidence:\n"
            f"{completeness}\n"
            "\n"
            "Du lieu dau vao:\n"
            f"{context}"
        )

        analysis_text = claude_service.generate_text(
            prompt=prompt,
            system_prompt=system_prompt,
            model=payload.model,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
        )
        structured = _extract_json_block(analysis_text)

        if not _validate_analysis_quality(analysis_text, structured):
            repair_prompt = (
                "Ban vua tra ve output chua dat quality gate. "
                "Hay viet lai day du theo dung yeu cau: co Risk matrix, Bias, Confidence va khoi JSON schema hop le.\n\n"
                f"Yeu cau goc:\n{prompt}"
            )
            analysis_text = claude_service.generate_text(
                prompt=repair_prompt,
                system_prompt=system_prompt,
                model=payload.model,
                max_tokens=payload.max_tokens,
                temperature=payload.temperature,
            )
            structured = _extract_json_block(analysis_text)
        if structured is None:
            structured = _build_structured_from_report(
                report_text=analysis_text,
                model=payload.model,
                temperature=payload.temperature,
                max_tokens=payload.max_tokens,
            )
        if structured is None:
            structured = _build_min_structured_from_text(analysis_text)

        response_data = {
            "symbol": symbol,
            "analysis": analysis_text,
            "analysis_structured": structured,
            "context": context,
            "data_completeness": completeness,
        }
        redis_cache_service.set_json(
            cache_key,
            response_data,
            ttl_seconds=max(1, int(settings.ai_cache_ttl_seconds)),
        )
        return {
            "success": True,
            "data": response_data,
            "cached": False,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/analyze-symbol-short")
def analyze_symbol_short(payload: AnalyzeSymbolShortRequest) -> dict:
    symbol = payload.symbol.strip().upper()
    source = payload.source.strip().upper()
    interval = payload.interval.strip().upper()
    try:
        cache_key = _build_short_analyze_cache_key(payload, symbol, source, interval)
        cached_data = redis_cache_service.get_json(cache_key)
        if cached_data:
            return {"success": True, "data": cached_data, "cached": True}

        context = _build_symbol_context(
            symbol=symbol,
            source=source,
            interval=interval,
            lookback_days=payload.lookback_days,
        )
        technical = context.get("technical_summary", {})
        recent_candles = context.get("recent_candles", [])

        system_prompt = (
            "Ban la technical analyst cho co phieu Viet Nam. "
            "Tap trung ky thuat ngan gon, ro rang, thuc dung. "
            "Bat buoc dua diem mua, chot loi, stop loss, va dieu kien vo hieu setup."
        )
        prompt = (
            "Phan tich ky thuat ngan gon theo bo chi so market_analyst: "
            "close_50_sma, close_200_sma, close_10_ema, macd, macds, macdh, rsi, "
            "boll, boll_ub, boll_lb, atr, vwma.\n"
            "Tra ve markdown voi DUNG 4 muc bat buoc, khong them muc khac:\n"
            "1) Diem mua\n"
            "2) Dieu kien\n"
            "3) Diem ban\n"
            "4) Stop loss\n"
            "Yeu cau: ngan gon, uu tien so lieu cu the, de lenh giao dich, khong vuot 1800 ky tu.\n\n"
            f"Symbol: {symbol}\n"
            f"Interval: {interval}\n"
            f"Lookback days: {payload.lookback_days}\n"
            f"Technical summary: {technical}\n"
            f"Recent candles (tail): {recent_candles}"
        )

        analysis_text = claude_service.generate_text(
            prompt=prompt,
            system_prompt=system_prompt,
            model=payload.model,
            max_tokens=min(1200, payload.max_tokens),
            temperature=payload.temperature,
        )
        response_data = {
            "symbol": symbol,
            "analysis": analysis_text,
            "context": {
                "interval": interval,
                "lookback_days": payload.lookback_days,
                "technical_summary": technical,
            },
        }
        redis_cache_service.set_json(
            cache_key,
            response_data,
            ttl_seconds=max(1, int(settings.ai_cache_ttl_seconds)),
        )
        return {"success": True, "data": response_data, "cached": False}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
