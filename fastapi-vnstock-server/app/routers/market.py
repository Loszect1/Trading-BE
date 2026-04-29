import json
import re
from datetime import date, timedelta
from hashlib import sha256
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import APIRouter, HTTPException, Query

from app.core.config import settings
from app.services.claude_service import ClaudeService
from app.services.redis_cache import RedisCacheService
from app.services.vnstock_api_service import VNStockApiService
from app.services.vnstock_service import VNStockService
from app.services.vn_market_holiday_calendar import is_vn_market_trading_day_live

router = APIRouter(prefix="/market", tags=["market"])
vnstock_service = VNStockService()
vnstock_api_service = VNStockApiService()
redis_cache_service = RedisCacheService()
claude_service = ClaudeService()


def _build_scanner_cache_key(as_of: date, days: int, top_n: int, use_ai: bool) -> str:
    return "market:scanner-top:" + sha256(
        f"{as_of.isoformat()}:{days}:{top_n}:{use_ai}".encode("utf-8")
    ).hexdigest()


def _build_trading_day_check_cache_key(day: date) -> str:
    return f"market:trading-day-check:{day.isoformat()}"


@router.get("/history")
def get_price_history(
    symbol: str = Query(..., min_length=3, max_length=10),
    start: str = Query(..., description="Format YYYY-MM-DD"),
    end: str = Query(..., description="Format YYYY-MM-DD"),
    interval: str = Query("1D"),
) -> dict:
    try:
        data = vnstock_service.get_price_history(
            symbol=symbol.upper(),
            start=start,
            end=end,
            interval=interval,
        )
        return {
            "symbol": symbol.upper(),
            "start": start,
            "end": end,
            "interval": interval,
            "rows": len(data),
            "data": data,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch market data: {str(exc)}",
        ) from exc


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _scan_symbol_score(symbol: str, exchange: str, start: str, end: str) -> Dict[str, Any] | None:
    try:
        rows = vnstock_api_service.call_quote(
            "history",
            source="VCI",
            symbol=symbol,
            show_log=False,
            method_kwargs={"interval": "1D", "start": start, "end": end},
        )
        if not isinstance(rows, list) or not rows:
            return None

        turnover = 0.0
        total_volume = 0.0
        volumes: List[float] = []
        latest_close = 0.0
        valid_days = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            close = _to_float(row.get("close"))
            volume = _to_float(row.get("volume"))
            if close <= 0 or volume <= 0:
                continue
            turnover += close * volume
            total_volume += volume
            volumes.append(volume)
            latest_close = close
            valid_days += 1

        if valid_days == 0:
            return None

        latest_volume = volumes[-1] if volumes else 0.0
        baseline_window = volumes[:-1]
        baseline_avg_volume = (
            (sum(baseline_window) / len(baseline_window)) if baseline_window else 0.0
        )
        volume_spike_ratio = (
            (latest_volume / baseline_avg_volume) if baseline_avg_volume > 0 else 0.0
        )
        # Only keep abnormal liquidity increase, ignore low/flat volume cases.
        if volume_spike_ratio <= 1.0:
            return None
        spike_score = volume_spike_ratio * (latest_close if latest_close > 0 else 1.0)

        return {
            "symbol": symbol,
            "exchange": exchange,
            "turnover_window": round(turnover, 2),
            "avg_volume_window": round(total_volume / valid_days, 2),
            "latest_volume": round(latest_volume, 2),
            "baseline_avg_volume": round(baseline_avg_volume, 2),
            "volume_spike_ratio": round(volume_spike_ratio, 4),
            "spike_score": round(spike_score, 4),
            "close_latest": round(latest_close, 4),
        }
    except BaseException:
        return None


def _extract_ai_json(text: str) -> Dict[str, Any] | None:
    match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", text)
    candidate = match.group(1) if match else text
    try:
        parsed = json.loads(candidate.strip())
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _ai_pick_top_symbols(
    exchange: str,
    candidates: List[Dict[str, Any]],
    top_n: int,
) -> Dict[str, Any]:
    if not candidates:
        return {"selected_symbols": [], "reasoning": "No candidates available."}

    prompt = (
        "You are a Vietnam stock market scanner assistant specialized in unusual liquidity detection. "
        "Your primary objective is to identify symbols with abnormally high liquidity spikes "
        "that are still tradable and not pure noise.\n"
        f"Exchange: {exchange}\n"
        f"Top required: {top_n}\n"
        "Selection rules (strict priority):\n"
        "1) Prioritize higher volume_spike_ratio and spike_score.\n"
        "2) Reject low-quality spikes: very low baseline_avg_volume or one-off micro-liquidity noise.\n"
        "3) Prefer symbols with both strong spike and reasonable absolute liquidity (latest_volume and turnover_window).\n"
        "4) Keep picks diversified; avoid selecting near-duplicate behavior when better alternatives exist.\n"
        "Return ONLY valid JSON with this schema:\n"
        "{\n"
        '  "selected_symbols": ["SYM1", "SYM2"],\n'
        '  "reasoning": "short explanation focused on abnormal liquidity signals",\n'
        '  "risk_by_symbol": {"SYM1":"Low|Medium|High","SYM2":"Low|Medium|High"}\n'
        "}\n"
        "Candidates:\n"
        f"{candidates}"
    )
    try:
        ai_text = claude_service.generate_text(
            prompt=prompt,
            system_prompt=(
                "You are an equity scanner. Be concise, evidence-based, and return strict JSON only."
            ),
            model=settings.claude_model,
            max_tokens=min(1200, settings.claude_max_tokens),
            temperature=0.2,
        )
        parsed = _extract_ai_json(ai_text)
        if not parsed:
            return {"selected_symbols": [], "reasoning": "AI output parse failed."}
        return parsed
    except Exception:
        return {"selected_symbols": [], "reasoning": "AI scan failed, fallback to turnover rank."}


@router.get("/scanner-top")
def scanner_top_symbols(
    days: int = Query(90, ge=2, le=365),
    top_n: int = Query(5, ge=1, le=20),
    force_refresh: bool = Query(False),
    use_ai: bool = Query(True),
    max_scan_per_exchange: int = Query(20, ge=10, le=300),
) -> Dict[str, Any]:
    try:
        as_of = date.today()
        start_date = as_of - timedelta(days=days)
        cache_key = _build_scanner_cache_key(as_of, days, top_n, use_ai)
        normal_cache_key = _build_scanner_cache_key(as_of, days, top_n, False)
        cached = None if force_refresh else redis_cache_service.get_json(cache_key)
        if cached is not None:
            return cached

        exchanges = ("HOSE", "HNX", "UPCOM")
        symbols_by_exchange: Dict[str, List[str]] = {exchange: [] for exchange in exchanges}
        # vnstock `all_symbols` may not include exchange in some versions.
        # Prefer symbols_by_exchange and hard-filter by `exchange` field.
        for exchange in exchanges:
            try:
                listing_raw = vnstock_api_service.call_listing(
                    "symbols_by_exchange",
                    method_kwargs={"exchange": exchange},
                )
            except Exception:
                listing_raw = []
            rows = listing_raw if isinstance(listing_raw, list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol", "")).strip().upper()
                row_exchange = str(row.get("exchange", "")).strip().upper()
                if not symbol:
                    continue
                if row_exchange and row_exchange != exchange:
                    continue
                symbols_by_exchange[exchange].append(symbol)

        # De-duplicate while keeping stable order.
        for exchange in exchanges:
            seen = set()
            deduped = []
            for symbol in symbols_by_exchange[exchange]:
                if symbol in seen:
                    continue
                seen.add(symbol)
                deduped.append(symbol)
            symbols_by_exchange[exchange] = deduped
            if len(symbols_by_exchange[exchange]) > max_scan_per_exchange:
                symbols_by_exchange[exchange] = symbols_by_exchange[exchange][:max_scan_per_exchange]

        by_exchange: Dict[str, List[Dict[str, Any]]] = {exchange: [] for exchange in exchanges}
        tasks = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            for exchange in exchanges:
                for symbol in symbols_by_exchange[exchange]:
                    tasks.append(
                        executor.submit(
                            _scan_symbol_score,
                            symbol,
                            exchange,
                            start_date.isoformat(),
                            as_of.isoformat(),
                        )
                    )

            for future in as_completed(tasks):
                try:
                    item = future.result()
                except BaseException:
                    continue
                if not item:
                    continue
                ex = item["exchange"]
                by_exchange[ex].append(item)

        ai_reasoning_by_exchange: Dict[str, str] = {}
        ai_risk_by_exchange: Dict[str, Dict[str, str]] = {}
        for exchange in exchanges:
            by_exchange[exchange].sort(key=lambda item: item["spike_score"], reverse=True)
            candidates = by_exchange[exchange][: max(top_n * 3, top_n)]
            if use_ai and candidates:
                ai_result = _ai_pick_top_symbols(exchange, candidates, top_n)
                ai_reasoning_by_exchange[exchange] = str(ai_result.get("reasoning", "")).strip()
                raw_risk = ai_result.get("risk_by_symbol")
                risk_map: Dict[str, str] = {}
                if isinstance(raw_risk, dict):
                    for key, value in raw_risk.items():
                        symbol = str(key).strip().upper()
                        level = str(value).strip().capitalize()
                        if symbol and level in {"Low", "Medium", "High"}:
                            risk_map[symbol] = level
                ai_risk_by_exchange[exchange] = risk_map
                selected_symbols = [
                    str(item).strip().upper()
                    for item in (ai_result.get("selected_symbols") or [])
                    if str(item).strip()
                ]
                selected_set = set(selected_symbols)
                selected_rows = [row for row in candidates if row["symbol"] in selected_set]
                if len(selected_rows) < top_n:
                    used = {row["symbol"] for row in selected_rows}
                    for row in candidates:
                        if row["symbol"] in used:
                            continue
                        selected_rows.append(row)
                        used.add(row["symbol"])
                        if len(selected_rows) >= top_n:
                            break
                by_exchange[exchange] = selected_rows[:top_n]
            else:
                by_exchange[exchange] = candidates[:top_n]

        response = {
            "scanned_days": days,
            "as_of": as_of.isoformat(),
            "by_exchange": by_exchange,
            "ai_used": use_ai,
            "ai_reasoning_by_exchange": ai_reasoning_by_exchange,
            "ai_risk_by_exchange": ai_risk_by_exchange,
        }
        # If AI mode returns empty due upstream rate limit/noisy errors, fallback to normal cached scan.
        if use_ai and all(len(by_exchange.get(exchange, [])) == 0 for exchange in exchanges):
            fallback = redis_cache_service.get_json(normal_cache_key)
            if fallback:
                fallback["ai_used"] = True
                fallback["ai_reasoning_by_exchange"] = {
                    "HOSE": "Fallback to normal cached scan due temporary data source limit.",
                    "HNX": "Fallback to normal cached scan due temporary data source limit.",
                    "UPCOM": "Fallback to normal cached scan due temporary data source limit.",
                }
                fallback["ai_risk_by_exchange"] = {}
                return fallback
        redis_cache_service.set_json(
            cache_key,
            response,
            ttl_seconds=max(1, int(settings.ai_cache_ttl_seconds)),
        )
        return response
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to scan top symbols: {str(exc)}",
        ) from exc


@router.get("/trading-day/check")
def check_vn_trading_day(
    day: str = Query(..., description="Date to check, format YYYY-MM-DD"),
    force_refresh: bool = Query(False, description="Bypass Redis cache and probe live market data"),
) -> dict:
    try:
        target_day = date.fromisoformat(day.strip())
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD") from exc
    cache_key = _build_trading_day_check_cache_key(target_day)
    if not force_refresh:
        cached = redis_cache_service.get_json(cache_key)
        if isinstance(cached, dict):
            return {"success": True, "data": cached}
    is_open = is_vn_market_trading_day_live(target_day)
    if target_day.weekday() >= 5:
        reason = "weekend"
    else:
        reason = "open" if is_open else "closed_no_session_data"
    data = {
        "date": target_day.isoformat(),
        "is_trading_day": bool(is_open),
        "reason": reason,
        "method": "live_market_data_probe",
        "probe_symbol": "VNINDEX",
    }
    # Keep one-day cache per checked date to reduce repeated upstream probes.
    redis_cache_service.set_json(cache_key, data, ttl_seconds=86_400 + 600)
    return {"success": True, "data": data}
