"""
Modular signal quality scoring: technical, news (RSS snapshot), macro/fundamental proxy.

Designed for production-safe use inside the signal engine: external calls are wrapped
in try/except at the boundaries; callers receive structured scores + explainability.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Sequence

from psycopg import connect

from app.core.config import settings

SCORING_PIPELINE_VERSION = "1.2.2"

# Deterministic post-lexical sentiment shaping (no external APIs).
SENTIMENT_INTERPRETATION_LAYER_VERSION = "1.1.0"

# Per-article lexical tilt (p - r units) thresholds for cross-article consistency.
_CROSS_ARTICLE_TILT_STRONG = 0.42

# Default weights (must sum to 1.0 for documented composite; callers may override for tests).
DEFAULT_WEIGHTS: dict[str, float] = {
    "technical": 0.45,
    "news": 0.30,
    "macro_fundamental": 0.25,
}


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _norm_text(text: str) -> str:
    return _strip_accents(text or "").upper()


# Keyword lists are conservative: used only to shift a bounded score, not as trading advice.
_RISK_TERMS = (
    "PHA SAN",
    "KHOI TO",
    "BAT GIAM",
    "DIEN TRA",
    "THAM NHUNG",
    "GIAN LAN",
    "CANH BAO",
    "BI PHAT",
    "BI CAM",
    "THUA LO LON",
    "LO LIEN TUC",
    "NEGATIVE OUTLOOK",
    "DOWNGRADE",
    "BANKRUPTCY",
    "FRAUD",
    "PROBE",
    "SANCTION",
    "DEFAULT",
    "CRISIS",
    "SCANDAL",
    "ARREST",
    "VI PHAM",
    "RUI RO CAO",
)

_POSITIVE_TERMS = (
    "TANG TRUONG",
    "KY LUC",
    "HOP DONG",
    "MO RONG",
    "CO TUC",
    "UPGRADE",
    "TICH CUC",
    "TANG LOI NHUAN",
    "RECORD",
    "EXPANSION",
    "PARTNERSHIP",
    "BREAKTHROUGH",
    "OUTPERFORM",
)

# Multi-token phrases (normalized) — counted with higher weight than isolated keywords.
_RISK_PHRASES = (
    "BI DIEU TRA",
    "MO DIEU TRA",
    "CANH BAO RUI RO",
    "GIAN LAN TAI CHINH",
    "CREDIT DOWNGRADE",
    "NO GO DEAL",
    "THAT BAI THAU",
    "MAT KHAI THAC",
    "LO LIEN KY",
)

_POSITIVE_PHRASES = (
    "TANG TRUONG MANH",
    "DAT KY LUC",
    "KY LUC MOI",
    "VON HOA TANG",
    "TANG TRUONG VUOT BAC",
    "HOAN THANH CHI TIEU",
)

# Topic buckets (upper hay): tune lexical *interpretation* only; longer needles first where needed.
_MACRO_TOPIC_NEEDLES = (
    "MONETARY POLICY",
    "QUANTITATIVE TIGHTENING",
    "INTEREST RATE",
    "RECESSION RISK",
    "INFLATION TARGET",
    "USD/VND",
    "FED RATE",
    "FOMC",
    "NHNN",
    "LAI SUAT",
    "TY GIA",
    "INFLATION",
    "RECESSION",
    "CPI",
)

_CORPORATE_TOPIC_NEEDLES = (
    "PHAT HANH TRAI PHIEU",
    "THUONG VU M&A",
    "MUA CO PHAN",
    "HOP DONG TRI GIA",
    "HOP DONG",
    "CO TUC",
    "PHAT HANH CO PHIEU",
    "PRIVATE PLACEMENT",
    "IPO",
)

# Space-padded markers in a *prefix* window before a hit (substring search on uppercase hay).
_NEGATION_PREFIX_MARKERS = (
    " KHONG ",
    " CHUA ",
    " KHONG PHAI ",
    " KHONG CO ",
    " CHANG ",
    " NOT ",
    " WITHOUT ",
    " NEVER ",
    " DENIES ",
    " DENIAL ",
    " UNFOUNDED ",
    " PHU NHAN ",
    " BO QUA ",
    " NO EVIDENCE ",
    " FALSE RUMOR",
    " RUMORS UNTRUE",
)

# Curated per-feed weights (deterministic). Unknown RSS ids get a neutral default.
_NEWS_SOURCE_BASE_WEIGHT: dict[str, float] = {
    "vietstock_thi_truong_ck": 1.12,
    "vietstock_nhan_dinh": 1.12,
    "vnexpress_kinh_doanh": 1.08,
    "thanhnien_kinh_te": 1.06,
    "cafebiz_home": 1.02,
    "reuters_business": 1.08,
    "bbc_business": 1.05,
    "bloomberg_markets": 1.08,
    "yahoo_news_top": 0.92,
    "reddit_wallstreetbets": 0.72,
    "reddit_stocks": 0.78,
    "reddit_investing": 0.80,
    "reddit_securityanalysis": 0.82,
}


def _news_source_weight(source_id: str | None) -> float:
    sid = (source_id or "").strip()
    if not sid:
        return 0.95
    return float(_NEWS_SOURCE_BASE_WEIGHT.get(sid, 0.98))


def _news_category_weight(category: str | None) -> float:
    c = (category or "").strip().lower()
    if c == "domestic":
        return 1.05
    if c == "world":
        return 1.0
    if c == "social":
        return 0.88
    return 0.98


def _recency_weight_hours(age_hours: float, window_hours: float) -> float:
    """Linear decay from 1.0 (fresh) to 0.35 at the trailing edge of the window."""
    if window_hours <= 0:
        return 1.0
    x = _clamp(age_hours / window_hours, 0.0, 1.0)
    return 1.0 - 0.65 * x


def _item_age_hours_utc(now: datetime, dt: datetime | None) -> float:
    if dt is None:
        return 0.0
    if dt > now:
        return 0.0
    return max(0.0, (now - dt).total_seconds() / 3600.0)


def _symbol_token_pattern(symbol: str) -> re.Pattern[str]:
    sym = re.escape(symbol.strip().upper())
    return re.compile(rf"\b{sym}\b")


def _text_mentions_symbol(text: str, symbol: str) -> bool:
    if not text or not symbol:
        return False
    sym_u = symbol.strip().upper()
    if len(sym_u) < 2:
        return False
    hay = _norm_text(text)
    if sym_u in hay:
        return bool(_symbol_token_pattern(sym_u).search(hay))
    return False


def _count_term_hits(norm_hay: str, terms: Iterable[str]) -> int:
    hits = 0
    for term in terms:
        t = _norm_text(term)
        if len(t) >= 3 and t in norm_hay:
            hits += 1
    return hits


def _substring_start_indices(norm_hay: str, needle: str) -> list[int]:
    if len(needle) < 2:
        return []
    out: list[int] = []
    start = 0
    while True:
        i = norm_hay.find(needle, start)
        if i < 0:
            return out
        out.append(i)
        start = i + 1


def _negated_before(norm_hay: str, hit_start: int, window: int = 72) -> bool:
    lo = max(0, hit_start - window)
    buf = f" {norm_hay[lo:hit_start]} "
    return any(m in buf for m in _NEGATION_PREFIX_MARKERS)


def _effective_keyword_units(norm_hay: str, terms: Iterable[str]) -> tuple[float, int]:
    """
    Sum per-hit weight (1.0 normally, 0.25 when negated prefix) + count suppressed full hits.
    """
    units = 0.0
    suppressed = 0
    for term in terms:
        t = _norm_text(term)
        if len(t) < 3:
            continue
        for idx in _substring_start_indices(norm_hay, t):
            if _negated_before(norm_hay, idx):
                units += 0.25
                suppressed += 1
            else:
                units += 1.0
    return units, suppressed


_PHRASE_WEIGHT = 1.75


def _phrase_weighted_units(norm_hay: str, phrases: Iterable[str]) -> tuple[float, int]:
    units = 0.0
    suppressed = 0
    for phrase in phrases:
        p = _norm_text(phrase)
        if len(p) < 6:
            continue
        for idx in _substring_start_indices(norm_hay, p):
            if _negated_before(norm_hay, idx):
                units += 0.25 * _PHRASE_WEIGHT
                suppressed += 1
            else:
                units += _PHRASE_WEIGHT
    return units, suppressed


def _news_item_text(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("title") or ""),
        str(item.get("summary") or ""),
    ]
    return " ".join(parts)


def _news_item_published_utc(item: dict[str, Any]) -> datetime | None:
    published_raw = item.get("published_at")
    if not isinstance(published_raw, str) or not published_raw.strip():
        return None
    try:
        iso = published_raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _infer_article_topic(norm_hay: str) -> str:
    """Coarse topic tag for interpretation weighting (deterministic substring rules)."""
    padded = f" {norm_hay} "
    for needle in _MACRO_TOPIC_NEEDLES:
        n = _norm_text(needle)
        if len(n) < 4:
            if n == "CPI" and "CPI" in norm_hay:
                return "macro"
            if n == "FOMC" and "FOMC" in norm_hay:
                return "macro"
            if n == "NHNN" and "NHNN" in norm_hay:
                return "macro"
            continue
        if n in norm_hay:
            return "macro"
    if " FED " in padded or padded.startswith("FED ") or "THE FED" in norm_hay:
        return "macro"
    for needle in _CORPORATE_TOPIC_NEEDLES:
        n = _norm_text(needle)
        if len(n) >= 4 and n in norm_hay:
            return "corporate"
    return "general"


def _apply_sentiment_interpretation_layer(
    *,
    base_score: float,
    articles_in_window: int,
    weight_sum: float,
    per_article: list[dict[str, Any]],
) -> tuple[float, dict[str, Any]]:
    """
    Deterministic post-lexical adjustments:
    - Contradiction dampening when the same article carries strong opposing lexical units.
    - Cross-source consensus: shrink when credible feeds disagree; mild stretch when many agree.
    - Cross-article lexical alignment: dampen when different articles lean opposite directions;
      mild stretch when three-plus articles share the same strong tilt and the blended residual agrees.
    - Topic rescale: macro-heavy risk windows slightly amplify bearish tilt; dampen bullish reads.
    """
    if articles_in_window <= 0 or not per_article:
        return base_score, {"sentiment_layer_applied": False}

    ws = float(weight_sum) if weight_sum > 0 else 1.0

    contradiction_units = 0.0
    macro_w_risk = 0.0
    macro_w_sum = 0.0

    for row in per_article:
        w_eff = float(row["w_eff"])
        r_i = float(row["r"])
        p_i = float(row["p"])
        topic = str(row.get("topic") or "general")
        w_n = w_eff / ws

        if min(r_i, p_i) >= 0.85 and max(r_i, p_i) >= 1.1:
            topic_mult = 1.22 if topic == "macro" else 0.82 if topic == "corporate" else 1.0
            contradiction_units += w_n * min(r_i, p_i) * topic_mult

        if topic == "macro":
            macro_w_risk += w_eff * r_i
            macro_w_sum += w_eff

    contradiction_factor = 1.0 - _clamp(1.85 * contradiction_units, 0.0, 0.24)

    net_by_src: dict[str, float] = {}
    for row in per_article:
        src = str(row.get("src") or "").strip() or "_unknown"
        prev = net_by_src.get(src, 0.0)
        net_by_src[src] = prev + float(row["w_eff"]) * (float(row["p"]) - float(row["r"]))

    signs: list[int] = []
    for v in net_by_src.values():
        if v > 0.22:
            signs.append(1)
        elif v < -0.22:
            signs.append(-1)

    has_bull = any(s > 0 for s in signs)
    has_bear = any(s < 0 for s in signs)
    strong_aligned = len([s for s in signs if s != 0]) >= 2 and (all(s >= 0 for s in signs) or all(s <= 0 for s in signs))

    if has_bull and has_bear:
        consensus_factor = 0.89
        consensus_label = "mixed_sources"
    elif strong_aligned:
        consensus_factor = 1.035
        consensus_label = "aligned_sources"
    else:
        consensus_factor = 1.0
        consensus_label = "neutral_thin_or_single_sided"

    bullish_articles = 0
    bearish_articles = 0
    for row in per_article:
        tilt = float(row["p"]) - float(row["r"])
        if tilt > _CROSS_ARTICLE_TILT_STRONG:
            bullish_articles += 1
        elif tilt < -_CROSS_ARTICLE_TILT_STRONG:
            bearish_articles += 1

    cross_article_factor = 1.0
    cross_article_label = "neutral_lexical_spread"
    if bullish_articles >= 1 and bearish_articles >= 1:
        cross_article_factor = 0.955
        cross_article_label = "mixed_article_lexical_tilts"
    elif articles_in_window >= 3:
        if bullish_articles >= 2 and bearish_articles == 0:
            cross_article_factor = 1.022
            cross_article_label = "aligned_bullish_articles"
        elif bearish_articles >= 2 and bullish_articles == 0:
            cross_article_factor = 1.022
            cross_article_label = "aligned_bearish_articles"

    residual = float(base_score) - 50.0
    macro_share = macro_w_sum / ws if ws > 0 else 0.0
    macro_risk_density = macro_w_risk / ws if ws > 0 else 0.0

    topic_factor = 1.0
    if macro_share >= 0.34 and macro_risk_density >= 0.35:
        if residual > 0:
            topic_factor = max(0.90, 1.0 - 0.11 * min(macro_share, 1.0))
        elif residual < 0:
            topic_factor = min(1.10, 1.0 + 0.08 * min(macro_share, 1.0))

    car_f = float(cross_article_factor)
    if cross_article_label == "aligned_bullish_articles" and residual <= 0.5:
        car_f = 1.0
        cross_article_label = "aligned_bullish_articles_skipped_residual_not_bullish"
    elif cross_article_label == "aligned_bearish_articles" and residual >= -0.5:
        car_f = 1.0
        cross_article_label = "aligned_bearish_articles_skipped_residual_not_bearish"

    new_residual = residual * contradiction_factor * consensus_factor * topic_factor * car_f
    adjusted = _clamp(50.0 + new_residual, 0.0, 100.0)

    detail = {
        "sentiment_layer_applied": True,
        "layer_version": SENTIMENT_INTERPRETATION_LAYER_VERSION,
        "score_before_layer": round(float(base_score), 4),
        "delta_vs_base": round(adjusted - float(base_score), 4),
        "factors": {
            "contradiction_factor": round(contradiction_factor, 4),
            "contradiction_units_proxy": round(contradiction_units, 4),
            "consensus_factor": round(consensus_factor, 4),
            "consensus_label": consensus_label,
            "cross_article_factor": round(car_f, 4),
            "cross_article_label": cross_article_label,
            "cross_article_bullish_count": bullish_articles,
            "cross_article_bearish_count": bearish_articles,
            "topic_rescale_factor": round(topic_factor, 4),
            "macro_topic_weight_share": round(macro_share, 4),
            "macro_weighted_risk_density": round(macro_risk_density, 4),
        },
    }
    return adjusted, detail


def score_news_for_symbol(
    symbol: str,
    news_items: Sequence[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
    window_hours: int = 72,
    scoring_mode: str = "weighted_lexical_v2",
) -> dict[str, Any]:
    """
    Heuristic 0-100 news score from a pre-fetched RSS snapshot (no network).

    - ``weighted_lexical_v2`` (default): keyword + weighted phrase patterns, negation-aware
      prefix window, per-article RSS weights, linear recency decay, calibration metadata,
      plus a bounded deterministic sentiment-interpretation layer (contradictions, cross-source
      consensus, cross-article lexical alignment, macro-topic rescale) recorded under
      ``explainability.sentiment_interpretation``.
    - ``weighted_lexical_v1``: keyword-only weighted masses (pre-1.2.0 behavior).
    - ``legacy``: pre-1.1.0 global-hit aggregation (audit / A-B).
    """
    sym = symbol.strip().upper()
    now = now_utc or datetime.now(timezone.utc)
    window_start = now - timedelta(hours=max(1, int(window_hours)))
    matched: list[dict[str, Any]] = []
    risk_hits = 0
    pos_hits = 0
    weighted_risk_mass = 0.0
    weighted_pos_mass = 0.0
    weight_sum = 0.0
    articles_in_window = 0
    source_ids: list[str] = []
    negation_suppressed_total = 0
    phrase_risk_units_acc = 0.0
    phrase_pos_units_acc = 0.0
    mean_article_reliability = 0.0
    per_article: list[dict[str, Any]] = []

    if not news_items:
        return {
            "score_0_100": 50.0,
            "neutral_because": "no_news_items_in_snapshot",
            "matched_headlines": [],
            "risk_term_hits_window": 0,
            "positive_term_hits_window": 0,
            "symbol_specific_items": 0,
            "window_hours": int(window_hours),
            "scoring_mode": scoring_mode,
        }

    wh = float(max(1, int(window_hours)))
    rel_accum = 0.0

    for item in news_items:
        dt = _news_item_published_utc(item)
        if dt is not None and dt < window_start:
            continue
        articles_in_window += 1
        blob = _news_item_text(item)
        hay = _norm_text(blob)
        r_raw = _count_term_hits(hay, _RISK_TERMS)
        p_raw = _count_term_hits(hay, _POSITIVE_TERMS)
        risk_hits += r_raw
        pos_hits += p_raw

        if scoring_mode == "weighted_lexical_v2":
            r_kw_u, sup_r_kw = _effective_keyword_units(hay, _RISK_TERMS)
            p_kw_u, sup_p_kw = _effective_keyword_units(hay, _POSITIVE_TERMS)
            r_ph_u, sup_r_ph = _phrase_weighted_units(hay, _RISK_PHRASES)
            p_ph_u, sup_p_ph = _phrase_weighted_units(hay, _POSITIVE_PHRASES)
            negation_suppressed_total += sup_r_kw + sup_p_kw + sup_r_ph + sup_p_ph
            phrase_risk_units_acc += r_ph_u
            phrase_pos_units_acc += p_ph_u
            r_i = float(r_kw_u + r_ph_u)
            p_i = float(p_kw_u + p_ph_u)
        else:
            r_i = float(r_raw)
            p_i = float(p_raw)

        age_h = _item_age_hours_utc(now, dt)
        w_rec = _recency_weight_hours(age_h, wh)
        w_src = _news_source_weight(str(item.get("source_id") or ""))
        w_cat = _news_category_weight(str(item.get("category") or ""))
        w_base = w_rec * w_src * w_cat
        sym_specific = _text_mentions_symbol(blob, sym)
        sym_mult = 1.35 if sym_specific else 1.0
        w_eff = w_base * sym_mult
        weight_sum += w_eff
        weighted_risk_mass += w_eff * float(r_i)
        weighted_pos_mass += w_eff * float(p_i)
        rel_accum += w_src * w_cat
        sid = str(item.get("source_id") or "").strip()
        per_article.append(
            {
                "w_eff": float(w_eff),
                "r": float(r_i),
                "p": float(p_i),
                "src": sid,
                "topic": _infer_article_topic(hay),
            }
        )
        if sid:
            source_ids.append(sid)

        if sym_specific:
            matched.append(
                {
                    "title": (item.get("title") or "")[:160],
                    "source_id": item.get("source_id"),
                    "category": item.get("category"),
                    "published_at": item.get("published_at"),
                }
            )

    symbol_boost = min(20.0, len(matched) * 6.0) if matched else 0.0

    if scoring_mode == "legacy":
        score = 50.0 + symbol_boost + min(20.0, pos_hits * 4.0) - min(45.0, risk_hits * 6.0)
    else:
        # weighted_lexical_v1 and v2: same blend; v2 differs by per-article lexical units in-loop.
        lexical_shift = weighted_pos_mass * 5.0 - weighted_risk_mass * 7.0
        lexical_shift = _clamp(lexical_shift, -48.0, 22.0)
        score = 50.0 + symbol_boost + lexical_shift

    score = _clamp(score, 0.0, 100.0)

    sentiment_layer_detail: dict[str, Any]
    if scoring_mode == "legacy":
        sentiment_layer_detail = {"sentiment_layer_applied": False, "reason": "legacy_mode"}
    else:
        score, sentiment_layer_detail = _apply_sentiment_interpretation_layer(
            base_score=score,
            articles_in_window=articles_in_window,
            weight_sum=weight_sum,
            per_article=per_article,
        )
        score = _clamp(score, 0.0, 100.0)

    distinct_sources = len(set(source_ids))
    if articles_in_window > 0:
        mean_article_reliability = rel_accum / float(articles_in_window)

    coverage = _clamp(float(articles_in_window) * 12.0 + float(distinct_sources) * 10.0, 0.0, 100.0)
    lex_strength = max(weighted_risk_mass, weighted_pos_mass) + 0.45 * min(weighted_risk_mass, weighted_pos_mass)
    ambiguity = "low"
    if articles_in_window >= 4 and lex_strength < 0.9 and len(matched) == 0:
        ambiguity = "high"
    elif articles_in_window >= 2 and lex_strength < 0.35:
        ambiguity = "med"

    clarity = 92.0 if lex_strength >= 1.6 else 70.0 if lex_strength >= 0.45 else 38.0
    if ambiguity == "high":
        clarity -= 18.0
    elif ambiguity == "med":
        clarity -= 8.0
    clarity = _clamp(clarity, 0.0, 100.0)

    reliability_norm = _clamp((mean_article_reliability - 0.72) / 0.48 * 100.0, 0.0, 100.0)
    confidence_calibration_0_100 = round(
        _clamp(0.48 * coverage + 0.34 * clarity + 0.18 * reliability_norm, 0.0, 100.0),
        2,
    )

    out: dict[str, Any] = {
        "score_0_100": round(score, 2),
        "matched_headlines": matched[:8],
        "risk_term_hits_window": risk_hits,
        "positive_term_hits_window": pos_hits,
        "symbol_specific_items": len(matched),
        "window_hours": int(window_hours),
        "scoring_mode": scoring_mode,
        "weighted_risk_mass": round(weighted_risk_mass, 4),
        "weighted_positive_mass": round(weighted_pos_mass, 4),
        "article_weight_sum": round(weight_sum, 4),
    }
    if scoring_mode != "legacy":
        out["confidence_calibration_0_100"] = confidence_calibration_0_100
        out["explainability"] = {
            "articles_in_window": articles_in_window,
            "distinct_source_ids": distinct_sources,
            "coverage_score_0_100": round(coverage, 2),
            "lexical_ambiguity": ambiguity,
            "lexical_strength_proxy": round(lex_strength, 4),
            "mean_article_reliability_weight": round(mean_article_reliability, 4),
            "negation_suppressed_hits": negation_suppressed_total,
            "phrase_risk_weighted_units": round(phrase_risk_units_acc, 4),
            "phrase_positive_weighted_units": round(phrase_pos_units_acc, 4),
            "phrase_weight": _PHRASE_WEIGHT,
            "negation_prefix_window_chars": 72,
            "sentiment_interpretation": sentiment_layer_detail,
        }
    return out


def _pct_return(closes: Sequence[float]) -> float | None:
    if len(closes) < 2 or closes[0] <= 0 or closes[-1] <= 0:
        return None
    return (closes[-1] - closes[0]) / closes[0] * 100.0


def _pct_return_slice(closes: Sequence[float], bars: int) -> float | None:
    if bars < 2 or len(closes) < bars:
        return None
    sl = closes[-bars:]
    return _pct_return(sl)


def _std_daily_returns_pct(closes: Sequence[float]) -> float | None:
    if len(closes) < 6:
        return None
    rets: list[float] = []
    for i in range(1, len(closes)):
        a, b = float(closes[i - 1]), float(closes[i])
        if a > 0 and b > 0:
            rets.append((b - a) / a * 100.0)
    if len(rets) < 3:
        return None
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / max(1, len(rets) - 1)
    return var**0.5


def _diff_to_rs_score(diff: float) -> float:
    if diff >= 3.0:
        return 78.0
    if diff >= 1.0:
        return 65.0
    if diff >= -1.0:
        return 52.0
    if diff >= -4.0:
        return 40.0
    return 28.0


def _revenue_column_key(sample: dict[str, Any]) -> str | None:
    best: tuple[int, str] | None = None
    for k in sample:
        lk = str(k).lower()
        if any(x in lk for x in ("cagr", "yoy", "qoq", "growth", "margin", "eps")):
            continue
        score = 0
        if "revenue" in lk or "net sales" in lk:
            score += 5
        if "doanh thu" in lk or "doanh_thu" in lk:
            score += 5
        if "total" in lk and "revenue" in lk:
            score += 2
        if "net" in lk and ("revenue" in lk or "sales" in lk):
            score += 1
        if "cost" in lk and "revenue" not in lk:
            score -= 10
        if score > 0 and (best is None or score > best[0]):
            best = (score, str(k))
    return best[1] if best else None


def _financial_statement_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    return []


def _income_growth_adjustment(rows: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
    hint: dict[str, Any] = {}
    if len(rows) < 2:
        hint["note"] = "income_rows_lt_2"
        return 0.0, hint
    key = _revenue_column_key(rows[0])
    if not key:
        hint["note"] = "no_revenue_like_column"
        return 0.0, hint
    v0 = _to_float(rows[0].get(key))
    v1 = _to_float(rows[1].get(key))
    hint["revenue_key_used"] = key
    if v0 is None or v1 is None or v1 == 0:
        hint["note"] = "revenue_values_missing"
        return 0.0, hint
    g = (v0 - v1) / abs(v1) * 100.0
    hint["revenue_qoq_or_seq_pct"] = round(g, 4)
    adj = 0.0
    if g >= 12:
        adj = 10.0
    elif g >= 5:
        adj = 6.0
    elif g >= 0:
        adj = 2.0
    elif g >= -8:
        adj = -4.0
    else:
        adj = -10.0
    return adj, hint


def _balance_leverage_adjustment(rows: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
    hint: dict[str, Any] = {}
    if not rows:
        hint["note"] = "no_balance_rows"
        return 0.0, hint
    row = rows[0]
    de = None
    for k, v in row.items():
        lk = str(k).lower()
        if ("debt" in lk and "equity" in lk) or lk in ("d/e", "de_ratio", "debt_to_equity"):
            de = _to_float(v)
            hint["debt_equity_key"] = str(k)
            break
    if de is None:
        tl = te = None
        for k, v in row.items():
            lk = str(k).lower()
            if tl is None and "total" in lk and "liabilit" in lk:
                tl = _to_float(v)
            if te is None and "total" in lk and "equit" in lk:
                te = _to_float(v)
        if tl is not None and te is not None and te > 0:
            de = tl / te
            hint["debt_equity_proxy"] = "total_liabilities/total_equity"
    if de is None:
        hint["note"] = "no_leverage_column"
        return 0.0, hint
    hint["debt_to_equity"] = round(de, 4)
    if de <= 0.8:
        return 6.0, hint
    if de <= 1.5:
        return 2.0, hint
    if de <= 3.0:
        return -3.0, hint
    return -8.0, hint


def _cash_flow_adjustment(rows: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
    hint: dict[str, Any] = {}
    if len(rows) < 2:
        hint["note"] = "cash_flow_rows_lt_2"
        return 0.0, hint
    cfo_key = None
    for k in rows[0]:
        lk = str(k).lower()
        if "operating" in lk and ("cash" in lk or "flow" in lk or "cfo" in lk):
            cfo_key = str(k)
            break
    if cfo_key is None:
        for k in rows[0]:
            lk = str(k).lower()
            if "luu chuyen" in lk or "hoat dong" in lk:
                cfo_key = str(k)
                break
    if not cfo_key:
        hint["note"] = "no_cfo_column"
        return 0.0, hint
    c0 = _to_float(rows[0].get(cfo_key))
    c1 = _to_float(rows[1].get(cfo_key))
    hint["cfo_key_used"] = cfo_key
    if c0 is None or c1 is None:
        return 0.0, hint
    hint["cfo_last_two"] = [c0, c1]
    if c0 > 0 and c1 > 0 and c0 >= c1 * 0.95:
        return 4.0, hint
    if c0 > 0 > c1:
        return 5.0, hint
    if c0 < 0 and c1 < 0:
        return -6.0, hint
    if c0 < 0 <= c1:
        return -4.0, hint
    return 0.0, hint


def _trading_microstructure_adjustment(
    api_call_trading: Callable[..., Any] | None,
    sym: str,
) -> tuple[float, dict[str, Any]]:
    """
    Optional vnstock trading snapshot: buy/sell imbalance + foreign flow labels.
    Returns bounded RS adjustment in [-5, +5] and a detail dict (errors swallowed per call).
    """
    hint: dict[str, Any] = {}
    if api_call_trading is None:
        hint["note"] = "api_call_trading_none"
        return 0.0, hint
    adj = 0.0

    try:
        raw_stats = api_call_trading(
            "trading_stats",
            source="VCI",
            symbol=sym,
            show_log=False,
        )
    except Exception as exc:
        hint["trading_stats"] = {"error": f"{type(exc).__name__}: {exc}"}
    else:
        if isinstance(raw_stats, dict):
            hint["trading_stats"] = {
                "buy_volume": raw_stats.get("buy_volume"),
                "sell_volume": raw_stats.get("sell_volume"),
            }
            bv = _to_float(raw_stats.get("buy_volume"))
            sv = _to_float(raw_stats.get("sell_volume"))
            if bv is not None and sv is not None and bv + sv > 0:
                imb = (bv - sv) / (bv + sv)
                hint["buy_sell_imbalance"] = round(imb, 4)
                if imb >= 0.18:
                    adj += 2.5
                elif imb <= -0.18:
                    adj -= 2.5

    try:
        ft = api_call_trading(
            "foreign_trade",
            source="VCI",
            symbol=sym,
            show_log=False,
        )
    except Exception as exc:
        hint["foreign_trade"] = {"error": f"{type(exc).__name__}: {exc}"}
    else:
        rows = ft if isinstance(ft, list) else []
        hint["foreign_trade_row_count"] = len(rows)
        fb = 0.0
        fs = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            lab = str(row.get("label") or "").lower()
            val = _to_float(row.get("value"))
            if val is None:
                continue
            if "buy" in lab and "foreign" in lab:
                fb += abs(val)
            elif "sell" in lab and "foreign" in lab:
                fs += abs(val)
        if fb + fs > 0:
            nf = (fb - fs) / (fb + fs)
            hint["foreign_flow_net_ratio"] = round(nf, 4)
            if nf >= 0.12:
                adj += 2.0
            elif nf <= -0.12:
                adj -= 2.0

    return _clamp(adj, -5.0, 5.0), hint


def _fetch_symbol_daily_closes_from_db(symbol: str, bars: int) -> list[float]:
    """
    Last ``bars`` trading sessions of daily close from ``market_symbol_daily_volume``,
    chronological ASC (oldest first). Rows come from scan/warm ingest (vnstock history),
    not from a live quote call here.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return []
    safe_bars = max(1, min(int(bars), 500))
    try:
        from app.services.signal_engine_service import ensure_market_symbol_tables

        ensure_market_symbol_tables()
    except Exception:
        pass
    query = """
        WITH last_rows AS (
            SELECT close_price, trading_date
            FROM market_symbol_daily_volume
            WHERE UPPER(symbol) = %(symbol)s
              AND close_price IS NOT NULL
              AND close_price > 0
            ORDER BY trading_date DESC
            LIMIT %(bars)s
        )
        SELECT close_price FROM last_rows ORDER BY trading_date ASC
    """
    try:
        with connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query, {"symbol": sym, "bars": safe_bars})
                fetched = cur.fetchall()
    except Exception:
        return []
    out: list[float] = []
    for row in fetched:
        if not row:
            continue
        c = _to_float(row[0])
        if c is not None and c > 0:
            out.append(c)
    return out


def fetch_symbol_daily_closes(
    api_call_quote: Callable[..., Any],
    symbol: str,
    bars: int,
) -> list[float]:
    """
    Daily closes for macro/RS scoring from Postgres only (``market_symbol_daily_volume``).
    ``api_call_quote`` is retained for call-site compatibility; it is not invoked.
    Ensure history was persisted (short-term scan, warm jobs, or ingest) or RS may degrade.
    """
    _ = api_call_quote
    return _fetch_symbol_daily_closes_from_db(symbol, bars)


def score_macro_fundamental_proxy(
    symbol: str,
    api_call_quote: Callable[..., Any],
    api_call_company: Callable[..., Any] | None,
    api_call_financial: Callable[..., Any] | None,
    *,
    benchmark_symbol: str = "VNINDEX",
    lookback_bars: int = 30,
    benchmark_closes: Sequence[float] | None = None,
    api_call_trading: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """
    Proxy 0-100 score: dual-horizon relative strength vs benchmark, optional realized-vol
    context, vnstock financial statements (ratio + income + balance + cash flow), and
    company overview hints. Any failure yields partial neutral components (never raises).

    When ``benchmark_closes`` is provided, skips loading the benchmark series from DB.
    Otherwise benchmark closes are read from ``market_symbol_daily_volume`` (same as the symbol series).
    Persist history via scan/warm so 60-session RS can be evaluated when enough rows exist.

    When ``api_call_trading`` is provided, adds bounded demand/foreign-flow hints from
    vnstock trading snapshots (each call isolated in try/except).
    """
    sym = symbol.strip().upper()
    detail: dict[str, Any] = {"benchmark": benchmark_symbol, "macro_proxy_version": "vn_depth_v2"}
    rs_score = 50.0
    fund_score = 50.0

    series_bars = max(int(lookback_bars), 66)
    bench = list(benchmark_closes) if benchmark_closes is not None else fetch_symbol_daily_closes(
        api_call_quote, benchmark_symbol, series_bars
    )
    stk = fetch_symbol_daily_closes(api_call_quote, sym, series_bars)

    br21 = _pct_return_slice(bench, 21) if len(bench) >= 21 else _pct_return(bench)
    sr21 = _pct_return_slice(stk, 21) if len(stk) >= 21 else _pct_return(stk)
    br60 = _pct_return_slice(bench, 61) if len(bench) >= 61 else None
    sr60 = _pct_return_slice(stk, 61) if len(stk) >= 61 else None

    rs_short = 50.0
    rs_long = 50.0
    diff21: float | None = None
    diff60: float | None = None

    if br21 is not None and sr21 is not None:
        diff21 = float(sr21 - br21)
        rs_short = _diff_to_rs_score(diff21)
        detail["relative_strength_vs_benchmark_pct_21d"] = round(diff21, 3)
        # Backward-compatible alias (older clients read the 21d spread under this key).
        detail["relative_strength_vs_benchmark_pct"] = round(diff21, 3)
        detail["benchmark_return_pct_21d"] = round(float(br21), 3)
        detail["symbol_return_pct_21d"] = round(float(sr21), 3)
        detail["benchmark_return_pct"] = round(float(br21), 3)
        detail["symbol_return_pct"] = round(float(sr21), 3)
    if br60 is not None and sr60 is not None:
        diff60 = float(sr60 - br60)
        rs_long = _diff_to_rs_score(diff60)
        detail["relative_strength_vs_benchmark_pct_60d"] = round(diff60, 3)
        detail["benchmark_return_pct_60d"] = round(float(br60), 3)
        detail["symbol_return_pct_60d"] = round(float(sr60), 3)

    if diff21 is not None:
        if diff60 is not None:
            rs_score = _clamp(0.55 * rs_short + 0.45 * rs_long, 0.0, 100.0)
            detail["relative_strength_blend"] = "0.55*21d + 0.45*60d"
        else:
            rs_score = rs_short
            detail["relative_strength_blend"] = "21d_only_insufficient_history_for_60d"
    elif diff60 is not None:
        rs_score = rs_long
        detail["relative_strength_blend"] = "60d_only"
    else:
        detail["relative_strength_skipped"] = "insufficient_benchmark_or_symbol_history"

    if len(stk) >= 22 and len(bench) >= 22:
        ss = _std_daily_returns_pct(stk[-22:])
        bs = _std_daily_returns_pct(bench[-22:])
        if ss is not None and bs is not None and bs > 0:
            detail["realized_vol_pct_20d_symbol"] = round(ss, 4)
            detail["realized_vol_pct_20d_benchmark"] = round(bs, 4)
            if ss < bs * 0.88:
                rs_score = _clamp(rs_score + 2.0, 0.0, 100.0)
                detail["volatility_context"] = "lower_vol_than_benchmark"
            elif ss > bs * 1.35:
                rs_score = _clamp(rs_score - 2.0, 0.0, 100.0)
                detail["volatility_context"] = "higher_vol_than_benchmark"

    flow_adj, flow_detail = _trading_microstructure_adjustment(api_call_trading, sym)
    detail["market_flow"] = flow_detail
    if flow_adj != 0.0:
        detail["market_flow_rs_adjustment"] = round(flow_adj, 3)
    rs_score = _clamp(rs_score + flow_adj, 0.0, 100.0)

    ratio_hints: dict[str, Any] = {}
    stmt_hints: dict[str, Any] = {}
    if api_call_financial is not None:
        try:
            raw = api_call_financial(
                "ratio",
                source="VCI",
                symbol=sym,
                period="quarter",
                get_all=True,
                show_log=False,
            )
        except Exception as exc:
            ratio_hints["error"] = f"{type(exc).__name__}: {exc}"
            raw = None
        rows = raw if isinstance(raw, list) else []
        if rows:
            sample = rows[0] if isinstance(rows[0], dict) else {}
            if isinstance(sample, dict):
                roe = None
                pe = None
                for k, v in sample.items():
                    lk = str(k).lower()
                    if roe is None and "roe" in lk:
                        roe = _to_float(v)
                    if pe is None and lk in ("pe", "p/e", "pe_ratio", "pe_ttm", "p_e", "price_to_earnings"):
                        pe = _to_float(v)
                pts = 45.0
                if roe is not None:
                    if roe >= 18:
                        pts += 18
                    elif roe >= 12:
                        pts += 12
                    elif roe >= 8:
                        pts += 6
                    elif roe < 0:
                        pts -= 15
                    ratio_hints["roe_last"] = roe
                if pe is not None and pe > 0:
                    if pe <= 12:
                        pts += 8
                    elif pe >= 35:
                        pts -= 6
                    ratio_hints["pe_last"] = pe
                fund_score = _clamp(pts, 0.0, 100.0)
                ratio_hints["rows_available"] = len(rows)
        else:
            ratio_hints["note"] = "no_ratio_rows"

        for method, key in (
            ("income_statement", "income"),
            ("balance_sheet", "balance"),
            ("cash_flow", "cash_flow"),
        ):
            try:
                raw_s = api_call_financial(
                    method,
                    source="VCI",
                    symbol=sym,
                    period="quarter",
                    get_all=True,
                    show_log=False,
                )
            except Exception as exc:
                stmt_hints[key] = {"error": f"{type(exc).__name__}: {exc}"}
                continue
            srows = _financial_statement_rows(raw_s)
            if key == "income":
                adj, h = _income_growth_adjustment(srows)
                fund_score = _clamp(fund_score + adj, 0.0, 100.0)
                stmt_hints[key] = h
            elif key == "balance":
                adj, h = _balance_leverage_adjustment(srows)
                fund_score = _clamp(fund_score + adj, 0.0, 100.0)
                stmt_hints[key] = h
            else:
                adj, h = _cash_flow_adjustment(srows)
                fund_score = _clamp(fund_score + adj, 0.0, 100.0)
                stmt_hints[key] = h

    profile_hints: dict[str, Any] = {}
    if api_call_company is not None:
        try:
            profile = api_call_company(
                "overview",
                source="VCI",
                symbol=sym,
                show_log=False,
            )
        except Exception as exc:
            profile_hints["error"] = f"{type(exc).__name__}: {exc}"
            profile = None
        if isinstance(profile, dict):
            wanted = ("name", "industry", "exchange", "market_cap", "issue_share", "website")
            hits = sum(1 for f in wanted if profile.get(f) not in (None, "", 0))
            profile_hints["completeness_hits"] = hits
            fund_score = _clamp(fund_score + (hits / len(wanted)) * 12.0, 0.0, 100.0)
            extras = ("listed_date", "beta", "icb_name3", "history", "charter_capital")
            extra_hits = sum(1 for f in extras if profile.get(f) not in (None, "", 0))
            profile_hints["extended_completeness_hits"] = extra_hits
            if extra_hits:
                fund_score = _clamp(fund_score + min(6.0, extra_hits * 1.5), 0.0, 100.0)

    corp_structure: dict[str, Any] = {}
    if api_call_company is not None:
        for method, label in (("events", "events"), ("subsidiaries", "subsidiaries")):
            try:
                raw_c = api_call_company(method, source="VCI", symbol=sym, show_log=False)
            except Exception as exc:
                corp_structure[label] = {"error": f"{type(exc).__name__}: {exc}"}
            else:
                lst = raw_c if isinstance(raw_c, list) else []
                corp_structure[label] = {"row_count": len(lst)}
    if corp_structure:
        detail["corporate_structure"] = corp_structure

    composite_proxy = _clamp(0.55 * rs_score + 0.45 * fund_score, 0.0, 100.0)
    detail["relative_strength_score_0_100"] = round(rs_score, 2)
    detail["fundamental_data_score_0_100"] = round(fund_score, 2)
    detail["ratio"] = ratio_hints
    detail["profile"] = profile_hints
    detail["statements"] = stmt_hints

    return {
        "score_0_100": round(composite_proxy, 2),
        "detail": detail,
    }


def score_short_term_technical(
    *,
    spike: float,
    last_close: float,
    ema20_proxy: float,
    closes: Sequence[float],
    volumes: Sequence[float],
) -> dict[str, Any]:
    """0-100 technical module aligned with existing spike + EMA20 proxy gate."""
    spike_c = _clamp(spike, 0.0, 8.0)
    spike_component = _clamp((spike_c - 1.0) / 2.0 * 45.0, 0.0, 55.0)  # 1x -> 0, ~3x -> cap
    trend_component = 35.0 if last_close > ema20_proxy and ema20_proxy > 0 else 12.0
    vol_confirm = 0.0
    if len(volumes) >= 2 and len(closes) >= 2:
        v_prev = float(volumes[-2]) if volumes[-2] > 0 else 0.0
        if v_prev > 0 and volumes[-1] >= v_prev:
            vol_confirm = 10.0
    technical = _clamp(spike_component + trend_component + vol_confirm, 0.0, 100.0)
    return {
        "score_0_100": round(technical, 2),
        "detail": {
            "spike_component": round(spike_component, 2),
            "trend_component": round(trend_component, 2),
            "volume_confirmation": round(vol_confirm, 2),
            "volume_spike_ratio": round(spike, 4),
            "ema20_proxy": round(ema20_proxy, 4),
            "last_close": round(last_close, 4),
        },
    }


def score_long_term_technical(
    *,
    last_close: float,
    ma60: float,
    ma120: float,
    momentum_pct: float,
) -> dict[str, Any]:
    trend = 25.0 if last_close > ma60 > ma120 else 8.0
    mom = _clamp(20.0 + momentum_pct * 1.2, 0.0, 55.0)
    technical = _clamp(trend + mom, 0.0, 100.0)
    return {
        "score_0_100": round(technical, 2),
        "detail": {
            "stack_alignment_score": round(trend, 2),
            "momentum_score": round(mom, 2),
            "momentum_percent": round(momentum_pct, 4),
        },
    }


def score_technical_strategy_module(
    *,
    last_close: float,
    ema20: float,
    ema50: float,
    ema200: float,
) -> dict[str, Any]:
    aligned = ema20 > ema50 > ema200 and last_close >= ema20
    base = 55.0 if aligned else 22.0
    separation = 0.0
    if ema200 > 0:
        separation = _clamp((ema20 - ema200) / ema200 * 120.0, 0.0, 30.0)
    technical = _clamp(base + separation, 0.0, 100.0)
    return {
        "score_0_100": round(technical, 2),
        "detail": {
            "ema_stack_aligned": aligned,
            "ema_separation_bonus": round(separation, 2),
            "ema20": round(ema20, 4),
            "ema50": round(ema50, 4),
            "ema200": round(ema200, 4),
        },
    }


def aggregate_weighted_score(
    technical: float,
    news: float,
    macro: float,
    weights: dict[str, float] | None = None,
) -> tuple[float, dict[str, float]]:
    w = dict(DEFAULT_WEIGHTS)
    if weights:
        w.update(weights)
    wt = float(w.get("technical", 0.45))
    wn = float(w.get("news", 0.30))
    wm = float(w.get("macro_fundamental", 0.25))
    s = wt + wn + wm
    if s <= 0:
        wt, wn, wm = 0.45, 0.30, 0.25
        s = 1.0
    wt, wn, wm = wt / s, wn / s, wm / s
    composite = technical * wt + news * wn + macro * wm
    return round(_clamp(composite, 0.0, 100.0), 2), {"technical": wt, "news": wn, "macro_fundamental": wm}


def build_explainability(
    strategy_label: str,
    technical: dict[str, Any],
    news: dict[str, Any],
    macro: dict[str, Any],
    composite: float,
    weights_used: dict[str, float],
) -> dict[str, Any]:
    factors: list[str] = []
    t = technical.get("score_0_100")
    factors.append(f"Technical ({strategy_label}): score {t}, drivers {technical.get('detail', {})}")
    n = news.get("score_0_100")
    nb = news.get("neutral_because")
    rh = news.get("risk_term_hits_window")
    wh = news.get("window_hours", 72)
    nmode = news.get("scoring_mode") or "weighted_lexical_v2"
    wrm = news.get("weighted_risk_mass")
    wpm = news.get("weighted_positive_mass")
    cal = news.get("confidence_calibration_0_100")
    negx = (news.get("explainability") or {}).get("negation_suppressed_hits")
    if nmode == "legacy":
        news_label = f"News (RSS legacy window {wh}h)"
    elif nmode == "weighted_lexical_v1":
        news_label = f"News (RSS weighted lexical v1, {wh}h)"
    else:
        news_label = f"News (RSS weighted lexical v2 + phrases/negation, {wh}h)"
    si = (news.get("explainability") or {}).get("sentiment_interpretation") or {}
    si_note = ""
    if isinstance(si, dict) and si.get("sentiment_layer_applied"):
        fac = si.get("factors") or {}
        si_note = (
            f", sentiment_layer({fac.get('consensus_label')}, "
            f"con={fac.get('contradiction_factor')}, mix={fac.get('consensus_factor')}, "
            f"xart={fac.get('cross_article_factor')}/{fac.get('cross_article_label')}, "
            f"topic={fac.get('topic_rescale_factor')})"
        )
    factors.append(
        f"{news_label}: score {n}"
        + (f", note {nb}" if nb else f", symbol headlines {news.get('symbol_specific_items', 0)}")
        + (f", risk_term_hits {rh}" if rh is not None else "")
        + (f", weighted_risk_mass {wrm}" if wrm is not None and nb is None else "")
        + (f", weighted_positive_mass {wpm}" if wpm is not None and nb is None else "")
        + (f", calibration {cal}" if cal is not None and nb is None else "")
        + (f", negation_suppressed {negx}" if negx is not None and nb is None else "")
        + si_note
    )
    m = macro.get("score_0_100")
    dkeys = list((macro.get("detail") or {}).keys())
    mver = (macro.get("detail") or {}).get("macro_proxy_version")
    factors.append(
        f"Macro/fundamental proxy (vnstock-only depth, {mver or 'n/a'}): score {m}, detail keys {dkeys}"
    )
    w_str = ", ".join(f"{k}={round(v, 3)}" for k, v in sorted(weights_used.items()))
    summary = (
        f"Composite {composite} from weighted blend ({w_str}). "
        "News uses deterministic lexicon/phrases with negation windows, per-article RSS weights "
        "(freshness, feed tier, domestic/world/social), a coverage-based calibration score, "
        "and a bounded post-lexical interpretation layer (contradictions, cross-source consensus, "
        "cross-article lexical alignment, macro topic rescale); "
        "macro uses dual-horizon benchmark RS, optional realized-vol context, vnstock ratio + "
        "statements + overview, optional trading flow snapshot, and corporate-structure row counts."
    )
    return {
        "summary": summary,
        "factors": factors,
        "pipeline_version": SCORING_PIPELINE_VERSION,
        "news_confidence_calibration_0_100": news.get("confidence_calibration_0_100"),
        "macro_proxy_version": (macro.get("detail") or {}).get("macro_proxy_version"),
    }


def build_signal_quality_metadata(
    *,
    strategy_kind: str,
    technical: dict[str, Any],
    news: dict[str, Any],
    macro: dict[str, Any],
    weights: dict[str, float] | None,
    legacy_flat: dict[str, Any] | None = None,
) -> dict[str, Any]:
    comp, w_used = aggregate_weighted_score(
        float(technical["score_0_100"]),
        float(news["score_0_100"]),
        float(macro["score_0_100"]),
        weights,
    )
    explain = build_explainability(strategy_kind, technical, news, macro, comp, w_used)
    meta: dict[str, Any] = {
        "scoring_pipeline_version": SCORING_PIPELINE_VERSION,
        "signal_quality": {
            "weights_normalized": w_used,
            "components": {
                "technical": technical,
                "news": news,
                "macro_fundamental": macro,
            },
            "composite_score_0_100": comp,
            "explainability": explain,
        },
    }
    if legacy_flat:
        meta.update(legacy_flat)
    return meta


def confidence_from_composite_and_action(
    *,
    action: str,
    base_confidence_from_rules: float,
    composite: float,
    blend: float = 0.55,
) -> float:
    """Blend legacy rule-of-thumb confidence with composite quality (keeps stable UX)."""
    if action == "BUY":
        mixed = blend * composite + (1.0 - blend) * base_confidence_from_rules
        return _clamp(mixed, 0.0, 95.0)
    return _clamp(0.5 * composite + 0.5 * base_confidence_from_rules, 0.0, 85.0)


def fetch_news_snapshot_for_scoring(
    aggregate_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """
    Run RSS aggregation once per scan batch. ``aggregate_fn`` should be a zero-arg callable
    that returns {"items": [...], ...} so tests can inject fakes without network.
    """
    try:
        return aggregate_fn()
    except Exception as exc:
        return {"items": [], "feed_errors": {"aggregate": f"{type(exc).__name__}: {exc}"}}


@dataclass(frozen=True, slots=True)
class NewsFetchConfig:
    per_feed_limit: int = 4
    max_workers: int = 5
    timeout_seconds: float = 5.0


def default_news_aggregate_callable() -> Callable[[], dict[str, Any]]:
    """Late-bound factory to avoid circular imports (uses NewsAggregatorService)."""
    from app.core.news_feed_registry import NewsCategory
    from app.services.news_aggregator_service import NewsAggregatorService

    svc = NewsAggregatorService()
    categories: set[NewsCategory] = {"domestic", "world"}

    def _run() -> dict[str, Any]:
        cfg = NewsFetchConfig()
        return svc.aggregate(
            categories=categories,
            per_feed_limit=cfg.per_feed_limit,
            max_workers=cfg.max_workers,
            request_timeout_seconds=cfg.timeout_seconds,
        )

    return _run
