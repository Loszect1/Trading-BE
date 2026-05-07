"""Unit tests for modular signal quality scoring (no Postgres, no external HTTP)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.services.signal_scoring_pipeline import (
    SCORING_PIPELINE_VERSION,
    aggregate_weighted_score,
    build_explainability,
    build_signal_quality_metadata,
    confidence_from_composite_and_action,
    fetch_news_snapshot_for_scoring,
    fetch_symbol_daily_closes,
    score_long_term_technical,
    score_macro_fundamental_proxy,
    score_news_for_symbol,
    score_short_term_technical,
    score_technical_strategy_module,
)


def test_aggregate_weighted_score_normalizes_custom_weights():
    composite, w = aggregate_weighted_score(80.0, 40.0, 60.0, {"technical": 2.0, "news": 2.0, "macro_fundamental": 2.0})
    assert composite == pytest.approx(60.0)
    assert w["technical"] == pytest.approx(1 / 3)
    assert w["news"] == pytest.approx(1 / 3)
    assert w["macro_fundamental"] == pytest.approx(1 / 3)


def test_score_news_symbol_match_and_risk_terms():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    items = [
        {
            "title": "VCB reports contract expansion",
            "summary": "",
            "published_at": now.isoformat(),
            "source_id": "t",
            "category": "domestic",
        },
        {
            "title": "Market overview",
            "summary": "Sector faces fraud investigation",
            "published_at": now.isoformat(),
            "source_id": "t2",
            "category": "world",
        },
    ]
    out = score_news_for_symbol("VCB", items, now_utc=now)
    assert out["symbol_specific_items"] >= 1
    assert out["risk_term_hits_window"] >= 1
    assert 0 <= out["score_0_100"] <= 100


def test_score_news_empty_snapshot_neutral():
    out = score_news_for_symbol("ABC", [], now_utc=datetime.now(timezone.utc))
    assert out["score_0_100"] == 50.0
    assert out["neutral_because"] == "no_news_items_in_snapshot"
    assert out.get("scoring_mode") == "weighted_lexical_v2"


def test_score_news_legacy_mode_regression_numeric():
    """Legacy path preserves the pre-1.1.0 global-hit aggregation for A/B or audits."""
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    items = [
        {
            "title": "VCB reports contract expansion",
            "summary": "",
            "published_at": now.isoformat(),
            "source_id": "t",
            "category": "domestic",
        },
        {
            "title": "Market overview",
            "summary": "Sector faces fraud investigation",
            "published_at": now.isoformat(),
            "source_id": "t2",
            "category": "world",
        },
    ]
    out = score_news_for_symbol("VCB", items, now_utc=now, scoring_mode="legacy")
    # 50 + symbol_boost(6) + pos cap(4) - risk cap(6) = 54
    assert out["score_0_100"] == pytest.approx(54.0)
    assert out["scoring_mode"] == "legacy"


def test_score_news_weighted_risk_mass_higher_when_items_are_fresher():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    risk_title = "Regulator opens FRAUD probe into sector"
    old = (now - timedelta(hours=70)).isoformat()
    young = (now - timedelta(hours=2)).isoformat()
    mixed = [
        {
            "title": risk_title,
            "summary": "",
            "published_at": old,
            "source_id": "reuters_business",
            "category": "world",
        },
        {
            "title": risk_title,
            "summary": "",
            "published_at": young,
            "source_id": "reuters_business",
            "category": "world",
        },
    ]
    fresh = [
        {
            "title": risk_title,
            "summary": "",
            "published_at": young,
            "source_id": "reuters_business",
            "category": "world",
        },
        {
            "title": risk_title,
            "summary": "",
            "published_at": young,
            "source_id": "reuters_business",
            "category": "world",
        },
    ]
    out_mixed = score_news_for_symbol("ZZZ", mixed, now_utc=now)
    out_fresh = score_news_for_symbol("ZZZ", fresh, now_utc=now)
    assert out_fresh["weighted_risk_mass"] > out_mixed["weighted_risk_mass"]


def test_score_news_domestic_finance_feed_weights_above_social():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    domestic = [
        {
            "title": "ZZZ sees EXPANSION deal signed",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        }
    ]
    social = [
        {
            "title": "ZZZ EXPANSION chatter on forum",
            "summary": "",
            "published_at": t,
            "source_id": "reddit_stocks",
            "category": "social",
        }
    ]
    d = score_news_for_symbol("ZZZ", domestic, now_utc=now)
    s = score_news_for_symbol("ZZZ", social, now_utc=now)
    assert d["score_0_100"] > s["score_0_100"]


def test_short_term_technical_score_bounds():
    tech = score_short_term_technical(
        spike=2.2,
        last_close=12.0,
        ema20_proxy=10.0,
        closes=[10.0, 11.0, 12.0],
        volumes=[100.0, 100.0, 200.0],
    )
    assert 0 <= tech["score_0_100"] <= 100
    assert "rsi14" in tech["detail"]
    assert "stretch_penalty" in tech["detail"]


def test_short_term_technical_penalizes_overstretched_chase():
    base = score_short_term_technical(
        spike=2.5,
        last_close=105.0,
        ema20_proxy=100.0,
        closes=[100.0 + i * 0.1 for i in range(30)],
        volumes=[100.0] * 29 + [220.0],
        momentum_5d_pct=2.0,
        rsi14=62.0,
        distance_from_ema20_pct=5.0,
    )
    stretched = score_short_term_technical(
        spike=2.5,
        last_close=118.0,
        ema20_proxy=100.0,
        closes=[100.0 + i * 0.1 for i in range(30)],
        volumes=[100.0] * 29 + [220.0],
        momentum_5d_pct=8.0,
        rsi14=84.0,
        distance_from_ema20_pct=18.0,
    )
    assert stretched["detail"]["stretch_penalty"] > 0
    assert stretched["score_0_100"] < base["score_0_100"]


def test_long_term_technical():
    t = score_long_term_technical(last_close=110.0, ma60=100.0, ma120=90.0, momentum_pct=10.0)
    assert t["score_0_100"] > 50


def test_technical_strategy_module_alignment():
    t = score_technical_strategy_module(last_close=50.0, ema20=49.0, ema50=47.0, ema200=40.0)
    assert t["detail"]["ema_stack_aligned"] is True


def test_macro_proxy_uses_precomputed_benchmark(monkeypatch):
    calls: list[str] = []

    def quote(method: str, **kwargs):
        calls.append(kwargs.get("symbol", ""))
        sym = kwargs.get("symbol", "")
        if sym == "AAA":
            return [{"close": 10.0 + i * 0.1, "volume": 1.0} for i in range(25)]
        return []

    def fake_fetch(_quote, sym, bars):
        _ = bars
        if sym == "AAA":
            return [10.0 + i * 0.1 for i in range(25)]
        return []

    monkeypatch.setattr(
        "app.services.signal_scoring_pipeline.fetch_symbol_daily_closes",
        fake_fetch,
    )

    def company(method: str, **kwargs):
        return {"name": "A", "industry": "B", "exchange": "HOSE", "market_cap": 1, "issue_share": 1, "website": "x"}

    def financial(method: str, **kwargs):
        if method == "ratio":
            return [{"roe": 20.0, "pe": 10.0}]
        return []

    bench = [100.0 + i * 0.05 for i in range(25)]
    out = score_macro_fundamental_proxy(
        "AAA",
        quote,
        company,
        financial,
        benchmark_closes=bench,
    )
    assert 0 <= out["score_0_100"] <= 100
    assert "VNINDEX" not in calls
    assert out["detail"].get("relative_strength_vs_benchmark_pct") is not None
    assert "statements" in out["detail"]
    assert out["detail"].get("macro_proxy_version") == "vn_depth_v2"
    assert "market_flow" in out["detail"]
    assert "corporate_structure" in out["detail"]


def test_macro_proxy_dual_horizon_when_history_long_enough(monkeypatch):
    bench = [100.0 + i * 0.02 for i in range(70)]
    rows = [{"close": 10.0 + i * 0.14, "volume": 1.0} for i in range(70)]
    full_closes = [10.0 + i * 0.14 for i in range(70)]

    def quote(method: str, **kwargs):
        if kwargs.get("symbol") == "RUN":
            return rows
        return []

    def fake_fetch(_quote, sym, bars):
        if sym == "RUN":
            return full_closes[-bars:] if len(full_closes) >= bars else full_closes
        return []

    monkeypatch.setattr(
        "app.services.signal_scoring_pipeline.fetch_symbol_daily_closes",
        fake_fetch,
    )

    out = score_macro_fundamental_proxy(
        "RUN",
        quote,
        None,
        None,
        benchmark_closes=bench,
    )
    assert out["detail"].get("relative_strength_blend") == "0.55*21d + 0.45*60d"
    assert "relative_strength_vs_benchmark_pct_60d" in out["detail"]


def test_macro_proxy_income_statement_revenue_growth():
    def quote(method: str, **kwargs):
        return []

    def financial(method: str, **kwargs):
        if method == "ratio":
            return [{"roe": 12.0, "pe": 14.0}]
        if method == "income_statement":
            return [
                {"net_revenue": 120.0, "ticker": "X"},
                {"net_revenue": 100.0, "ticker": "X"},
            ]
        if method == "balance_sheet":
            return [{"debt_to_equity": 1.1}]
        if method == "cash_flow":
            return [
                {"operating_cash_flow": 50.0},
                {"operating_cash_flow": 48.0},
            ]
        return []

    out = score_macro_fundamental_proxy("X", quote, None, financial, benchmark_closes=[100.0, 101.0])
    st = out["detail"].get("statements") or {}
    assert "income" in st
    assert st["income"].get("revenue_qoq_or_seq_pct") == pytest.approx(20.0)
    assert 0 <= out["score_0_100"] <= 100


def test_build_signal_quality_metadata_preserves_legacy_keys():
    tech = {"score_0_100": 80.0, "detail": {}}
    news = {"score_0_100": 50.0, "matched_headlines": [], "risk_term_hits_window": 0, "positive_term_hits_window": 0, "symbol_specific_items": 0, "window_hours": 72}
    macro = {"score_0_100": 60.0, "detail": {}}
    meta = build_signal_quality_metadata(
        strategy_kind="SHORT_TERM",
        technical=tech,
        news=news,
        macro=macro,
        weights=None,
        legacy_flat={"volume_spike_ratio": 2.1},
    )
    assert meta["volume_spike_ratio"] == 2.1
    assert "signal_quality" in meta
    assert meta["signal_quality"]["composite_score_0_100"] > 0
    assert "explainability" in meta["signal_quality"]
    assert meta["signal_quality"]["explainability"].get("pipeline_version") == SCORING_PIPELINE_VERSION


def test_confidence_blend_buy():
    c = confidence_from_composite_and_action(
        action="BUY",
        base_confidence_from_rules=70.0,
        composite=50.0,
        blend=0.5,
    )
    assert c == pytest.approx(60.0)


def test_fetch_news_snapshot_handles_exception():
    def boom():
        raise RuntimeError("network")

    snap = fetch_news_snapshot_for_scoring(boom)
    assert snap["items"] == []
    assert "aggregate" in (snap.get("feed_errors") or {})


def test_aggregate_weighted_score_resets_when_weight_sum_non_positive():
    composite, w = aggregate_weighted_score(10.0, 20.0, 30.0, {"technical": 0.0, "news": 0.0, "macro_fundamental": 0.0})
    assert w["technical"] == pytest.approx(0.45)
    assert composite == pytest.approx(10.0 * 0.45 + 20.0 * 0.30 + 30.0 * 0.25)


def test_confidence_sell_path_uses_half_blend():
    c = confidence_from_composite_and_action(
        action="SELL",
        base_confidence_from_rules=80.0,
        composite=40.0,
        blend=0.55,
    )
    assert c == pytest.approx(60.0)


def test_fetch_symbol_daily_closes_reads_db_only(monkeypatch):
    def quote_should_not_run(*_a, **_k):
        raise AssertionError("vnstock quote must not be called; closes come from DB")

    monkeypatch.setattr(
        "app.services.signal_scoring_pipeline._fetch_symbol_daily_closes_from_db",
        lambda _sym, _bars: [],
    )
    assert fetch_symbol_daily_closes(quote_should_not_run, "AAA", 10) == []

    monkeypatch.setattr(
        "app.services.signal_scoring_pipeline._fetch_symbol_daily_closes_from_db",
        lambda _sym, _bars: [10.5],
    )
    assert fetch_symbol_daily_closes(quote_should_not_run, "AAA", 10) == [10.5]


def test_build_explainability_contains_weight_string():
    tech = {"score_0_100": 70.0, "detail": {"k": 1}}
    news = {
        "score_0_100": 55.0,
        "risk_term_hits_window": 1,
        "symbol_specific_items": 0,
        "window_hours": 72,
        "weighted_risk_mass": 1.2,
        "weighted_positive_mass": 0.4,
        "scoring_mode": "weighted_lexical_v1",
    }
    macro = {"score_0_100": 60.0, "detail": {"relative_strength_blend": "21d_only", "macro_proxy_version": "vn_depth_v2"}}
    exp = build_explainability("SHORT_TERM", tech, news, macro, 62.0, {"technical": 0.5, "news": 0.25, "macro_fundamental": 0.25})
    assert "Composite 62.0" in exp["summary"]
    assert any("News (RSS weighted lexical" in f for f in exp["factors"])
    assert exp.get("pipeline_version") == SCORING_PIPELINE_VERSION
    assert exp.get("macro_proxy_version") == "vn_depth_v2"


def test_score_news_negation_softens_risk_mass():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    negated = [
        {
            "title": "Official: KHONG PHA SAN for VCB this quarter after restructuring",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        }
    ]
    risk_only = [
        {
            "title": "Analyst warning: VCB faces PHA SAN risk on weak liquidity",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        }
    ]
    on = score_news_for_symbol("VCB", negated, now_utc=now)
    ro = score_news_for_symbol("VCB", risk_only, now_utc=now)
    assert on["weighted_risk_mass"] < ro["weighted_risk_mass"]
    assert (on.get("explainability") or {}).get("negation_suppressed_hits", 0) >= 1


def test_score_news_phrase_patterns_add_weighted_units():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    items = [
        {
            "title": "ZZZ regulator CANH BAO RUI RO to the trading network",
            "summary": "",
            "published_at": t,
            "source_id": "x",
            "category": "world",
        }
    ]
    out = score_news_for_symbol("ZZZ", items, now_utc=now)
    assert (out.get("explainability") or {}).get("phrase_risk_weighted_units", 0) >= 1.5


def test_macro_proxy_trading_snapshot_rs_adjustment():
    def quote(method: str, **kwargs):
        if kwargs.get("symbol") == "SYM":
            return [{"close": 10.0 + i * 0.12, "volume": 1.0} for i in range(25)]
        return []

    bench = [100.0 + i * 0.05 for i in range(25)]

    def trading(method: str, **kwargs):
        if method == "trading_stats":
            return {"buy_volume": 9000.0, "sell_volume": 500.0}
        if method == "foreign_trade":
            return [
                {"label": "Foreign buy volume (session)", "value": 800.0},
                {"label": "Foreign sell volume (session)", "value": 100.0},
            ]
        return None

    out = score_macro_fundamental_proxy(
        "SYM",
        quote,
        None,
        None,
        benchmark_closes=bench,
        api_call_trading=trading,
    )
    assert out["detail"].get("market_flow_rs_adjustment", 0) > 0


def test_sentiment_interpretation_layer_contradiction_dampens_extremes():
    """Single article with strong opposing phrase units triggers contradiction shrink."""
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    contradictory = [
        {
            "title": "VCB regulator CANH BAO RUI RO while board cites TANG TRUONG MANH outlook",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        }
    ]
    pos_only = [
        {
            "title": "VCB board cites TANG TRUONG MANH outlook on expansion",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        }
    ]
    c = score_news_for_symbol("VCB", contradictory, now_utc=now)
    p = score_news_for_symbol("VCB", pos_only, now_utc=now)
    si = (c.get("explainability") or {}).get("sentiment_interpretation") or {}
    assert si.get("sentiment_layer_applied") is True
    assert (si.get("factors") or {}).get("contradiction_factor", 1.0) < 1.0
    assert abs(c["score_0_100"] - 50.0) < abs(p["score_0_100"] - 50.0)


def test_sentiment_interpretation_layer_mixed_cross_source_penalty():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    mixed = [
        {
            "title": "VCB signs major EXPANSION deal with partners",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        },
        {
            "title": "VCB faces FRAUD probe after scandal report",
            "summary": "",
            "published_at": t,
            "source_id": "reuters_business",
            "category": "world",
        },
    ]
    out = score_news_for_symbol("VCB", mixed, now_utc=now)
    si = (out.get("explainability") or {}).get("sentiment_interpretation") or {}
    assert si.get("factors", {}).get("consensus_label") == "mixed_sources"
    assert si.get("factors", {}).get("consensus_factor") == pytest.approx(0.89)


def test_sentiment_interpretation_layer_aligned_sources_mild_stretch():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    aligned = [
        {
            "title": "VCB EXPANSION OUTPERFORM guidance",
            "summary": "",
            "published_at": t,
            "source_id": "vietstock_thi_truong_ck",
            "category": "domestic",
        },
        {
            "title": "VCB expansion OUTPERFORM on record",
            "summary": "",
            "published_at": t,
            "source_id": "vnexpress_kinh_doanh",
            "category": "domestic",
        },
        {
            "title": "VCB OUTPERFORM peers on expansion",
            "summary": "",
            "published_at": t,
            "source_id": "thanhnien_kinh_te",
            "category": "domestic",
        },
    ]
    out = score_news_for_symbol("VCB", aligned, now_utc=now)
    si = (out.get("explainability") or {}).get("sentiment_interpretation") or {}
    assert si.get("factors", {}).get("consensus_label") == "aligned_sources"
    assert si.get("factors", {}).get("consensus_factor") == pytest.approx(1.035)


def test_sentiment_interpretation_layer_macro_topic_rescale_on_bullish():
    """Macro-tagged articles with meaningful macro-window risk dampen an otherwise bullish residual."""
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    macro_bull = [
        {
            "title": "VCB FED RATE path EXPANSION OUTPERFORM tailwinds",
            "summary": "CRISIS DEFAULT sector watch",
            "published_at": t,
            "source_id": "reuters_business",
            "category": "world",
        },
        {
            "title": "VCB LAI SUAT CPI INFLATION EXPANSION OUTPERFORM",
            "summary": "CRISIS tone",
            "published_at": t,
            "source_id": "bbc_business",
            "category": "world",
        },
    ]
    plain_bull = [
        {
            "title": "VCB EXPANSION OUTPERFORM tailwinds",
            "summary": "",
            "published_at": t,
            "source_id": "reuters_business",
            "category": "world",
        },
        {
            "title": "VCB expansion OUTPERFORM",
            "summary": "",
            "published_at": t,
            "source_id": "bbc_business",
            "category": "world",
        },
    ]
    m = score_news_for_symbol("VCB", macro_bull, now_utc=now)
    pl = score_news_for_symbol("VCB", plain_bull, now_utc=now)
    fac_m = (m.get("explainability") or {}).get("sentiment_interpretation", {}).get("factors", {})
    assert fac_m.get("macro_topic_weight_share", 0) >= 0.5
    assert m["score_0_100"] <= pl["score_0_100"]
    t_fac = fac_m.get("topic_rescale_factor")
    assert t_fac is not None and t_fac < 1.0


def test_sentiment_interpretation_layer_skipped_for_legacy_mode():
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    items = [
        {
            "title": "VCB EXPANSION and FRAUD probe mixed",
            "summary": "",
            "published_at": now.isoformat(),
            "source_id": "a",
            "category": "domestic",
        },
    ]
    out = score_news_for_symbol("VCB", items, now_utc=now, scoring_mode="legacy")
    assert out.get("explainability") is None
    assert "sentiment_interpretation" not in out


def test_sentiment_interpretation_cross_article_mixed_tilts_dampens():
    """Opposite lexical tilts across articles (same RSS source) pull score toward neutral."""
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    same_feed_mixed = [
        {
            "title": "VCB EXPANSION OUTPERFORM RECORD on guidance",
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
        {
            "title": "VCB FRAUD scandal CRISIS probe deepens",
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
    ]
    out = score_news_for_symbol("VCB", same_feed_mixed, now_utc=now)
    fac = (out.get("explainability") or {}).get("sentiment_interpretation", {}).get("factors") or {}
    assert fac.get("cross_article_label") == "mixed_article_lexical_tilts"
    assert fac.get("cross_article_factor") == pytest.approx(0.955)
    assert fac.get("consensus_label") == "neutral_thin_or_single_sided"


def test_sentiment_interpretation_cross_article_aligned_bullish_stretch():
    """Three bullish-tilt articles with bullish residual get a mild cross-article stretch."""
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    tpl = "VCB EXPANSION OUTPERFORM RECORD PARTNERSHIP tailwinds batch {}"
    three_bull = [
        {
            "title": tpl.format(1),
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
        {
            "title": tpl.format(2),
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
        {
            "title": tpl.format(3),
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
    ]
    out = score_news_for_symbol("VCB", three_bull, now_utc=now)
    fac = (out.get("explainability") or {}).get("sentiment_interpretation", {}).get("factors") or {}
    assert fac.get("cross_article_label") == "aligned_bullish_articles"
    assert fac.get("cross_article_factor") == pytest.approx(1.022)
    assert out["score_0_100"] > 50.0


def test_sentiment_interpretation_cross_article_aligned_bullish_skipped_when_residual_flat():
    """Agreement stretch does not fire when blended score is not meaningfully bullish."""
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    t = now.isoformat()
    three_mild = [
        {
            "title": "VCB market notes batch 1",
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
        {
            "title": "VCB market notes batch 2",
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
        {
            "title": "VCB market notes batch 3",
            "summary": "",
            "published_at": t,
            "source_id": "same_feed",
            "category": "domestic",
        },
    ]
    out = score_news_for_symbol("VCB", three_mild, now_utc=now)
    fac = (out.get("explainability") or {}).get("sentiment_interpretation", {}).get("factors") or {}
    assert fac.get("cross_article_label") in (
        "neutral_lexical_spread",
        "aligned_bullish_articles_skipped_residual_not_bullish",
    )
    assert fac.get("cross_article_factor") == pytest.approx(1.0)
