from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.long_term_strategy import router as strategy_router
from app.routers.macro import router as macro_router
from app.routers.market import router as market_router
from app.routers.stocks import router as stocks_router


def _client(*routers) -> TestClient:
    app = FastAPI()
    for router in routers:
        app.include_router(router)
    return TestClient(app)


def test_macro_router_response_shapes(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.routers.macro.upsert_macro_observation",
        lambda body: {"id": "1", **body},
    )
    monkeypatch.setattr(
        "app.routers.macro.list_macro_observations",
        lambda **_kwargs: {"items": [], "count": 0},
    )
    client = _client(macro_router)

    r = client.post(
        "/macro/observations",
        json={
            "metric_key": "gdp_growth_yoy",
            "period": "2026Q1",
            "value": 6.8,
            "source_name": "NSO/GSO",
        },
    )
    assert r.status_code == 200
    assert r.json()["success"] is True

    r2 = client.get("/macro/observations")
    assert r2.status_code == 200
    assert r2.json()["data"]["count"] == 0


def test_macro_gpt_analysis_router_success_and_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.routers.macro.analyze_macro_news_economics_with_gpt",
        lambda **_kwargs: {"success": True, "analysis_source": "gpt_in_be_container"},
    )
    client = _client(macro_router)

    r = client.post("/macro/gpt-analysis", json={"language": "en"})
    assert r.status_code == 200
    assert r.json()["analysis_source"] == "gpt_in_be_container"

    def disabled(**_kwargs):
        raise RuntimeError("gpt_disabled")

    monkeypatch.setattr("app.routers.macro.analyze_macro_news_economics_with_gpt", disabled)
    r2 = client.post("/macro/gpt-analysis", json={})
    assert r2.status_code == 503
    assert "USE_GPT=true" in r2.json()["detail"]


def test_market_macro_regime_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.routers.market.get_macro_regime",
        lambda: {"regime": "Recovery", "regime_score": 50, "components": {}},
    )
    client = _client(market_router)
    r = client.get("/market/macro-regime")
    assert r.status_code == 200
    assert r.json()["regime"] == "Recovery"


def test_long_term_strategy_and_stock_routers(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.routers.long_term_strategy.run_long_term_scan",
        lambda **_kwargs: {"success": True, "run_id": "run-1", "rankings": []},
    )
    monkeypatch.setattr(
        "app.routers.long_term_strategy.list_long_term_rankings",
        lambda **_kwargs: {"items": [{"symbol": "AAA"}], "count": 1},
    )
    monkeypatch.setattr(
        "app.routers.long_term_strategy.get_long_term_scan",
        lambda run_id: {"run": {"id": run_id}, "rankings": []},
    )
    monkeypatch.setattr(
        "app.routers.stocks.analyze_symbol_for_research",
        lambda symbol, **_kwargs: {"symbol": symbol.upper(), "final_score": 70, "rating": "Watchlist candidate"},
    )
    monkeypatch.setattr(
        "app.routers.stocks.get_latest_symbol_score",
        lambda symbol: {"symbol": symbol.upper(), "final_score": 70},
    )
    client = _client(strategy_router, stocks_router)

    assert client.post("/strategy/long-term/scans", json={"universe_size": 10}).json()["success"] is True
    assert client.get("/strategy/long-term/rankings").json()["data"]["count"] == 1
    assert client.get("/strategy/long-term/scans/run-1").json()["data"]["run"]["id"] == "run-1"
    assert client.post("/stocks/aaa/long-term-analysis").json()["symbol"] == "AAA"
    assert client.get("/stocks/aaa/long-term-score").json()["final_score"] == 70
