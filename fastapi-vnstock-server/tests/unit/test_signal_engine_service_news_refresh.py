"""Unit tests for post-close symbol news refresh behavior (no DB, no external HTTP)."""

from __future__ import annotations

import app.services.signal_engine_service as sigsvc


def test_scan_and_persist_symbol_news_falls_back_when_vci_company_news_breaks(
    monkeypatch,
) -> None:
    calls: list[tuple[str, str]] = []
    persisted: list[dict] = []

    def fake_call_company(method_name: str, **kwargs):
        source = str(kwargs.get("source") or "")
        symbol = str(kwargs.get("symbol") or "")
        calls.append((source, symbol))
        assert method_name == "news"
        if source == "VCI":
            raise KeyError("data")
        if source == "KBS":
            return [
                {
                    "title": f"{symbol} wins new contract",
                    "summary": "Backlog expands after new order.",
                    "publishDate": "2026-04-16T09:00:00+07:00",
                    "publisher": "kbs_news",
                    "url": "https://example.test/article",
                }
            ]
        raise AssertionError(f"Unexpected source: {source}")

    def fake_persist_symbol_news_rows(*, symbol: str, exchange: str, news_items: list[dict], max_items: int) -> int:
        persisted.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "news_items": news_items,
                "max_items": max_items,
            }
        )
        return len(news_items)

    monkeypatch.setattr(
        sigsvc,
        "_get_scan_symbol_exchange_pairs_from_liquidity_cache",
        lambda _max_symbols, _exchange_scope: [("SGN", "HOSE")],
    )
    monkeypatch.setattr(sigsvc.vnstock_api_service, "call_company", fake_call_company)
    monkeypatch.setattr(sigsvc, "persist_symbol_news_rows", fake_persist_symbol_news_rows)

    out = sigsvc.scan_and_persist_symbol_news_from_liquidity_cache(
        exchange_scope="ALL",
        max_symbols=0,
        per_symbol_news_limit=20,
    )

    assert calls == [("VCI", "SGN"), ("KBS", "SGN")]
    assert out["symbols_from_redis"] == 1
    assert out["symbols_scanned"] == 1
    assert out["symbols_persisted"] == 1
    assert out["news_rows_persisted"] == 1
    assert out["errors"] == 0
    assert len(persisted) == 1
    assert persisted[0]["symbol"] == "SGN"
    assert persisted[0]["exchange"] == "HOSE"
    assert persisted[0]["news_items"][0]["source_id"] == "kbs_news"
