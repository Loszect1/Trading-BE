"""
In-memory stand-in for ``VNStockApiService`` driven by deterministic data.

Used to run real ``run_short_term_scan_batch`` / production cycle code paths
without network I/O.
"""

from __future__ import annotations

from typing import Any, Mapping

JSONValue = Any


class FixtureVNStockApi:
    def __init__(
        self,
        *,
        symbols_by_exchange: Mapping[str, list[dict[str, str]]],
        history_by_symbol: Mapping[str, list[dict[str, Any]]],
    ) -> None:
        self._symbols_by_exchange = {k.upper(): list(v) for k, v in symbols_by_exchange.items()}
        self._history_by_symbol = {k.upper(): list(v) for k, v in history_by_symbol.items()}

    # call_listing signature matches VNStockApiService enough for signal_engine
    def call_listing(
        self,
        method_name: str,
        source: str = "KBS",
        random_agent: bool = False,
        show_log: bool = False,
        method_kwargs: dict[str, Any] | None = None,
    ) -> JSONValue:
        _ = source, random_agent, show_log
        mk = method_kwargs or {}
        if method_name != "symbols_by_exchange":
            return []
        ex = str(mk.get("exchange", "")).upper()
        return list(self._symbols_by_exchange.get(ex, []))

    def call_quote(
        self,
        method_name: str,
        source: str = "VCI",
        symbol: str = "",
        random_agent: bool = False,
        show_log: bool = False,
        method_kwargs: dict[str, Any] | None = None,
    ) -> JSONValue:
        _ = source, random_agent, show_log, method_kwargs
        if method_name != "history":
            return []
        sym = str(symbol or "").strip().upper()
        return list(self._history_by_symbol.get(sym, []))
