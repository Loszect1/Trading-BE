from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from tenacity import RetryError

from vnstock import register_user
from vnstock.api.company import Company
from vnstock.api.financial import Finance
from vnstock.api.listing import Listing
from vnstock.api.quote import Quote
from vnstock.api.trading import Trading

from app.core.config import settings

JSONValue = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

# vnstock Trading providers (KBS/VCI) only implement price_board; the high-level Trading API
# declares trading_stats / side_stats / foreign_trade / prop_trade but they raise NotImplementedError
# on the explorer classes. Map these to VCI Company GraphQL + price board instead.
_TRADING_METHODS_VIA_VCI_SNAPSHOT = frozenset(
    {"trading_stats", "side_stats", "foreign_trade", "prop_trade"},
)


class VNStockApiService:
    def __init__(self) -> None:
        self._api_key_registered = False

    def _ensure_api_key(self) -> None:
        if self._api_key_registered:
            return
        if settings.vnstock_api_key:
            register_user(api_key=settings.vnstock_api_key)
            self._api_key_registered = True

    @staticmethod
    def _serialize_result(result: Any) -> JSONValue:
        if result is None:
            return None
        if isinstance(result, (str, int, float, bool)):
            return result
        if isinstance(result, dict):
            return {str(k): VNStockApiService._serialize_result(v) for k, v in result.items()}
        if isinstance(result, (list, tuple, set)):
            return [VNStockApiService._serialize_result(v) for v in result]

        # pandas DataFrame
        if hasattr(result, "to_dict"):
            try:
                return result.to_dict(orient="records")
            except TypeError:
                return result.to_dict()

        # fallback for unknown objects
        return str(result)

    def call_quote(
        self,
        method_name: str,
        source: str = "VCI",
        symbol: str = "",
        random_agent: bool = False,
        show_log: bool = False,
        method_kwargs: Dict[str, Any] | None = None,
    ) -> JSONValue:
        self._ensure_api_key()
        call_kwargs = self._normalize_quote_kwargs(method_name, method_kwargs or {})
        errors: List[str] = []

        for candidate_source in self._quote_source_order(source):
            try:
                quote = Quote(
                    source=candidate_source,
                    symbol=symbol,
                    random_agent=random_agent,
                    show_log=show_log,
                )
                method = getattr(quote, method_name)
                result = method(**call_kwargs)
                return self._serialize_result(result)
            except Exception as exc:
                errors.append(f"{candidate_source}: {self._stringify_exception(exc)}")

        raise RuntimeError(
            "All quote sources failed. "
            + " | ".join(errors)
        )

    @staticmethod
    def _normalize_quote_kwargs(method_name: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(kwargs)
        if method_name == "history":
            # Accept common frontend aliases and map to vnstock names.
            alias_map = {
                "start_date": "start",
                "startDate": "start",
                "from": "start",
                "end_date": "end",
                "endDate": "end",
                "to": "end",
                "countBack": "count_back",
                "count-back": "count_back",
            }
            for old_key, new_key in alias_map.items():
                if old_key in normalized and new_key not in normalized:
                    normalized[new_key] = normalized.pop(old_key)

        if method_name == "history" and "interval" in normalized and isinstance(normalized["interval"], str):
            interval = normalized["interval"].strip().upper()
            interval_alias = {"D": "1D", "W": "1W", "M": "1M", "H": "1H"}
            normalized["interval"] = interval_alias.get(interval, interval)

        if method_name == "history":
            has_start = bool(normalized.get("start"))
            has_alt_window = any(
                normalized.get(k) is not None and str(normalized.get(k)).strip() != ""
                for k in ("length", "period", "count_back")
            )
            if not has_start and not has_alt_window:
                raise ValueError(
                    "Missing history window. Provide 'start' (or aliases: startDate/start_date/from) "
                    "or provide one of 'length'/'period'/'count_back'."
                )
        return normalized

    @staticmethod
    def _quote_source_order(source: str) -> List[str]:
        normalized = (source or "").strip().upper()
        if normalized == "KBS":
            return ["KBS", "VCI"]
        return ["VCI", "KBS"]

    @staticmethod
    def _stringify_exception(exc: Exception) -> str:
        if isinstance(exc, RetryError) and exc.last_attempt is not None:
            last_exc = exc.last_attempt.exception()
            if last_exc is not None:
                return f"{type(last_exc).__name__}: {str(last_exc)}"
        return f"{type(exc).__name__}: {str(exc)}"

    def call_company(
        self,
        method_name: str,
        source: str = "VCI",
        symbol: str = "",
        random_agent: bool = False,
        show_log: bool = False,
        method_kwargs: Dict[str, Any] | None = None,
    ) -> JSONValue:
        self._ensure_api_key()
        company = Company(source=source, symbol=symbol, random_agent=random_agent, show_log=show_log)
        method = getattr(company, method_name)
        result = method(**(method_kwargs or {}))
        return self._serialize_result(result)

    def call_financial(
        self,
        method_name: str,
        source: str = "VCI",
        symbol: str = "",
        period: str = "quarter",
        get_all: bool = True,
        show_log: bool = False,
        method_kwargs: Dict[str, Any] | None = None,
    ) -> JSONValue:
        self._ensure_api_key()
        call_kwargs = method_kwargs or {}
        errors: List[str] = []

        for candidate_source in self._financial_source_order(source):
            try:
                financial = Finance(
                    source=candidate_source,
                    symbol=symbol,
                    period=period,
                    get_all=get_all,
                    show_log=show_log,
                )
                method = getattr(financial, method_name)
                result = method(**call_kwargs)
                return self._serialize_result(result)
            except Exception as exc:
                errors.append(f"{candidate_source}: {str(exc)}")

        raise RuntimeError(
            "All financial sources failed. "
            + " | ".join(errors)
        )

    @staticmethod
    def _financial_source_order(source: str) -> List[str]:
        normalized = (source or "").strip().upper()
        if normalized == "KBS":
            return ["KBS", "VCI"]
        return ["VCI", "KBS"]

    def call_listing(
        self,
        method_name: str,
        source: str = "KBS",
        random_agent: bool = False,
        show_log: bool = False,
        method_kwargs: Dict[str, Any] | None = None,
    ) -> JSONValue:
        self._ensure_api_key()
        listing = Listing(source=source, random_agent=random_agent, show_log=show_log)
        method = getattr(listing, method_name)
        result = method(**(method_kwargs or {}))
        return self._serialize_result(result)

    @staticmethod
    def _trading_float(value: Any) -> float:
        if value is None:
            return 0.0
        try:
            if hasattr(value, "item"):
                value = value.item()
            out = float(value)
        except (TypeError, ValueError):
            return 0.0
        if math.isnan(out) or math.isinf(out):
            return 0.0
        return out

    def _vci_company_trading_row(
        self,
        symbol: str,
        random_agent: bool,
        show_log: bool,
    ) -> Optional[pd.Series]:
        try:
            company = Company(
                source="vci",
                symbol=symbol,
                random_agent=random_agent,
                show_log=show_log,
            )
            df = company.trading_stats()
            if df is None or getattr(df, "empty", True):
                return None
            return df.iloc[0]
        except Exception:
            return None

    def _vci_price_board_flat(
        self,
        symbol: str,
        random_agent: bool,
        show_log: bool,
    ) -> Optional[pd.DataFrame]:
        try:
            trading = Trading(
                source="vci",
                symbol=symbol,
                random_agent=random_agent,
                show_log=show_log,
            )
            return trading.price_board(
                symbols_list=[symbol],
                show_log=False,
                flatten_columns=True,
                separator="_",
            )
        except Exception:
            return None

    def _build_trading_stats_payload(
        self,
        symbol: str,
        random_agent: bool,
        show_log: bool,
    ) -> Dict[str, Any]:
        row = self._vci_company_trading_row(symbol, random_agent, show_log)
        if row is not None:
            total_volume = self._trading_float(row.get("total_volume"))
            price = self._trading_float(
                row.get("match_price") if row.get("match_price") is not None else row.get("close_price")
            )
            return {
                "symbol": symbol,
                "total_volume": total_volume,
                "total_value": total_volume * price,
                "buy_volume": self._trading_float(row.get("buy_volume")),
                "sell_volume": self._trading_float(row.get("sell_volume")),
            }

        board = self._vci_price_board_flat(symbol, random_agent, show_log)
        if board is not None and not board.empty:
            r = board.iloc[0]
            acc_vol = self._trading_float(r.get("match_accumulated_volume"))
            acc_val = self._trading_float(r.get("match_accumulated_value"))
            if acc_vol or acc_val:
                return {
                    "symbol": symbol,
                    "total_volume": acc_vol,
                    "total_value": acc_val,
                    "buy_volume": 0.0,
                    "sell_volume": 0.0,
                }

        return {
            "symbol": symbol,
            "total_volume": 0.0,
            "total_value": 0.0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
        }

    def _build_foreign_trade_rows(
        self,
        symbol: str,
        random_agent: bool,
        show_log: bool,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        company_specs = [
            ("foreign_volume", "Foreign total volume"),
            ("foreign_room", "Foreign room"),
            ("avg_match_volume_2w", "Avg match volume (2 weeks)"),
            ("foreign_holding_room", "Foreign holding room"),
            ("current_holding_ratio", "Current holding ratio"),
            ("max_holding_ratio", "Max holding ratio"),
        ]
        row = self._vci_company_trading_row(symbol, random_agent, show_log)
        if row is not None:
            for key, label in company_specs:
                if key not in row.index:
                    continue
                val = row[key]
                if pd.isna(val):
                    continue
                rows.append({"label": label, "value": self._trading_float(val)})

        board = self._vci_price_board_flat(symbol, random_agent, show_log)
        if board is not None and not board.empty:
            r = board.iloc[0]
            board_specs = [
                ("match_foreign_buy_volume", "Foreign buy volume (session)"),
                ("match_foreign_sell_volume", "Foreign sell volume (session)"),
                ("match_foreign_buy_value", "Foreign buy value (session)"),
                ("match_foreign_sell_value", "Foreign sell value (session)"),
            ]
            for key, label in board_specs:
                if key not in r.index:
                    continue
                v = self._trading_float(r[key])
                if v:
                    rows.append({"label": label, "value": v})

        return rows

    def _build_side_stats_rows(
        self,
        symbol: str,
        random_agent: bool,
        show_log: bool,
    ) -> List[Dict[str, Any]]:
        board = self._vci_price_board_flat(symbol, random_agent, show_log)
        if board is None or board.empty:
            return []

        r = board.iloc[0]
        bid_sum = 0.0
        ask_sum = 0.0
        for col in board.columns:
            name = str(col).lower()
            if "volume" in name and "bid_count" not in name and "ask_count" not in name:
                if "bid" in name:
                    bid_sum += self._trading_float(r[col])
                elif "ask" in name:
                    ask_sum += self._trading_float(r[col])

        out: List[Dict[str, Any]] = []
        if bid_sum:
            out.append({"label": "Bid depth volume", "value": bid_sum})
        if ask_sum:
            out.append({"label": "Ask depth volume", "value": ask_sum})
        if out:
            return out

        buy_orders = self._trading_float(r.get("match_total_buy_orders"))
        sell_orders = self._trading_float(r.get("match_total_sell_orders"))
        if buy_orders or sell_orders:
            out.append({"label": "Total buy orders", "value": buy_orders})
            out.append({"label": "Total sell orders", "value": sell_orders})
        return out

    def _call_trading_via_vci_snapshot(
        self,
        method_name: str,
        symbol: str,
        random_agent: bool,
        show_log: bool,
    ) -> JSONValue:
        sym = (symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol is required")

        if method_name == "prop_trade":
            return []

        if method_name == "trading_stats":
            return self._build_trading_stats_payload(sym, random_agent, show_log)

        if method_name == "foreign_trade":
            return self._build_foreign_trade_rows(sym, random_agent, show_log)

        if method_name == "side_stats":
            return self._build_side_stats_rows(sym, random_agent, show_log)

        raise ValueError(f"Unsupported trading snapshot method: {method_name}")

    def call_trading(
        self,
        method_name: str,
        source: str = "KBS",
        symbol: str = "",
        random_agent: bool = False,
        show_log: bool = False,
        method_kwargs: Dict[str, Any] | None = None,
    ) -> JSONValue:
        self._ensure_api_key()
        if method_name in _TRADING_METHODS_VIA_VCI_SNAPSHOT:
            result = self._call_trading_via_vci_snapshot(
                method_name,
                symbol,
                random_agent,
                show_log,
            )
            return self._serialize_result(result)

        trading = Trading(source=source, symbol=symbol, random_agent=random_agent, show_log=show_log)
        method = getattr(trading, method_name)
        result = method(**(method_kwargs or {}))
        return self._serialize_result(result)
