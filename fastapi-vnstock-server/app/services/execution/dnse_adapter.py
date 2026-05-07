from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from app.services.execution.broker_status import (
    dnse_row_to_dict,
    extract_dnse_order_id,
    extract_dnse_status_token,
    map_dnse_status_to_internal,
)
from app.services.dnse_trading_token_store import get_runtime_dnse_trading_token
from app.services.execution.dnse_token import normalize_dnse_error, validate_dnse_tokens_for_live
from app.services.execution.types import ExecutionOutcome, OrderExecutionContext

if TYPE_CHECKING:
    from app.core.config import AppSettings

T = TypeVar("T")
logger = logging.getLogger(__name__)


class DnseLiveExecutionAdapter:
    """Live DNSE path using vnstock Trade (timeouts + bounded retries)."""

    name = "dnse_live"

    def __init__(self, *, settings: "AppSettings") -> None:
        self._settings = settings

    def _sub_account(self) -> str:
        sub = (getattr(self._settings, "dnse_sub_account", None) or "").strip()
        if sub:
            return sub
        return (self._settings.dnse_default_sub_account or "").strip()

    def _order_type(self, ctx: OrderExecutionContext) -> str:
        meta = ctx.order_metadata or {}
        ot = meta.get("dnse_order_type") or meta.get("order_type") or self._settings.dnse_order_type
        return str(ot or "LO").strip().upper() or "LO"

    def _asset_type(self, ctx: OrderExecutionContext) -> str:
        meta = ctx.order_metadata or {}
        at = meta.get("dnse_asset_type") or meta.get("asset_type") or self._settings.dnse_asset_type
        s = str(at or "stock").strip().lower()
        return s if s in {"stock", "derivative"} else "stock"

    def _loan_package_id(self, ctx: OrderExecutionContext) -> int | None:
        meta = ctx.order_metadata or {}
        if "dnse_loan_package_id" in meta and meta["dnse_loan_package_id"] is not None:
            try:
                return int(meta["dnse_loan_package_id"])
            except (TypeError, ValueError):
                return None
        lid = getattr(self._settings, "dnse_loan_package_id", None)
        return int(lid) if lid is not None else None

    def _token_gate(self) -> ExecutionOutcome | None:
        ok, reason = validate_dnse_tokens_for_live(
            self._settings.dnse_access_token or "",
            get_runtime_dnse_trading_token(self._settings),
        )
        if ok:
            return None
        return ExecutionOutcome(
            internal_status="REJECTED",
            reason=reason or "dnse_tokens_invalid",
            broker_order_id=None,
            broker_raw={},
            log_messages=["DNSE live adapter: token validation failed"],
        )

    def _call_with_timeout(self, fn: Callable[[], T], timeout_sec: float) -> T:
        timeout_sec = max(0.5, float(timeout_sec))
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(fn)
            try:
                return fut.result(timeout=timeout_sec)
            except FuturesTimeoutError as e:
                raise TimeoutError(f"DNSE call exceeded {timeout_sec}s") from e

    def _build_trade(self) -> Any:
        from vnstock.connector.dnse import Trade

        trade = Trade()
        trade.token = (self._settings.dnse_access_token or "").strip()
        trade.trading_token = get_runtime_dnse_trading_token(self._settings)
        return trade

    def _first_row_dict(self, df: Any) -> dict[str, Any]:
        if df is None:
            return {}
        if hasattr(df, "empty") and df.empty:
            return {}
        if hasattr(df, "iloc"):
            try:
                return dnse_row_to_dict(df.iloc[0])
            except Exception:
                return {}
        return dnse_row_to_dict(df)

    def _dataframe_rows_dicts(self, df: Any) -> list[dict[str, Any]]:
        if df is None:
            return []
        if hasattr(df, "empty") and df.empty:
            return []
        if hasattr(df, "to_dict"):
            try:
                rec = df.to_dict(orient="records")
            except (TypeError, ValueError):
                rec = None
            if isinstance(rec, list) and rec:
                return [{str(k): v for k, v in row.items()} for row in rec]
        if hasattr(df, "iloc"):
            out: list[dict[str, Any]] = []
            n = len(df)  # type: ignore[arg-type]
            for i in range(int(n)):
                try:
                    out.append(dnse_row_to_dict(df.iloc[i]))
                except Exception:
                    break
            return out
        one = dnse_row_to_dict(df)
        return [one] if one else []

    def readonly_list_sub_accounts(self) -> list[dict[str, Any]]:
        """
        Live read-only call: list DNSE sub-accounts using the same Trade token wiring
        and timeouts as ``execute`` / ``cancel_at_broker``. Does not place or cancel orders.
        """
        gate = self._token_gate()
        if gate:
            raise RuntimeError(gate.reason or "dnse_tokens_invalid")
        trade = self._build_trade()
        timeout = float(self._settings.dnse_execution_timeout_seconds)

        def _do() -> Any:
            return trade.sub_accounts()

        df = self._call_with_timeout(_do, timeout)
        return self._dataframe_rows_dicts(df)

    def readonly_list_holdings(self) -> list[dict[str, Any]]:
        """Live read-only call: fetch DNSE stock portfolio holdings for the configured sub-account."""
        gate = self._token_gate()
        if gate:
            raise RuntimeError(gate.reason or "dnse_tokens_invalid")
        sub = self._sub_account()
        if not sub:
            raise RuntimeError("dnse_sub_account_missing")
        token = (self._settings.dnse_access_token or "").strip()
        timeout = float(self._settings.dnse_execution_timeout_seconds)
        url = f"https://api.dnse.com.vn/order-service/portfolio?accountNo={sub}"
        headers = {"Authorization": f"Bearer {token}"}

        def _do() -> list[dict[str, Any]]:
            import requests as _req
            resp = _req.get(url, headers=headers, timeout=max(5.0, timeout))
            if resp.status_code == 200:
                body = resp.json()
                rows = body if isinstance(body, list) else body.get("data", [])
                if not isinstance(rows, list):
                    rows = []
                return [{str(k): v for k, v in row.items()} for row in rows if isinstance(row, dict)]
            raise RuntimeError(f"dnse_portfolio_http_{resp.status_code}")

        return self._call_with_timeout(_do, timeout)

    def _place_order(self, trade: Any, ctx: OrderExecutionContext) -> dict[str, Any]:
        side = "buy" if str(ctx.side).upper() == "BUY" else "sell"
        timeout = float(self._settings.dnse_execution_timeout_seconds)

        def _do() -> Any:
            return trade.place_order(
                sub_account=self._sub_account(),
                symbol=str(ctx.symbol).upper(),
                side=side,
                quantity=int(ctx.quantity),
                price=float(ctx.price),
                order_type=self._order_type(ctx),
                loan_package_id=self._loan_package_id(ctx),
                asset_type=self._asset_type(ctx),
            )

        df = self._call_with_timeout(_do, timeout)
        return self._first_row_dict(df)

    def _order_detail(self, trade: Any, broker_order_id: str, ctx: OrderExecutionContext) -> dict[str, Any]:
        timeout = float(self._settings.dnse_execution_timeout_seconds)

        def _do() -> Any:
            return trade.order_detail(
                order_id=str(broker_order_id),
                sub_account=self._sub_account(),
                asset_type=self._asset_type(ctx),
            )

        df = self._call_with_timeout(_do, timeout)
        return self._first_row_dict(df)

    def _cancel_order(self, trade: Any, broker_order_id: str, ctx: OrderExecutionContext) -> dict[str, Any]:
        timeout = float(self._settings.dnse_execution_timeout_seconds)

        def _do() -> Any:
            return trade.cancel_order(
                order_id=str(broker_order_id),
                sub_account=self._sub_account(),
                asset_type=self._asset_type(ctx),
            )

        df = self._call_with_timeout(_do, timeout)
        return self._first_row_dict(df)

    def cancel_at_broker(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
        """Request cancellation at DNSE for an existing broker order id."""
        gate = self._token_gate()
        if gate:
            return gate
        if not self._sub_account():
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_sub_account_missing",
                broker_order_id=None,
                broker_raw={},
                log_messages=["DNSE cancel: sub account not configured"],
            )
        broker_id = (ctx.broker_order_id or "").strip()
        if not broker_id:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_cancel_missing_broker_order_id",
                broker_order_id=None,
                broker_raw={},
                log_messages=["DNSE cancel: missing broker order id"],
            )
        trade = self._build_trade()
        try:
            raw = self._cancel_order(trade, broker_id, ctx)
        except TimeoutError as e:
            logger.warning("DNSE cancel timeout order_id=%s broker_id=%s err=%s", ctx.internal_order_id, broker_id, e)
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason=normalize_dnse_error(e),
                broker_order_id=broker_id,
                broker_raw={},
                log_messages=[str(e)],
            )
        except Exception as e:
            logger.exception("DNSE cancel failed order_id=%s", ctx.internal_order_id)
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason=normalize_dnse_error(e),
                broker_order_id=broker_id,
                broker_raw={},
                log_messages=[str(e)],
            )

        st = extract_dnse_status_token(raw)
        mapped = map_dnse_status_to_internal(st)
        if mapped == "CANCELLED":
            return ExecutionOutcome(
                internal_status="CANCELLED",
                reason=None,
                broker_order_id=broker_id,
                broker_raw=raw,
                log_messages=[f"DNSE cancel acknowledged status={st!r}"],
            )
        if mapped == "REJECTED":
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_cancel_rejected",
                broker_order_id=broker_id,
                broker_raw=raw,
                log_messages=[f"DNSE cancel rejected status={st!r}"],
            )
        # Pending / unknown: treat as soft success only if broker returned empty success pattern — otherwise fail closed
        if mapped in {"FILLED", "PARTIAL"}:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_cancel_ineligible_terminal",
                broker_order_id=broker_id,
                broker_raw=raw,
                log_messages=[f"DNSE cancel refused: order already terminal status={st!r}"],
            )
        return ExecutionOutcome(
            internal_status="REJECTED",
            reason="dnse_cancel_pending_or_ambiguous",
            broker_order_id=broker_id,
            broker_raw=raw,
            log_messages=[f"DNSE cancel result not terminal status={st!r} mapped={mapped}"],
        )

    def place_order_without_poll(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
        """
        Submit an order and return after interpreting the **place** response only (no ``order_detail`` polling loop).

        Production paths should keep using ``execute``, which polls until a terminal status. This method exists for
        strictly gated live E2E tests (place → snapshot → cancel) when a resting ``ACK`` order is expected.
        """
        gate = self._token_gate()
        if gate:
            return gate
        if not self._sub_account():
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_sub_account_missing",
                broker_order_id=None,
                broker_raw={},
                log_messages=["DNSE live adapter: sub account not configured (DNSE_SUB_ACCOUNT or DNSE_DEFAULT_SUB_ACCOUNT)"],
            )

        trade = self._build_trade()
        retries = max(0, int(self._settings.dnse_execution_place_retries))
        last_exc: Exception | None = None
        place_raw: dict[str, Any] = {}
        for attempt in range(retries + 1):
            try:
                place_raw = self._place_order(trade, ctx)
                break
            except Exception as e:
                last_exc = e
                logger.warning(
                    "DNSE place_order (no-poll) attempt %s/%s failed order_id=%s err=%s",
                    attempt + 1,
                    retries + 1,
                    ctx.internal_order_id,
                    e,
                )
                if attempt >= retries:
                    return ExecutionOutcome(
                        internal_status="REJECTED",
                        reason="dnse_place_error",
                        broker_order_id=None,
                        broker_raw={},
                        log_messages=[str(e)],
                    )
                time.sleep(0.35 * (attempt + 1))

        if last_exc is not None and not place_raw:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_place_error",
                broker_order_id=None,
                broker_raw={},
                log_messages=[str(last_exc)],
            )

        broker_id = extract_dnse_order_id(place_raw)
        if not broker_id:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_place_no_order_id",
                broker_order_id=None,
                broker_raw=place_raw,
                log_messages=["DNSE place response missing order id"],
            )

        st0 = extract_dnse_status_token(place_raw)
        mapped0 = map_dnse_status_to_internal(st0)
        if mapped0 in {"FILLED", "REJECTED", "CANCELLED", "PARTIAL"}:
            reason = "dnse_broker_rejected" if mapped0 == "REJECTED" else None
            return ExecutionOutcome(
                internal_status=mapped0,
                reason=reason,
                broker_order_id=broker_id,
                broker_raw=place_raw,
                log_messages=[f"DNSE place immediate status={st0!r} mapped={mapped0}"],
            )

        return ExecutionOutcome(
            internal_status="ACK",
            reason=None,
            broker_order_id=broker_id,
            broker_raw=place_raw,
            log_messages=[f"DNSE place resting status={st0!r} mapped={mapped0} (no poll)"],
        )

    def fetch_broker_order_snapshot(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
        """Single ``order_detail`` call mapped to internal status. ``ctx.broker_order_id`` must be set."""
        gate = self._token_gate()
        if gate:
            return gate
        if not self._sub_account():
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_sub_account_missing",
                broker_order_id=None,
                broker_raw={},
                log_messages=["DNSE snapshot: sub account not configured"],
            )
        broker_id = (ctx.broker_order_id or "").strip()
        if not broker_id:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_snapshot_missing_broker_order_id",
                broker_order_id=None,
                broker_raw={},
                log_messages=["DNSE snapshot: missing broker order id"],
            )
        trade = self._build_trade()
        try:
            last_raw = self._order_detail(trade, broker_id, ctx)
        except TimeoutError as e:
            logger.warning("DNSE snapshot timeout order_id=%s broker_id=%s err=%s", ctx.internal_order_id, broker_id, e)
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason=normalize_dnse_error(e),
                broker_order_id=broker_id,
                broker_raw={},
                log_messages=[str(e)],
            )
        except Exception as e:
            logger.exception("DNSE snapshot failed order_id=%s", ctx.internal_order_id)
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason=normalize_dnse_error(e),
                broker_order_id=broker_id,
                broker_raw={},
                log_messages=[str(e)],
            )

        st = extract_dnse_status_token(last_raw)
        internal = map_dnse_status_to_internal(st)
        reason = "dnse_broker_rejected" if internal == "REJECTED" else None
        return ExecutionOutcome(
            internal_status=internal,
            reason=reason,
            broker_order_id=broker_id,
            broker_raw=last_raw,
            log_messages=[f"DNSE snapshot: status={st!r} mapped={internal}"],
        )

    def _poll_until_terminal(self, trade: Any, broker_order_id: str, ctx: OrderExecutionContext) -> ExecutionOutcome:
        attempts = max(1, int(self._settings.dnse_execution_poll_attempts))
        interval = max(0.05, float(self._settings.dnse_execution_poll_interval_seconds))
        last_raw: dict[str, Any] = {}
        for i in range(attempts):
            last_raw = self._order_detail(trade, broker_order_id, ctx)
            st = extract_dnse_status_token(last_raw)
            internal = map_dnse_status_to_internal(st)
            if internal in {"FILLED", "REJECTED", "CANCELLED", "PARTIAL"}:
                reason = None
                if internal == "REJECTED":
                    reason = "dnse_broker_rejected"
                return ExecutionOutcome(
                    internal_status=internal,
                    reason=reason,
                    broker_order_id=broker_order_id,
                    broker_raw=last_raw,
                    log_messages=[f"DNSE poll {i + 1}/{attempts}: status={st!r} mapped={internal}"],
                )
            if i + 1 < attempts:
                time.sleep(interval)
        return ExecutionOutcome(
            internal_status="REJECTED",
            reason="dnse_status_pending_timeout",
            broker_order_id=broker_order_id,
            broker_raw=last_raw,
            log_messages=[f"DNSE status still pending after {attempts} polls"],
        )

    def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
        gate = self._token_gate()
        if gate:
            return gate
        if not self._sub_account():
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_sub_account_missing",
                broker_order_id=None,
                broker_raw={},
                log_messages=["DNSE live adapter: sub account not configured (DNSE_SUB_ACCOUNT or DNSE_DEFAULT_SUB_ACCOUNT)"],
            )

        trade = self._build_trade()
        broker_id = (ctx.broker_order_id or "").strip() or None
        resume = ctx.current_status.upper() in {"ACK", "PARTIAL"} and broker_id

        if resume:
            try:
                return self._poll_until_terminal(trade, broker_id, ctx)
            except TimeoutError as e:
                logger.warning("DNSE poll timeout order_id=%s broker_id=%s err=%s", ctx.internal_order_id, broker_id, e)
                return ExecutionOutcome(
                    internal_status="REJECTED",
                    reason="dnse_poll_timeout",
                    broker_order_id=broker_id,
                    broker_raw={},
                    log_messages=[str(e)],
                )
            except Exception as e:
                logger.exception("DNSE poll failed order_id=%s", ctx.internal_order_id)
                return ExecutionOutcome(
                    internal_status="REJECTED",
                    reason="dnse_poll_error",
                    broker_order_id=broker_id,
                    broker_raw={},
                    log_messages=[str(e)],
                )

        retries = max(0, int(self._settings.dnse_execution_place_retries))
        last_exc: Exception | None = None
        place_raw: dict[str, Any] = {}
        for attempt in range(retries + 1):
            try:
                place_raw = self._place_order(trade, ctx)
                break
            except Exception as e:
                last_exc = e
                logger.warning(
                    "DNSE place_order attempt %s/%s failed order_id=%s err=%s",
                    attempt + 1,
                    retries + 1,
                    ctx.internal_order_id,
                    e,
                )
                if attempt >= retries:
                    return ExecutionOutcome(
                        internal_status="REJECTED",
                        reason="dnse_place_error",
                        broker_order_id=None,
                        broker_raw={},
                        log_messages=[str(e)],
                    )
                time.sleep(0.35 * (attempt + 1))

        if last_exc is not None and not place_raw:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_place_error",
                broker_order_id=None,
                broker_raw={},
                log_messages=[str(last_exc)],
            )

        broker_id = extract_dnse_order_id(place_raw)
        if not broker_id:
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_place_no_order_id",
                broker_order_id=None,
                broker_raw=place_raw,
                log_messages=["DNSE place response missing order id"],
            )

        st0 = extract_dnse_status_token(place_raw)
        mapped0 = map_dnse_status_to_internal(st0)
        if mapped0 in {"FILLED", "REJECTED", "CANCELLED", "PARTIAL"}:
            reason = "dnse_broker_rejected" if mapped0 == "REJECTED" else None
            return ExecutionOutcome(
                internal_status=mapped0,
                reason=reason,
                broker_order_id=broker_id,
                broker_raw=place_raw,
                log_messages=[f"DNSE place immediate status={st0!r} mapped={mapped0}"],
            )

        try:
            return self._poll_until_terminal(trade, broker_id, ctx)
        except TimeoutError as e:
            logger.warning("DNSE poll timeout after place order_id=%s broker_id=%s err=%s", ctx.internal_order_id, broker_id, e)
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_poll_timeout",
                broker_order_id=broker_id,
                broker_raw=place_raw,
                log_messages=[str(e)],
            )
        except Exception as e:
            logger.exception("DNSE poll failed after place order_id=%s", ctx.internal_order_id)
            return ExecutionOutcome(
                internal_status="REJECTED",
                reason="dnse_poll_error",
                broker_order_id=broker_id,
                broker_raw=place_raw,
                log_messages=[str(e)],
            )
