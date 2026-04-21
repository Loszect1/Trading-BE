# FastAPI VNStock Server

Backend service using FastAPI + vnstock.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Testing

```bash
pip install -r requirements-dev.txt
# Optional: dedicated DB (defaults to DATABASE_URL, else postgresql://postgres:postgres@127.0.0.1:5432/trading)
# PowerShell/cmd: set TEST_DATABASE_URL=postgresql://user:pass@127.0.0.1:5432/trading_test
# Unix:          export TEST_DATABASE_URL=postgresql://user:pass@127.0.0.1:5432/trading_test
```

### Commands (CI-friendly defaults)

- **Fast / no database** (pure unit):

```bash
pytest tests/unit -q
```

- **Integration + replay** (PostgreSQL required; skipped gracefully if the server is not reachable):

```bash
pytest tests/ -m postgres -q
```

- **Full suite** (unit + integration; Postgres-marked tests skip if DB down):

```bash
pytest tests/ -q
```

- **Exclude Postgres-marked tests** (same as unit-only plus any future non-marked tests):

```bash
pytest tests/ -q -m "not postgres"
```

- **Coverage + gate** (same selection as CI: exclude optional live vnstock tests; `TEST_DATABASE_URL` recommended so Postgres-marked tests execute):

```bash
pytest tests/ -q -m "not network_vnstock and not dnse_live" --cov-config=.coveragerc --cov=app --cov-report=term-missing --cov-fail-under=28
```

Coverage includes all `app/routers/*` in the aggregate (only `app/main.py` is omitted). `tests/unit/test_routers_thin.py` exercises at the HTTP layer (TestClient + monkeypatch, no DB/network): `monitoring`, `automation`, `trading_core`, `scanner` (schedule), `signal_engine`, `experience`, and `health`. Routers without thin tests (`ai`, `auto_trading`, `dnse_trade`, `market`, `news`, `vnstock_api`, …) still weigh on the headline total until covered similarly.

- **Optional live vnstock smoke** (network; never part of default CI):

```powershell
set RUN_VNSTOCK_NETWORK=1
pytest tests/integration/test_vnstock_network_optional.py -q -m network_vnstock
```

- **Optional live DNSE broker smoke** (real DNSE API; read-only `sub_accounts` via `DnseLiveExecutionAdapter.readonly_list_sub_accounts`; never part of default CI):

```powershell
set DNSE_ACCESS_TOKEN=...
set DNSE_TRADING_TOKEN=...
set RUN_DNSE_LIVE_TESTS=1
pytest tests/integration/test_dnse_live_optional.py -q -m dnse_live
```

- **Optional live DNSE place / poll / cancel E2E** (sends **real** orders on your DNSE sub-account when the broker accepts the place step; **never** part of default CI). Hard opt-in: `RUN_DNSE_LIVE_TESTS=1` **and** `RUN_DNSE_LIVE_E2E=1`, plus `DNSE_LIVE_E2E_SYMBOL_ALLOWLIST` and `DNSE_LIVE_E2E_SYMBOL` (symbol must be in the allowlist). Caps: `DNSE_LIVE_E2E_MAX_QTY` (default 1, max 100), `DNSE_LIVE_E2E_MAX_PRICE` (default 50_000, max 1_000_000). If the broker rejects placement, the test **skips** (dry-run fallback) instead of failing.

```powershell
set DNSE_ACCESS_TOKEN=...
set DNSE_TRADING_TOKEN=...
set DNSE_SUB_ACCOUNT=...   # or DNSE_DEFAULT_SUB_ACCOUNT
set RUN_DNSE_LIVE_TESTS=1
set RUN_DNSE_LIVE_E2E=1
set DNSE_LIVE_E2E_SYMBOL_ALLOWLIST=VNM
set DNSE_LIVE_E2E_SYMBOL=VNM
set DNSE_LIVE_E2E_LIMIT_PRICE=1000
set DNSE_LIVE_E2E_QTY=1
pytest tests/integration/test_dnse_live_e2e_optional.py -q -m "dnse_live and dnse_live_e2e"
```

Use only on a **safe test account** during market hours; prefer a limit price far from the market so the order rests as `ACK` and can be cancelled. Review `.env.example` for all E2E variables.

Scanner/signal **business logic** is covered with a **fixture vnstock API** (deterministic OHLCV, no network) in `tests/integration/test_pipeline_fixture_scanner.py`. The legacy **fully stubbed** batch remains in `tests/integration/test_pipeline_stub_scanner.py`. **Live** vnstock is intentionally not required in default CI (no flaky network).

**Replay fixtures:** JSON timelines under `tests/fixtures/replay/` plus **sanitized log v1** (`format_version: sanitized_trade_log_v1`), **raw production trade logs** (registered `format_version` values in `tests/replay/replay_schema_registry.py`: canonical **`raw_production_trade_log_v1`**, **`raw_production_trade_log_v2`** migrated to v1 via `migrate_raw_v2_to_v1` — `event`→`record_type`, strip `capture_marker` / `deploy_span` / `trace_context`, strip header-only `deploy_schema` / `capture_sdk` / `schema_minor` — and **`raw_production_trade_log_v0_compat`** for legacy migration tests only). After migration, **strict v1 record contracts** (`validate_canonical_raw_v1_record_contracts`) run before the parser. **Body record types** include clock/settlement/place plus **`cancel_order`**, **`reconcile_order`**, **`risk_event`**, **`broker_snapshot`** (maps to `get_monitoring_summary`); aliases **`order.cancel`**, **`order.reconcile`**, **`risk.log`**, **`monitoring.snapshot`** normalize to those types. JSON bundles use the same `format_version` on the outer object plus a `records` / `lines` / `events` array; the inner header’s `format_version`, if present, must match the bundle. **NDJSON** raw captures must start with a **header** line (`role: "header"` or `record_type: "header"`); the first line’s `format_version` must be registered—unknown `raw_production_trade_log_*` versions raise a clear `ValueError`. Plain **NDJSON step streams** (`op` per line, no raw `format_version`) are unchanged. Examples: `raw_production_trade_log_v1_extra_capture.ndjson` / `.bundle.json` and `raw_production_trade_log_v2_extra_capture.*`. Load with `tests/replay/replay_loader.py`, run with `tests/replay/replay_runner.py`. Sensitive keys in raw captures are scrubbed by `tests/replay/production_log_redact.py` (`redact_sensitive_fields`) before normalization.

## Run localhost

```bash
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Run Redis (Docker)

```bash
docker compose --env-file .env.docker up -d redis
```

Environment:
- `REDIS_URL` default: `redis://127.0.0.1:6379/0`
- `AI_CACHE_TTL_SECONDS` default: `86400` (1 day)
- `LISTING_EXCHANGE_INDUSTRY_REDIS_TTL_SECONDS` default: `31536000` (~365 days): Redis cache for `POST /vnstock-api/listing/symbols-by-exchange` and `symbols-by-industries` (sàn + ngành)

## API

- Health: `GET /health`
- AI generate text (Claude): `POST /ai/generate`
- AI analyze symbol (vnstock + Claude): `POST /ai/analyze-symbol`
  - Redis cache key is based on symbol + interval + lookback_days + model + max_tokens + temperature
  - Response includes `cached: true|false`
- Market history: `GET /market/history?symbol=VCI&start=2024-01-01&end=2024-01-31&interval=1D`
- Gmail fetch + download to local disk: `POST /gmail/fetch-download`
  - Uses OAuth desktop flow with readonly scope (`gmail.readonly`).
  - First call may open browser consent and create token cache file.
- Full vnstock api gateway: `POST /vnstock-api/*`
  - Quote: `/vnstock-api/quote/{history|intraday|price-depth}`
  - Company: `/vnstock-api/company/{overview|shareholders|officers|subsidiaries|affiliate|news|events}`
  - Financial: `/vnstock-api/financial/{balance-sheet|income-statement|cash-flow|ratio}`
  - Listing: `/vnstock-api/listing/{all-symbols|symbols-by-industries|symbols-by-exchange|industries-icb|symbols-by-group|all-future-indices|all-government-bonds|all-covered-warrant|all-bonds}`
  - Trading: `/vnstock-api/trading/{trading-stats|side-stats|price-board|price-history|foreign-trade|prop-trade|insider-deal|order-stats}`

Example payload (POST):

```json
{
  "source": "VCI",
  "symbol": "VCI",
  "method_kwargs": {
    "start": "2024-01-02",
    "end": "2024-01-10",
    "interval": "1D"
  }
}
```

Example AI payload (POST `/ai/generate`):

```json
{
  "prompt": "Tom tat nhanh tin hieu ky thuat co phieu HPG trong 3 dong.",
  "system_prompt": "Ban la tro ly phan tich tai chinh, tra loi ngan gon, ro rang.",
  "model": "claude-sonnet-4-6",
  "max_tokens": 300,
  "temperature": 0.2
}
```

Example AI symbol analysis payload (POST `/ai/analyze-symbol`):

```json
{
  "symbol": "HPG",
  "interval": "1D",
  "lookback_days": 90,
  "source": "VCI",
  "model": "claude-sonnet-4-6",
  "max_tokens": 700,
  "temperature": 0.2
}
```

Note:
- Do not send `"model": "string"` from API docs default examples.
- If `model` is missing or invalid placeholder, backend now auto-falls back to `CLAUDE_MODEL`.

## DNSE Trading API

- Login (JWT): `POST /dnse/auth/login`
- Trading token (OTP): `POST /dnse/auth/trading-token`
- Place order: `POST /dnse/orders/place`

### Trading core REAL execution (optional)

`POST /execution/place` + `POST /execution/process/{order_id}` drive the internal order state machine. For `account_mode=REAL`, broker integration is selected by `REAL_EXECUTION_ADAPTER` in `.env`:

- `demo` (default): simulated fills, same behavior as historical internal engine (safe if DNSE is down).
- `dnse_live`: calls DNSE via vnstock `Trade` using `DNSE_ACCESS_TOKEN`, `DNSE_TRADING_TOKEN`, and `DNSE_SUB_ACCOUNT` (or `DNSE_DEFAULT_SUB_ACCOUNT`). On misconfiguration, timeouts, or hard errors, orders end in `REJECTED` with a clear reason (no silent fills).

**Tokens (server-side):** before any live call, the adapter validates that access and trading tokens are non-empty, meet minimum length, and that JWT-shaped access tokens have three non-empty dot segments. Failures map to stable `reason` codes such as `dnse_tokens_missing`, `dnse_access_token_too_short`, or `dnse_access_token_malformed_jwt` (values are never logged). Refresh the trading token out of band via `POST /dnse/auth/trading-token` when the broker returns unauthorized or ambiguous cancel errors.

**Automated trading-token refresh (optional):** set `DNSE_TRADING_TOKEN_REFRESH_ENABLED=true` to start a background loop on application startup. The loop calls the same broker path as `POST /dnse/auth/trading-token` on a fixed interval (`DNSE_TRADING_TOKEN_REFRESH_INTERVAL_SECONDS`, minimum 60), with an independent timeout (`DNSE_TRADING_TOKEN_REFRESH_TIMEOUT_SECONDS`). When a refresh succeeds, the new token is stored in memory and used by `dnse_live` ahead of `DNSE_TRADING_TOKEN` from the environment. Failed refreshes keep the last good cached or env token and emit structured logs (`dnse_trading_token_refresh_skipped`, `dnse_trading_token_refresh_failed`, and so on) without logging secrets. If `DNSE_TRADING_TOKEN_REFRESH_SMART_OTP=false`, you must supply `DNSE_REFRESH_OTP` for each refresh attempt. Leave the feature disabled unless you have a production-safe OTP strategy (for example smart OTP where vnstock can obtain the trading token without embedding static OTPs in config).

**Cancel / partial sync:**

- `POST /execution/cancel` on `REAL` + `dnse_live`: if `broker_order_id` is set and the order is still `SENT` / `ACK` / `PARTIAL`, the backend calls the broker cancel API first. If the broker refuses or the call fails, the order stays open and the response returns `cancelled: false` with a normalized `reason` (fail-closed). Orders without a broker id are cancelled internally only (safe fallback when nothing was placed at DNSE).
- `POST /execution/reconcile/{order_id}`: re-runs `process_order` for `NEW` / `SENT` / `ACK` / `PARTIAL` so `PARTIAL` orders poll the broker again; partial fills also persist a compact `dnse_last_reconcile` object inside `order_metadata` for auditing.

See `.env.example` for timeout, retry, and poll settings.

Example place order payload:

```json
{
  "username": "your_dnse_username",
  "password": "your_dnse_password",
  "otp": "123456",
  "smart_otp": true,
  "sub_account": "022C123456",
  "symbol": "VCI",
  "side": "buy",
  "quantity": 100,
  "price": 23000,
  "order_type": "LO",
  "loan_package_id": null,
  "asset_type": "stock"
}
```

