# FastAPI VNStock Server

Backend service using FastAPI + vnstock.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

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

- Authenticate DNSE account: `POST /dnse/auth`
- Place order: `POST /dnse/orders/place`

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

