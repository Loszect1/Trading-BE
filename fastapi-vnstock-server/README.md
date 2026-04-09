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

## API

- Health: `GET /health`
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

