CREATE TABLE IF NOT EXISTS market_symbol_daily_volume (
    symbol VARCHAR(20) NOT NULL,
    trading_date DATE NOT NULL,
    exchange VARCHAR(16) NOT NULL,
    volume DOUBLE PRECISION NOT NULL CHECK (volume >= 0),
    close_price DOUBLE PRECISION NULL,
    high_price DOUBLE PRECISION NULL,
    low_price DOUBLE PRECISION NULL,
    source VARCHAR(16) NOT NULL DEFAULT 'VCI',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trading_date)
);

ALTER TABLE market_symbol_daily_volume
ADD COLUMN IF NOT EXISTS high_price DOUBLE PRECISION NULL;

ALTER TABLE market_symbol_daily_volume
ADD COLUMN IF NOT EXISTS low_price DOUBLE PRECISION NULL;

CREATE INDEX IF NOT EXISTS idx_market_symbol_daily_volume_exchange_date
ON market_symbol_daily_volume(exchange, trading_date DESC);
