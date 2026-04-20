-- Broker linkage + FE/execution hints for trading_core orders (also applied via ensure_trading_core_tables at runtime).
ALTER TABLE orders_core ADD COLUMN IF NOT EXISTS broker_order_id VARCHAR(128);
ALTER TABLE orders_core ADD COLUMN IF NOT EXISTS order_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_orders_core_broker_order_id
ON orders_core(broker_order_id)
WHERE broker_order_id IS NOT NULL;
