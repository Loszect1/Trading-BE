ALTER TABLE orders_core
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128);

ALTER TABLE orders_core
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_core_idempotency_key
ON orders_core(idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS order_events (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders_core(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_events_order_created
ON order_events(order_id, created_at);
