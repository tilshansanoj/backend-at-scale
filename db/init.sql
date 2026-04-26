CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL
);

-- Generate up to 1,000,000 product rows on first initialization.
-- If rows already exist, only the missing amount is inserted.
DO $$
DECLARE
    target_count BIGINT := 1000000;
    existing_count BIGINT;
    missing_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO existing_count FROM products;
    missing_count := GREATEST(target_count - existing_count, 0);

    IF missing_count > 0 THEN
        INSERT INTO products (name, price)
        SELECT
            'Product-' || LPAD(gs::text, 7, '0') AS name,
            ROUND((10 + random() * 4990)::numeric, 2) AS price
        FROM generate_series(1, missing_count) AS gs;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
-- List endpoint: ORDER BY id LIMIT 100 — INCLUDE helps index-only scans after VACUUM fills the visibility map.
CREATE INDEX IF NOT EXISTS idx_products_id_list ON products (id) INCLUDE (name, price);
ANALYZE products;

DO $$ BEGIN
    CREATE TYPE order_status AS ENUM (
        'waiting',
        'order_received',
        'sent_for_shipping',
        'completed'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    product_id BIGINT NOT NULL REFERENCES products (id),
    quantity INT NOT NULL CHECK (quantity > 0 AND quantity <= 100000),
    status order_status NOT NULL DEFAULT 'waiting',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_status_id ON orders (status, id);
CREATE INDEX IF NOT EXISTS idx_orders_request_id ON orders (request_id);

CREATE OR REPLACE FUNCTION orders_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS orders_updated_at ON orders;
CREATE TRIGGER orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION orders_touch_updated_at();

ANALYZE orders;
