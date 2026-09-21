-- Initial schema for cafe orders.
CREATE TABLE IF NOT EXISTS menu_items (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
    available   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id               TEXT PRIMARY KEY,
    kiosk_id         TEXT NOT NULL,
    customer_name    TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    total_cents      INTEGER NOT NULL CHECK (total_cents >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_status_created_at
    ON orders (status, created_at);

CREATE TABLE IF NOT EXISTS order_items (
    id               BIGSERIAL PRIMARY KEY,
    order_id         TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    menu_item_id     TEXT NOT NULL REFERENCES menu_items(id),
    quantity         INTEGER NOT NULL CHECK (quantity >= 1),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON order_items (order_id);

-- Seed a small cafe menu for kiosks.
INSERT INTO menu_items (id, name, description, price_cents, available) VALUES
    ('latte',   'Latte',   'Espresso with steamed milk', 450, TRUE),
    ('americano','Americano','Espresso with hot water', 350, TRUE),
    ('cappuccino','Cappuccino','Espresso with foam', 425, TRUE),
    ('muffin',  'Blueberry Muffin', 'Fresh baked muffin', 300, TRUE),
    ('bagel',   'Bagel',   'Toasted bagel with cream cheese', 375, TRUE)
ON CONFLICT (id) DO NOTHING;
