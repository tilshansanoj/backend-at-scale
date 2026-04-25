CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL
);

INSERT INTO products (name, price)
VALUES
    ('Laptop', 1200.00),
    ('Keyboard', 89.99),
    ('Mouse', 39.99),
    ('Monitor', 399.99),
    ('Headphones', 129.99)
ON CONFLICT DO NOTHING;
