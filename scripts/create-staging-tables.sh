docker compose exec -T postgres psql -U de_user -d de <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.customers;
CREATE TABLE staging.customers (
    customer_id TEXT PRIMARY KEY,
    signup_date DATE,
    region TEXT,
    age_group TEXT
);

DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_ts TIMESTAMP,
    product_category TEXT,
    price NUMERIC,
    quantity INT,
    status TEXT
);
SQL
