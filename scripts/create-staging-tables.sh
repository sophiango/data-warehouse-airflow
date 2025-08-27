docker compose exec -T postgres psql -U de_user -d de <<'SQL'
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.customers;
CREATE TABLE staging.customers (
    customer_id UUID PRIMARY KEY,
    signup_date DATE,
    region TEXT,
    age_group TEXT
);

DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    order_ts TIMESTAMP,
    product_category TEXT,
    price NUMERIC,
    quantity INT,
    status TEXT
);
SQL
