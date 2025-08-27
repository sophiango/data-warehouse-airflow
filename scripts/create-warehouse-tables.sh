docker compose exec -T postgres psql -U de_user -d de <<'SQL'
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_customer;
CREATE TABLE warehouse.dim_customer AS
SELECT
  customer_id,
  signup_date,
  region,
  age_group
FROM staging.customers;

DROP TABLE IF EXISTS warehouse.fact_orders;
CREATE TABLE warehouse.fact_orders AS
SELECT
  order_id,
  customer_id,
  order_ts,
  product_category,
  price,
  quantity,
  (price::numeric * quantity::int) AS total_revenue,
  status
FROM staging.orders
WHERE status = 'completed';

CREATE INDEX idx_fact_orders_ts ON warehouse.fact_orders(order_ts);
SQL
