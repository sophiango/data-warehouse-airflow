CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_customer;
CREATE TABLE warehouse.dim_customer AS
SELECT
  customer_id,
  signup_date,
  COALESCE(NULLIF(TRIM(region), ''), 'Unknown') AS region,
  COALESCE(NULLIF(TRIM(age_group), ''), 'Unknown') AS age_group
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
  (price * quantity) AS total_revenue,
  status
FROM staging.orders
WHERE status = 'completed';

CREATE INDEX IF NOT EXISTS idx_fact_orders_ts ON warehouse.fact_orders(order_ts);
ANALYZE warehouse.dim_customer;
ANALYZE warehouse.fact_orders;
