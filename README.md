# 🏗️ Data Warehouse & Analytics Pipeline

This project is a **portfolio-ready data engineering pipeline** that simulates a small-scale e-commerce data warehouse. It demonstrates how to ingest raw data, transform it into analytics-ready schemas, and query it using **SQL, PySpark, and Airflow orchestration**.

---

## 🚀 Features

- **Synthetic data generation**: Customers and orders created with Python & Faker
- **Postgres staging schema**: Raw CSVs loaded into `staging.customers` and `staging.orders`
- **Warehouse star schema**:
  - `warehouse.dim_customer` — customer dimension (cleaned, deduplicated)
  - `warehouse.fact_orders` — fact table with order events, revenue metrics
- **ETL scripts**:
  - Python loader (`etl/load_to_postgres.py`) with SQLAlchemy & Pandas
  - SQL transformations to build star schema
- **PySpark analytics**:
  - KPIs: daily revenue, orders by region, top customer LTV
  - Data cleaning: handle `NULL` regions, enforce decimal precision
- **Airflow DAG (coming soon)**: Orchestrates the full pipeline
- **Dashboards (optional)**: Grafana/Metabase to visualize KPIs

---

## 🛠️ Tech Stack

- **Python** (Pandas, SQLAlchemy, Faker)
- **Postgres** (Dockerized)
- **PySpark** (batch analytics, JDBC to Postgres)
- **Airflow** (workflow orchestration, coming in next phase)
- **Grafana/Metabase** (optional visualization)


---

## ▶️ Getting Started

### 1. Setup environment
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Postgres
```bash
docker compose up -d
```

### 3. Generate mock data
```bash
python etl/generate_orders.py
```

### 4. Load into staging schema
```bash
python etl/load_to_postgres.py
```

### 5. Build warehouse schema

Run the SQL script (from psql or your IDE).

### 6. Run PySpark KPIs

Download postgres jar and replace SPARK_HOME with full path of installed spark
```bash
$SPARK_HOME/bin/spark-submit --jars ~/spark_jars/postgresql-42.7.4.jar spark/kpi.py
```
---

## 📊 Example KPIs
Revenue by day
Orders by region (with nulls handled → “Unknown”)
---
## 🎓 Learning Outcomes

This project demonstrates:
- End-to-end ETL: raw → staging → warehouse → analytics
- SQL schema design: star schema (fact + dimensions)
- PySpark analytics: batch aggregations & cleaning
- Data quality handling: type casting, null normalization
- Data engineering workflows