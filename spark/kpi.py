from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, count, desc, date_format, coalesce, col, lit

# Build SparkSession with Postgres driver pulled from Maven
spark = (
    SparkSession.builder
        .appName("DataWarehouse")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
)

fact_orders = (
    spark.read
         .format("jdbc")
         .option("url", "jdbc:postgresql://localhost:5432/de")
         .option("dbtable", "warehouse.fact_orders")
         .option("user", "de_user")
         .option("password", "de_pass")
         .option("driver", "org.postgresql.Driver")
         .load()
)

dim_customers = (
    spark.read
         .format("jdbc")
         .option("url", "jdbc:postgresql://localhost:5432/de")
         .option("dbtable", "warehouse.dim_customer")
         .option("user", "de_user")
         .option("password", "de_pass")
         .option("driver", "org.postgresql.Driver")
         .load()
)

# KPI 1: Revenue by day
revenue_by_day = (
    fact_orders.groupBy(date_format("order_ts", "yyyy-MM-dd").alias("order_date"))
               .agg(round(sum("total_revenue"), 2).alias("revenue"))
               .orderBy("order_date")
)
revenue_by_day.show(10, truncate=False)

# KPI 2: Orders by region
orders_by_region = (
    fact_orders.join(dim_customers, "customer_id", "left")
               .groupBy(coalesce(col("region"), lit("unknown")).alias("region"))
               .agg(count("*").alias("orders"))
               .orderBy(desc("orders"))
)
orders_by_region.show()

# KPI 3 (optional): Top product categories
top_products = (
    fact_orders.groupBy("product_category")
               .agg(count("*").alias("orders"))
               .orderBy(desc("orders"))
)
top_products.show()

spark.stop()
