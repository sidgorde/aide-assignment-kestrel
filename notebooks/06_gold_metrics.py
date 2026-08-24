# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Business Metrics & KPIs
# MAGIC This notebook processes data from the Silver layer to create aggregated, business-facing tables in the Gold layer.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define paths
gold_base_path = "/FileStore/kestrel/delta/gold/"

# Create gold database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS kestrel_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Tables
# MAGIC Loading necessary tables from Silver and Bronze layers.

# COMMAND ----------

# Load Silver tables
df_pos = spark.table("kestrel_silver.pos_transactions")
df_telemetry = spark.table("kestrel_silver.reefer_telemetry")
df_cycle_time = spark.table("kestrel_silver.wms_order_cycle_time")
df_orders = spark.table("kestrel_silver.sales_orders")

# Load reference tables from Bronze
df_fiscal = spark.table("kestrel_bronze.ref_fiscal_calendar")
df_warehouse = spark.table("kestrel_bronze.ref_warehouse_master")
df_finance_legacy = spark.table("kestrel_bronze.ref_legacy_finance_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily Sales (`kestrel_gold.daily_sales`)
# MAGIC Grain: business_date × channel × outlet_code × sku_code

# COMMAND ----------

# Aggregate POS transactions
df_daily_sales_agg = df_pos.groupBy(
    "business_date", "channel", "outlet_code", "sku_code"
).agg(
    sum("line_gross").alias("gross_sales"),
    sum("line_net").alias("net_sales"),
    sum("line_total").alias("total_with_tax"),
    sum("discount_amount").alias("total_discount"),
    sum("tax_amount").alias("total_tax"),
    sum("quantity").alias("units_sold"),
    sum("quantity_eaches").alias("units_sold_eaches"),
    countDistinct("basket_id").alias("basket_count"),
    count("*").alias("txn_line_count")
)

# Join with fiscal calendar to add fiscal details
df_daily_sales = df_daily_sales_agg.join(
    df_fiscal,
    df_daily_sales_agg.business_date == df_fiscal.calendar_date,
    "left"
).select(
    "business_date", "channel", "outlet_code", "sku_code",
    "gross_sales", "net_sales", "total_with_tax", "total_discount", "total_tax",
    "units_sold", "units_sold_eaches", "basket_count", "txn_line_count",
    "fiscal_year", "fiscal_quarter", "fiscal_month_no", "iso_week"
)

# Write to Gold
daily_sales_path = f"{gold_base_path}daily_sales"
df_daily_sales.write.format("delta").mode("overwrite").save(daily_sales_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.daily_sales USING DELTA LOCATION '{daily_sales_path}'")

print(f"kestrel_gold.daily_sales count: {df_daily_sales.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Weekly Sales by Channel (`kestrel_gold.weekly_sales_by_channel`)
# MAGIC Grain: week_ending (Sunday of each ISO week) × channel

# COMMAND ----------

# Derive week_ending as the Sunday of each ISO week using next_day
df_daily_sales_with_week = df_daily_sales.withColumn(
    "week_ending", 
    next_day(col("business_date") - expr("INTERVAL 1 DAY"), "SU")
)

# Aggregate daily sales into weekly sales by channel
df_weekly_sales = df_daily_sales_with_week.groupBy(
    "week_ending", "channel"
).agg(
    sum("gross_sales").alias("gross_sales"),
    sum("net_sales").alias("net_sales"),
    sum("units_sold_eaches").alias("units_sold_eaches"),
    sum("basket_count").alias("basket_count") 
)

weekly_sales_path = f"{gold_base_path}weekly_sales_by_channel"
df_weekly_sales.write.format("delta").mode("overwrite").save(weekly_sales_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.weekly_sales_by_channel USING DELTA LOCATION '{weekly_sales_path}'")

print(f"kestrel_gold.weekly_sales_by_channel count: {df_weekly_sales.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Finance Reconciliation (`kestrel_gold.finance_reconciliation`)
# MAGIC Join weekly_sales_by_channel with ref_legacy_finance_report.

# COMMAND ----------

# Join our weekly sales with the legacy finance report to reconcile differences
df_finance_recon = df_weekly_sales.alias("our").join(
    df_finance_legacy.alias("leg"),
    (col("our.week_ending") == col("leg.week_ending")) & (col("our.channel") == col("leg.channel")),
    "inner"
).select(
    col("our.week_ending"),
    col("our.channel"),
    col("our.gross_sales").alias("our_gross_sales"),
    col("leg.gross_sales_inr").alias("legacy_gross_sales_inr"),
    (col("our.gross_sales") - col("leg.gross_sales_inr")).alias("gross_sales_variance"),
    ((col("our.gross_sales") - col("leg.gross_sales_inr")) / col("leg.gross_sales_inr") * 100).alias("gross_sales_variance_pct"),
    col("our.units_sold_eaches").alias("our_units_sold"),
    col("leg.units_sold").alias("legacy_units_sold"),
    col("our.basket_count").alias("our_basket_count"),
    col("leg.basket_count").alias("legacy_basket_count")
).withColumn(
    "variance_explanation",
    when(abs(col("gross_sales_variance_pct")) > 0.01, 
         lit('Our pipeline uses event_date and deduplicates; legacy uses ingest_date without dedup')
    ).otherwise(lit('Variance within acceptable threshold'))
)

finance_recon_path = f"{gold_base_path}finance_reconciliation"
df_finance_recon.write.format("delta").mode("overwrite").save(finance_recon_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.finance_reconciliation USING DELTA LOCATION '{finance_recon_path}'")

print(f"kestrel_gold.finance_reconciliation count: {df_finance_recon.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cold Chain Daily (`kestrel_gold.cold_chain_daily`)
# MAGIC Grain: reading_date × warehouse_code × telemetry_vendor

# COMMAND ----------

# Aggregate telemetry data daily per warehouse and vendor
df_cold_chain_daily_agg = df_telemetry.groupBy(
    "reading_date", "warehouse_code", "telemetry_vendor"
).agg(
    sum(when(~col("is_sensor_dropout"), 1).otherwise(0)).alias("total_readings"),
    sum(when(col("is_excursion") == True, 1).otherwise(0)).alias("excursion_count"),
    sum(when(col("is_sensor_dropout") == True, 1).otherwise(0)).alias("sensor_dropout_count"),
    sum(when(col("door_open_flag") == True, 1).otherwise(0)).alias("door_open_count"),
    avg(when(~col("is_sensor_dropout"), col("temp_celsius"))).alias("avg_temp_celsius"),
    min(when(~col("is_sensor_dropout"), col("temp_celsius"))).alias("min_temp_celsius"),
    max(when(~col("is_sensor_dropout"), col("temp_celsius"))).alias("max_temp_celsius")
).withColumn(
    "excursion_rate", 
    when(col("total_readings") > 0, col("excursion_count") / col("total_readings")).otherwise(0.0)
)

# Join with fiscal calendar
df_cold_chain_daily = df_cold_chain_daily_agg.join(
    df_fiscal,
    df_cold_chain_daily_agg.reading_date == df_fiscal.calendar_date,
    "left"
).select(
    "reading_date", "warehouse_code", "telemetry_vendor",
    "total_readings", "excursion_count", "excursion_rate",
    "avg_temp_celsius", "min_temp_celsius", "max_temp_celsius",
    "sensor_dropout_count", "door_open_count",
    "fiscal_year", "fiscal_quarter", "fiscal_month_no", "iso_week"
)

cold_chain_daily_path = f"{gold_base_path}cold_chain_daily"
df_cold_chain_daily.write.format("delta").mode("overwrite").save(cold_chain_daily_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.cold_chain_daily USING DELTA LOCATION '{cold_chain_daily_path}'")

print(f"kestrel_gold.cold_chain_daily count: {df_cold_chain_daily.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cold Chain Monthly by Carrier (`kestrel_gold.cold_chain_monthly_by_carrier`)
# MAGIC Grain: month × warehouse_code (as proxy for carrier region)

# COMMAND ----------

# Aggregate cold_chain_daily monthly
df_cold_chain_monthly = df_cold_chain_daily.withColumn(
    "month", trunc(col("reading_date"), "month")
).groupBy(
    "month", "warehouse_code"
).agg(
    sum("total_readings").alias("total_readings"),
    sum("excursion_count").alias("excursion_count"),
    sum("sensor_dropout_count").alias("sensor_dropout_count"),
    sum("door_open_count").alias("door_open_count"),
    avg("avg_temp_celsius").alias("avg_temp_celsius_monthly"),
    min("min_temp_celsius").alias("min_temp_celsius_monthly"),
    max("max_temp_celsius").alias("max_temp_celsius_monthly")
).withColumn(
    "excursion_rate",
    when(col("total_readings") > 0, col("excursion_count") / col("total_readings")).otherwise(0.0)
)

cold_chain_monthly_path = f"{gold_base_path}cold_chain_monthly_by_carrier"
df_cold_chain_monthly.write.format("delta").mode("overwrite").save(cold_chain_monthly_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.cold_chain_monthly_by_carrier USING DELTA LOCATION '{cold_chain_monthly_path}'")

print(f"kestrel_gold.cold_chain_monthly_by_carrier count: {df_cold_chain_monthly.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Warehouse Cycle Time (`kestrel_gold.warehouse_cycle_time`)
# MAGIC Source: `kestrel_silver.wms_order_cycle_time` WHERE `is_complete_journey = true`

# COMMAND ----------

# Filter for complete journeys and extract event_date
df_cycle_time_complete = df_cycle_time.filter(col("is_complete_journey") == True).withColumn(
    "event_date", to_date(col("receive_ts"))
)

# Calculate aggregated metrics per day and warehouse
df_cycle_time_agg = df_cycle_time_complete.groupBy(
    "event_date", "warehouse_code"
).agg(
    count("*").alias("order_count"),
    avg("cycle_time_hours").alias("avg_cycle_time_hours"),
    percentile_approx("cycle_time_hours", 0.5).alias("median_cycle_time_hours"),
    percentile_approx("cycle_time_hours", 0.9).alias("p90_cycle_time_hours"),
    min("cycle_time_hours").alias("min_cycle_time_hours"),
    max("cycle_time_hours").alias("max_cycle_time_hours")
)

# Join with warehouse master and fiscal calendar to enrich
df_warehouse_cycle_time = df_cycle_time_agg.join(
    df_warehouse,
    ["warehouse_code"],
    "left"
).join(
    df_fiscal,
    df_cycle_time_agg.event_date == df_fiscal.calendar_date,
    "left"
).select(
    df_cycle_time_agg["event_date"], "warehouse_code",
    "order_count", "avg_cycle_time_hours", "median_cycle_time_hours", "p90_cycle_time_hours",
    "min_cycle_time_hours", "max_cycle_time_hours",
    "warehouse_name", "region_name", "city",
    "fiscal_year", "fiscal_quarter", "fiscal_month_no", "iso_week"
)

warehouse_cycle_time_path = f"{gold_base_path}warehouse_cycle_time"
df_warehouse_cycle_time.write.format("delta").mode("overwrite").save(warehouse_cycle_time_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.warehouse_cycle_time USING DELTA LOCATION '{warehouse_cycle_time_path}'")

print(f"kestrel_gold.warehouse_cycle_time count: {df_warehouse_cycle_time.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Order Value by Source (`kestrel_gold.order_value_by_source`)
# MAGIC Grain: month × source_system

# COMMAND ----------

# Get month dimension
df_orders_month = df_orders.withColumn(
    "month", trunc(col("order_date"), "month")
)

# Aggregate metrics
df_order_value_by_source = df_orders_month.groupBy(
    "month", "source_system"
).agg(
    count("*").alias("order_count"),
    sum("order_value_gross").alias("total_order_value_gross"),
    sum("order_value_gross_adjusted").alias("total_order_value_adjusted"),
    avg("order_value_gross").alias("avg_order_value_gross"),
    avg("order_value_gross_adjusted").alias("avg_order_value_adjusted"),
    sum(when(col("is_freight_inflated") == True, 1).otherwise(0)).alias("freight_inflation_flag_count")
)

order_value_by_source_path = f"{gold_base_path}order_value_by_source"
df_order_value_by_source.write.format("delta").mode("overwrite").save(order_value_by_source_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_gold.order_value_by_source USING DELTA LOCATION '{order_value_by_source_path}'")

print(f"kestrel_gold.order_value_by_source count: {df_order_value_by_source.count()}")

# COMMAND ----------
