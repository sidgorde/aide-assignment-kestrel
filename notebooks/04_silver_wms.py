# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: WMS Scan Events
# MAGIC Clean and process WMS scan events, standardizing timestamps and calculating order cycle times. 
# MAGIC Handles missing scan events (Defect L11) by calculating journey completeness.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze Data

# COMMAND ----------

# Read from bronze table
df_bronze = spark.table("kestrel_bronze.wms_scan_events")
print(f"Total rows in bronze: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse Timestamp & Validate Event Types

# COMMAND ----------

# Step 1: Parse Timestamp
# event_ts is string 'YYYY-MM-DD HH:MM:SS', local time (Asia/Kolkata)
df_parsed = df_bronze.withColumn('event_ts_parsed', F.to_timestamp('event_ts', 'yyyy-MM-dd HH:mm:ss')) \
                     .withColumn('event_date', F.to_date('event_ts_parsed'))

# Step 2: Validate Event Types and Assign Numeric Stage Order
# Valid stages: RECEIVE (1), PUTAWAY (2), PICK (3), PACK (4), STAGE (5), DISPATCH (6)
df_clean = df_parsed.withColumn('stage_order', 
    F.when(F.col('event_type') == 'RECEIVE', 1)
    .when(F.col('event_type') == 'PUTAWAY', 2)
    .when(F.col('event_type') == 'PICK', 3)
    .when(F.col('event_type') == 'PACK', 4)
    .when(F.col('event_type') == 'STAGE', 5)
    .when(F.col('event_type') == 'DISPATCH', 6)
    .otherwise(0)
)

print("Scans by event_type:")
df_clean.groupBy('event_type', 'stage_order').count().orderBy('stage_order').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute Dock-to-Dispatch Cycle Time & Order Journey Status

# COMMAND ----------

# Calculate cycle time and flags (Defect L11 - missing scan events)
# Aggregate per order_number and warehouse_code
df_cycle = df_clean.filter(F.col("order_number").isNotNull()).groupBy('order_number', 'warehouse_code').agg(
    F.min(F.when(F.col('event_type') == 'RECEIVE', F.col('event_ts_parsed'))).alias('receive_ts'),
    F.max(F.when(F.col('event_type') == 'DISPATCH', F.col('event_ts_parsed'))).alias('dispatch_ts'),
    F.collect_set('event_type').alias('stages_present'),
    F.countDistinct('event_type').alias('stage_count')
)

# Add flags and calculate cycle time in hours
df_cycle = df_cycle.withColumn(
    'has_receive', F.col('receive_ts').isNotNull()
).withColumn(
    'has_dispatch', F.col('dispatch_ts').isNotNull()
).withColumn(
    'is_complete_journey', F.col('has_receive') & F.col('has_dispatch')
).withColumn(
    'cycle_time_hours',
    F.round((F.unix_timestamp('dispatch_ts') - F.unix_timestamp('receive_ts')) / 3600.0, 2)
)

print(f"Total orders processed for cycle time: {df_cycle.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Output DataFrames

# COMMAND ----------

# Output 1: Silver WMS Scan Events (all individual scans)
df_silver_scans = df_clean.select(
    'scan_id', 
    'warehouse_code', 
    'event_type', 
    'stage_order', 
    'order_number', 
    'sku_code', 
    'batch_id', 
    'qty_cases', 
    'pallet_id', 
    'dock_door', 
    'operator_id', 
    'handheld_device', 
    F.col('event_ts_parsed').alias('event_ts'), 
    'event_date'
)

# Output 2: Silver WMS Order Cycle Time
df_silver_cycle = df_cycle.select(
    'order_number', 
    'warehouse_code', 
    'receive_ts', 
    'dispatch_ts', 
    'cycle_time_hours', 
    'stages_present', 
    'stage_count', 
    'has_receive', 
    'has_dispatch', 
    'is_complete_journey'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Silver Layer

# COMMAND ----------

# Create Database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS kestrel_silver")

# Write WMS Scan Events
scans_path = "/FileStore/kestrel/delta/silver/wms_scan_events"
df_silver_scans.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .option("path", scans_path) \
    .saveAsTable("kestrel_silver.wms_scan_events")

print(f"Written to kestrel_silver.wms_scan_events at {scans_path}")

# Write WMS Order Cycle Time
cycle_path = "/FileStore/kestrel/delta/silver/wms_order_cycle_time"
df_silver_cycle.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", cycle_path) \
    .saveAsTable("kestrel_silver.wms_order_cycle_time")

print(f"Written to kestrel_silver.wms_order_cycle_time at {cycle_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality & Statistics Check

# COMMAND ----------

print("--- Order Journey Statistics ---")
df_silver_cycle.groupBy("is_complete_journey").count().show()

print("--- Stage Count per Order ---")
df_silver_cycle.groupBy("stage_count").count().orderBy("stage_count").show()

print("--- Cycle Time Statistics (Complete Journeys) ---")
df_silver_cycle.filter("is_complete_journey").select(
    F.mean("cycle_time_hours").alias("mean_cycle_time"),
    F.expr("percentile_approx(cycle_time_hours, 0.5)").alias("median_cycle_time"),
    F.expr("percentile_approx(cycle_time_hours, 0.9)").alias("p90_cycle_time"),
    F.min("cycle_time_hours").alias("min_cycle_time"),
    F.max("cycle_time_hours").alias("max_cycle_time")
).show()

# COMMAND ----------
