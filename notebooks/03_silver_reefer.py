# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Kestrel Provisions - Silver Layer: Reefer Telemetry
# MAGIC 
# MAGIC This notebook processes raw reefer telemetry data from the Bronze layer and applies data quality rules to create a clean, standardized Silver dataset.
# MAGIC 
# MAGIC **Source:** `kestrel_bronze.reefer_telemetry`
# MAGIC **Target:** `kestrel_silver.reefer_telemetry` at `/FileStore/kestrel/delta/silver/reefer_telemetry`
# MAGIC 
# MAGIC ### Data Quality Defects Handled:
# MAGIC - **L5:** Firmware 2.1.4 devices have +7h clock offset. Corrected by subtracting 7 hours.
# MAGIC - **L6 & L7:** Temperature units. COLDEYE uses Fahrenheit, THERMLOG uses Celsius. Missing `temp_unit` (~8%) is inferred from the vendor, then all values are standardized to Celsius.
# MAGIC - **L8:** Sensor dropouts. ~0.6% have null `temp_value`, flagged as `is_sensor_dropout`.
# MAGIC - **L9:** Exact duplicates (~3.2%) are removed.
# MAGIC - **L10:** Gateway GW-017 outage on 2026-02-11 and 2026-02-12. (Noted: data is simply missing for those days).

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze Data

# COMMAND ----------

# Read from Bronze layer
bronze_df = spark.table("kestrel_bronze.reefer_telemetry")
bronze_count = bronze_df.count()
print(f"Total rows in Bronze: {bronze_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplication (Defect L9)
# MAGIC Remove exact duplicate records which account for ~3.2% of the dataset.

# COMMAND ----------

# Deduplicate based on all business columns, excluding `_ingest_timestamp` and partition column `dt` if needed.
# Since `_ingest_timestamp` might differ for duplicates, we drop it before dedup or dedup on subset.
business_columns = [col for col in bronze_df.columns if col not in ['_ingest_timestamp']]
dedup_df = bronze_df.dropDuplicates(subset=business_columns)

dedup_count = dedup_df.count()
print(f"Rows after deduplication: {dedup_count:,}")
print(f"Duplicates removed: {bronze_count - dedup_count:,} ({(bronze_count - dedup_count) / bronze_count * 100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse Timestamp and Fix Clock Offset (Defect L5)
# MAGIC Parse the string timestamp and fix a known +7 hour clock offset for firmware 2.1.4.

# COMMAND ----------

# Parse timestamp
df = dedup_df.withColumn('reading_ts_parsed', F.to_timestamp('reading_ts', "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Fix clock offset for firmware 2.1.4
df = df.withColumn('reading_ts_corrected',
    F.when(F.col('firmware_version') == '2.1.4',
           F.col('reading_ts_parsed') - F.expr('INTERVAL 7 HOURS'))
    .otherwise(F.col('reading_ts_parsed')))

# Flag corrected rows
df = df.withColumn('clock_corrected', (F.col('firmware_version') == '2.1.4').cast('boolean'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Normalise Temperature Units (Defects L6, L7)
# MAGIC Resolve missing units by inferring from vendor (COLDEYE = F, THERMLOG = C), then standardize all to Celsius.

# COMMAND ----------

# Infer missing temp_unit from telemetry_vendor
df = df.withColumn('temp_unit_resolved',
    F.coalesce(F.col('temp_unit'),
               F.when(F.col('telemetry_vendor') == 'COLDEYE', F.lit('F'))
                .otherwise(F.lit('C'))))

# Convert all temperatures to Celsius
df = df.withColumn('temp_celsius',
    F.when(F.col('temp_unit_resolved') == 'F',
           F.round((F.col('temp_value') - 32) * 5.0 / 9.0, 2))
    .otherwise(F.round(F.col('temp_value'), 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Flag Sensor Issues (Defect L8) & Excursion Detection
# MAGIC Flag null temperatures as sensor dropouts. Detect temperature excursions outside the target chilled band of 2-8°C.

# COMMAND ----------

# Flag sensor dropouts (null temp_value)
df = df.withColumn('is_sensor_dropout', F.col('temp_value').isNull())

# Excursion Detection (Target band: 2-8°C)
df = df.withColumn('is_excursion',
    F.when(F.col('is_sensor_dropout'), F.lit(None).cast('boolean'))
    .otherwise((F.col('temp_celsius') > 8.0) | (F.col('temp_celsius') < 2.0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derive Date and Prepare Final Schema
# MAGIC Derive `reading_date` for partitioning and select final columns.

# COMMAND ----------

# Derive date for partitioning
df = df.withColumn('reading_date', F.to_date('reading_ts_corrected'))

# Final schema selection
final_cols = [
    'device_id', 
    'telemetry_vendor', 
    'firmware_version', 
    'vehicle_registration', 
    'route_code', 
    'warehouse_code', 
    'gateway_id', 
    F.col('reading_ts_corrected').alias('reading_ts'), 
    'reading_date', 
    'clock_corrected', 
    F.col('temp_value').alias('temp_value_raw'), 
    F.col('temp_unit').alias('temp_unit_raw'), 
    'temp_unit_resolved', 
    'temp_celsius', 
    'is_sensor_dropout', 
    'is_excursion', 
    'humidity_pct', 
    'door_open_flag', 
    'gps_lat', 
    'gps_lon', 
    'battery_pct'
]

silver_df = df.select(*final_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Silver Layer

# COMMAND ----------

target_path = "/FileStore/kestrel/delta/silver/reefer_telemetry"

spark.sql("CREATE DATABASE IF NOT EXISTS kestrel_silver")

(silver_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("reading_date")
    .option("path", target_path)
    .saveAsTable("kestrel_silver.reefer_telemetry"))

print(f"Data successfully written to {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary Statistics

# COMMAND ----------

# Summary stats on Silver data
silver_count = spark.table("kestrel_silver.reefer_telemetry").count()
print(f"Bronze row count: {bronze_count:,}")
print(f"Silver row count: {silver_count:,}")
print("-" * 40)

# Temperature Distribution
print("Temperature Distribution (°C):")
silver_df.select('temp_celsius').summary('mean', 'stddev', 'min', 'max').show()

# Excursion and Dropout Rates
metrics = silver_df.select(
    (F.sum(F.when(F.col('is_excursion') == True, 1).otherwise(0)) / F.count('*') * 100).alias('excursion_rate_pct'),
    (F.sum(F.when(F.col('is_sensor_dropout') == True, 1).otherwise(0)) / F.count('*') * 100).alias('dropout_rate_pct')
).collect()[0]

print(f"Excursion Rate: {metrics['excursion_rate_pct']:.2f}%")
print(f"Sensor Dropout Rate: {metrics['dropout_rate_pct']:.2f}%")
print("-" * 40)

# Readings by Vendor
print("Readings by Vendor:")
silver_df.groupBy('telemetry_vendor').count().show()

# COMMAND ----------
