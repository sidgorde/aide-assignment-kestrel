# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: POS Transactions
# MAGIC
# MAGIC This notebook transforms POS transactions from Bronze to Silver.
# MAGIC It handles schema drift, timezone conversion, deduplication, and UOM conversions.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

bronze_df = spark.table("kestrel_bronze.pos_transactions")
initial_count = bronze_df.count()
print(f"Initial row count: {initial_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Schema Harmonisation (Defect L4)
# MAGIC Unify quantity columns due to schema drift around 2025-10-01.

# COMMAND ----------

# Unify qty and quantity_units into a single 'quantity' column
df = bronze_df.withColumn('quantity', F.coalesce(F.col('qty'), F.col('quantity_units')).cast('int'))

# Set default uom to 'EA' where missing (before schema drift)
df = df.withColumn('uom', F.coalesce(F.col('uom'), F.lit('EA')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplication (Defect L3)
# MAGIC Remove exact duplicate rows (at-least-once delivery semantics).

# COMMAND ----------

df_dedup = df.dropDuplicates(['txn_id', 'txn_line_no'])
dedup_count = df_dedup.count()
print(f"Removed {initial_count - dedup_count} duplicate rows.")

df = df_dedup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Timezone Conversion (Defect L2)
# MAGIC Convert UTC `event_ts` to Asia/Kolkata and derive `business_date`.

# COMMAND ----------

df = df.withColumn('event_ts_utc', F.to_timestamp('event_ts', "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df = df.withColumn('event_ts_ist', F.from_utc_timestamp('event_ts_utc', 'Asia/Kolkata'))
df = df.withColumn('business_date', F.to_date('event_ts_ist'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: UOM Conversion (Defect L16)
# MAGIC Join reference data to convert cases to eaches.

# COMMAND ----------

ref_uom = spark.table("kestrel_bronze.ref_uom_conversion")

# Left join to maintain all transactions even if SKU is missing in reference
df_joined = df.join(ref_uom, on="sku_code", how="left")

# Flag missing references and fallback to 1 eaches per case
df_joined = df_joined.withColumn(
    'uom_conversion_missing',
    F.col('eaches_per_case').isNull()
).withColumn(
    'eaches_per_case_fixed',
    F.coalesce(F.col('eaches_per_case'), F.lit(1))
)

# Calculate quantity in eaches
df_joined = df_joined.withColumn(
    'quantity_eaches',
    F.when(F.col('uom') == 'CS', F.col('quantity') * F.col('eaches_per_case_fixed'))
     .otherwise(F.col('quantity')).cast('int')
)

df = df_joined

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Compute Line Values

# COMMAND ----------

df = df.withColumn('line_gross', F.round(F.col('quantity') * F.col('unit_price'), 2))
df = df.withColumn('line_net', F.round(F.col('line_gross') - F.col('discount_amount'), 2))
df = df.withColumn('line_total', F.round(F.col('line_net') + F.col('tax_amount'), 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Late Arrival Analysis (Defect L1)
# MAGIC Calculate difference between ingest date and business date.

# COMMAND ----------

df = df.withColumn('days_late', F.datediff(F.to_date('ingest_date'), F.col('business_date')))

print("Distribution of Late Arrivals (days_late):")
display(df.groupBy('days_late').count().orderBy('days_late'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Final Schema and Write

# COMMAND ----------

final_cols = [
    'txn_id', 'txn_line_no', 'basket_id', 'outlet_code', 'channel', 'sku_code', 
    'event_ts_utc', 'event_ts_ist', 'business_date', 'ingest_date', 'days_late', 
    'quantity', 'uom', 'quantity_eaches', 'uom_conversion_missing', 'unit_price', 
    'line_gross', 'discount_amount', 'line_net', 'tax_amount', 'line_total', 
    'payment_mode', 'till_id', 'cashier_id', 'promo_code', 'loyalty_id', 'source_file'
]

df_final = df.select(*final_cols)

silver_path = "/FileStore/kestrel/delta/silver/pos_transactions"

df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("business_date") \
    .option("path", silver_path) \
    .saveAsTable("kestrel_silver.pos_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Verification

# COMMAND ----------

final_count = df_final.count()
print(f"Total Bronze Rows: {initial_count}")
print(f"Total Silver Rows: {final_count}")

print("\nDistribution of UOM:")
display(df_final.groupBy("uom").count())

print("\nSample rows:")
display(df_final.limit(5))

# COMMAND ----------
