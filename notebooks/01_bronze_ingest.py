# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Kestrel Provisions - Bronze Ingest (Layer 1)
# MAGIC Ingests ALL raw feeds from Parquet and CSV into Delta Lake tables with NO transformations (raw copy).

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.utils import AnalysisException
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

# Setup widgets and basic variables
dbutils.widgets.text("data_path", "/FileStore/kestrel/data")
data_path = dbutils.widgets.get("data_path")

spark.sql("CREATE DATABASE IF NOT EXISTS kestrel_bronze")

# Dictionary to hold row counts for summary
row_counts = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. POS Transactions
# MAGIC Partitioned by `ingest_date=YYYY-MM-DD`
# MAGIC Schema drift at 2025-10-01 (L4). Requires `mergeSchema=true`.

# COMMAND ----------

pos_raw_path = f"{data_path}/raw/pos_transactions/"
pos_delta_path = "/FileStore/kestrel/delta/bronze/pos_transactions"

# Use mergeSchema=true to handle schema drift
df_pos = spark.read.option("mergeSchema", "true").parquet(pos_raw_path)
df_pos = df_pos.withColumn("_ingest_timestamp", current_timestamp())

df_pos.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(pos_delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.pos_transactions USING DELTA LOCATION '{pos_delta_path}'")

cnt = df_pos.count()
row_counts["pos_transactions"] = cnt
print(f"POS Transactions Row Count: {cnt}")
df_pos.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Reefer Telemetry
# MAGIC Partitioned by `dt=YYYY-MM-DD`
# MAGIC **CRITICAL**: Truncated parquet file at `dt=2025-07-14/part-00000.parquet` (L18). Handle with try/except. Read partition by partition.

# COMMAND ----------

reefer_raw_path = f"{data_path}/raw/reefer_telemetry/"
reefer_delta_path = "/FileStore/kestrel/delta/bronze/reefer_telemetry"

# List directories which represent partitions
partitions = [p for p in dbutils.fs.ls(reefer_raw_path) if p.isDir() or p.name.startswith("dt=")]
valid_dfs = []
skipped_partitions = []

print(f"Found {len(partitions)} partitions for reefer_telemetry.")

for p in partitions:
    try:
        df_part = spark.read.parquet(p.path)
        valid_dfs.append(df_part)
    except Exception as e:
        print(f"Skipping corrupted partition: {p.path}")
        skipped_partitions.append(p.path)

if valid_dfs:
    # Union all valid partitions
    df_reefer = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), valid_dfs)
else:
    # Fallback to empty schema if somehow all fail
    df_reefer = spark.createDataFrame([], StructType([]))

df_reefer = df_reefer.withColumn("_ingest_timestamp", current_timestamp())

# Write to delta
df_reefer.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(reefer_delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.reefer_telemetry USING DELTA LOCATION '{reefer_delta_path}'")

cnt = df_reefer.count()
row_counts["reefer_telemetry"] = cnt
print(f"Reefer Telemetry Row Count: {cnt}")
print(f"Skipped {len(skipped_partitions)} corrupted partitions.")
df_reefer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. WMS Scan Events
# MAGIC Partitioned by `dt=YYYY-MM-DD`. Straightforward ingest.

# COMMAND ----------

wms_raw_path = f"{data_path}/raw/wms_scan_events/"
wms_delta_path = "/FileStore/kestrel/delta/bronze/wms_scan_events"

df_wms = spark.read.parquet(wms_raw_path)
df_wms = df_wms.withColumn("_ingest_timestamp", current_timestamp())

df_wms.write.format("delta").mode("overwrite").save(wms_delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.wms_scan_events USING DELTA LOCATION '{wms_delta_path}'")

cnt = df_wms.count()
row_counts["wms_scan_events"] = cnt
print(f"WMS Scan Events Row Count: {cnt}")
df_wms.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. ERP CDC — Outlet Master

# COMMAND ----------

outlet_raw_path = f"{data_path}/raw/erp_cdc/outlet_master/"
outlet_delta_path = "/FileStore/kestrel/delta/bronze/erp_outlet_master"

df_outlet = spark.read.parquet(outlet_raw_path)
df_outlet = df_outlet.withColumn("_ingest_timestamp", current_timestamp())

df_outlet.write.format("delta").mode("overwrite").save(outlet_delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.erp_outlet_master USING DELTA LOCATION '{outlet_delta_path}'")

cnt = df_outlet.count()
row_counts["erp_outlet_master"] = cnt
print(f"ERP Outlet Master Row Count: {cnt}")
df_outlet.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. ERP CDC — Product Master

# COMMAND ----------

product_raw_path = f"{data_path}/raw/erp_cdc/product_master/"
product_delta_path = "/FileStore/kestrel/delta/bronze/erp_product_master"

df_product = spark.read.parquet(product_raw_path)
df_product = df_product.withColumn("_ingest_timestamp", current_timestamp())

df_product.write.format("delta").mode("overwrite").save(product_delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.erp_product_master USING DELTA LOCATION '{product_delta_path}'")

cnt = df_product.count()
row_counts["erp_product_master"] = cnt
print(f"ERP Product Master Row Count: {cnt}")
df_product.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. ERP CDC — Sales Order Header

# COMMAND ----------

sales_order_raw_path = f"{data_path}/raw/erp_cdc/sales_order_header/"
sales_order_delta_path = "/FileStore/kestrel/delta/bronze/erp_sales_order_header"

df_sales_order = spark.read.parquet(sales_order_raw_path)
df_sales_order = df_sales_order.withColumn("_ingest_timestamp", current_timestamp())

df_sales_order.write.format("delta").mode("overwrite").save(sales_order_delta_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.erp_sales_order_header USING DELTA LOCATION '{sales_order_delta_path}'")

cnt = df_sales_order.count()
row_counts["erp_sales_order_header"] = cnt
print(f"ERP Sales Order Header Row Count: {cnt}")
df_sales_order.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Reference Tables
# MAGIC Load ALL CSV files from `data/reference/` into Delta tables in `kestrel_bronze`

# COMMAND ----------

ref_data_path = f"{data_path}/reference/"

ref_files = [
    ("uom_conversion.csv", "ref_uom_conversion"),
    ("warehouse_master.csv", "ref_warehouse_master"),
    ("carrier_master.csv", "ref_carrier_master"),
    ("fiscal_calendar.csv", "ref_fiscal_calendar"),
    ("legacy_finance_weekly_report.csv", "ref_legacy_finance_report")
]

for filename, table_name in ref_files:
    file_path = f"{ref_data_path}/{filename}"
    delta_path = f"/FileStore/kestrel/delta/bronze/{table_name}"
    
    # Read CSV with headers
    df_ref = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    df_ref = df_ref.withColumn("_ingest_timestamp", current_timestamp())
    
    df_ref.write.format("delta").mode("overwrite").save(delta_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS kestrel_bronze.{table_name} USING DELTA LOCATION '{delta_path}'")
    
    cnt = df_ref.count()
    row_counts[table_name] = cnt
    print(f"Reference Table: {table_name} | Row Count: {cnt}")
    df_ref.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary of Bronze Layer

# COMMAND ----------

print("========================================")
print("BRONZE LAYER INGESTION SUMMARY")
print("========================================")
print(f"{'Table Name':<35} | {'Row Count':<15}")
print("-" * 55)
for table, count in row_counts.items():
    print(f"{table:<35} | {count:<15}")
print("========================================")

# COMMAND ----------
