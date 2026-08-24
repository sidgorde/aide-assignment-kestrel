# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: ERP CDC Processing
# MAGIC This notebook processes the CDC streams for the ERP systems, creating current state and history tables.
# MAGIC It resolves defects related to extract timing, tie-breaking, and inflated values.

# COMMAND ----------

from pyspark.sql.functions import col, row_number, lead, when, lit
from pyspark.sql.window import Window

# Ensure databases exist
spark.sql("CREATE DATABASE IF NOT EXISTS kestrel_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS kestrel_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Outlet Master CDC Processing
# MAGIC Create `outlet_history` (Type-2 SCD) and `outlet_current` (Type-1).
# MAGIC Fix L12: Do not rely on `extract_date`. Use `__op_ts` and `__seq` DESC.
# MAGIC Fix L13: When multiple records have the same `__op_ts`, use `__seq` as tiebreaker.

# COMMAND ----------

# Read bronze outlet master
outlet_df = spark.table("kestrel_bronze.erp_outlet_master")

# Define window for history
window_history = Window.partitionBy("outlet_code").orderBy(col("__op_ts").asc(), col("__seq").asc())

outlet_history = outlet_df.withColumn("valid_from", col("__op_ts")) \
    .withColumn("valid_to", lead("__op_ts").over(window_history))

outlet_history.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/FileStore/kestrel/delta/silver/outlet_history") \
    .saveAsTable("kestrel_silver.outlet_history")

# Define window for current state
window_current_outlet = Window.partitionBy("outlet_code").orderBy(col("__op_ts").desc(), col("__seq").desc())

outlet_current = outlet_df.withColumn("rn", row_number().over(window_current_outlet)) \
    .filter(col("rn") == 1) \
    .filter(col("__op") != "D") \
    .drop("rn")

outlet_current.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/FileStore/kestrel/delta/silver/outlet_current") \
    .saveAsTable("kestrel_silver.outlet_current")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Product Master CDC Processing
# MAGIC Create `product_current` (Type-1).

# COMMAND ----------

# Read bronze product master
product_df = spark.table("kestrel_bronze.erp_product_master")

# Define window for current state
window_current_product = Window.partitionBy("sku_code").orderBy(col("__op_ts").desc(), col("__seq").desc())

product_current = product_df.withColumn("rn", row_number().over(window_current_product)) \
    .filter(col("rn") == 1) \
    .filter(col("__op") != "D") \
    .drop("rn")

product_current.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/FileStore/kestrel/delta/silver/product_current") \
    .saveAsTable("kestrel_silver.product_current")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sales Order Header Processing
# MAGIC Process Sales orders to keep the latest record.
# MAGIC Fix L15: Exclude orders where the latest operation is `__op = 'D'`.
# MAGIC Fix L14: Adjust `order_value_gross` for `PARTNER_API` to remove freight double-count.

# COMMAND ----------

# Read bronze sales order header
sales_order_df = spark.table("kestrel_bronze.erp_sales_order_header")

# Define window for current state
window_current_so = Window.partitionBy("order_number").orderBy(col("__op_ts").desc(), col("__seq").desc())

sales_orders_current = sales_order_df.withColumn("rn", row_number().over(window_current_so)) \
    .filter(col("rn") == 1) \
    .filter(col("__op") != "D") \
    .drop("rn")

# Apply L14 fixes
sales_orders_final = sales_orders_current \
    .withColumn("is_freight_inflated", col("source_system") == lit("PARTNER_API")) \
    .withColumn("order_value_gross_adjusted", 
                when(col("source_system") == "PARTNER_API", col("order_value_gross") / 1.085)
                .otherwise(col("order_value_gross"))) \
    .withColumn("order_net_value", col("order_value_gross_adjusted") - col("discount_amount") + col("tax_amount"))

sales_orders_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/FileStore/kestrel/delta/silver/sales_orders") \
    .saveAsTable("kestrel_silver.sales_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification and Summaries

# COMMAND ----------

print(f"Outlet History Count: {spark.table('kestrel_silver.outlet_history').count()}")
print(f"Outlet Current Count: {spark.table('kestrel_silver.outlet_current').count()}")
print(f"Product Current Count: {spark.table('kestrel_silver.product_current').count()}")
print(f"Sales Orders Count: {spark.table('kestrel_silver.sales_orders').count()}")

print("Sample Sales Orders Data:")
display(spark.table('kestrel_silver.sales_orders').limit(5))

# COMMAND ----------
