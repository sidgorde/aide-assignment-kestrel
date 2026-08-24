# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Kestrel Provisions - Setup Notebook
# MAGIC This notebook sets up the necessary databases, installs dependencies, and verifies the raw data for the Kestrel Provisions medallion architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install numpy pandas pyarrow

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup Parameters

# COMMAND ----------

dbutils.widgets.text('data_path', '/FileStore/kestrel/data', 'Root data path')
data_path = dbutils.widgets.get('data_path')
print(f"Using data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS kestrel_bronze;
# MAGIC CREATE DATABASE IF NOT EXISTS kestrel_silver;
# MAGIC CREATE DATABASE IF NOT EXISTS kestrel_gold;

# COMMAND ----------

print("Successfully created/verified databases: kestrel_bronze, kestrel_silver, kestrel_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Raw Data Feeds

# COMMAND ----------

raw_data_path = f"{data_path}/raw"
try:
    feeds = dbutils.fs.ls(raw_data_path)
    print("Raw Data Feeds found:")
    for feed in feeds:
        try:
            files = dbutils.fs.ls(feed.path)
            print(f" - {feed.name}: {len(files)} files/partitions")
        except Exception as e:
            print(f" - {feed.name}: Unable to list contents ({e})")
except Exception as e:
    print(f"Error accessing raw data path {raw_data_path}. Is the data generated/uploaded?")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load and Display Manifest

# COMMAND ----------

manifest_path = f"{data_path}/_manifest/expected_partitions.csv"
try:
    manifest_df = spark.read.csv(manifest_path, header=True, inferSchema=True)
    print("Manifest File: expected_partitions.csv")
    display(manifest_df)
except Exception as e:
    print(f"Failed to load manifest at {manifest_path}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load and Display Reference Data

# COMMAND ----------

reference_path = f"{data_path}/reference"

reference_files = [
    ("uom_conversion", "csv"),
    ("warehouse_master", "csv"),
    ("carrier_master", "csv"),
    ("fiscal_calendar", "csv"),
    ("legacy_finance_weekly_report", "csv")
]

for ref_name, ref_fmt in reference_files:
    file_path = f"{reference_path}/{ref_name}.{ref_fmt}"
    print(f"\nLoading Reference File: {ref_name}.{ref_fmt}")
    try:
        if ref_fmt == "csv":
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        else:
            df = spark.read.parquet(file_path)
        print(f"Row count: {df.count()}")
        display(df)
    except Exception as e:
        print(f"Failed to load {file_path}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=========================================")
print("SETUP COMPLETE")
print("=========================================")
print("- Databases created.")
print("- Dependencies installed.")
print("- Data paths verified.")
print("- Reference files checked.")
print("You are ready to proceed with 01_bronze.py")
print("=========================================")

# COMMAND ----------
