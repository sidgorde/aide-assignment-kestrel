# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Kestrel Provisions - Interactive SQL Query Runner
# MAGIC 
# MAGIC This interactive notebook allows users to run parameterized queries against the Silver and Gold tables in the Kestrel Provisions data lake.
# MAGIC 
# MAGIC ## Available Queries
# MAGIC 1. **gross_sales_by_channel**: Gross sales by channel for the specified fiscal quarter.
# MAGIC 2. **finance_reconciliation**: Compare our numbers vs legacy Finance report.
# MAGIC 3. **units_sold_eaches**: Units sold last month in eaches.
# MAGIC 4. **cold_chain_excursion**: Proportion of chilled trips breaching temperature, by month and warehouse.
# MAGIC 5. **dock_to_dispatch**: Median dock-to-dispatch cycle time by warehouse.
# MAGIC 6. **outlet_channel_changes**: Outlets that changed channel classification during the period.
# MAGIC 7. **order_value_by_source**: Order value by source system and comparability analysis.
# MAGIC 8. **missing_data_detection**: Days with missing data in any feed.
# MAGIC 
# MAGIC ## Parameters
# MAGIC - **fiscal_year**: Fiscal Year (e.g., FY26)
# MAGIC - **fiscal_quarter**: Fiscal Quarter (e.g., Q4)
# MAGIC - **month**: Month in YYYY-MM format (e.g., 2026-06)
# MAGIC - **warehouse**: Warehouse Code (or ALL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup Parameters

# COMMAND ----------

dbutils.widgets.dropdown('query', 'gross_sales_by_channel', 
    ['gross_sales_by_channel', 'finance_reconciliation', 'units_sold_eaches', 
     'cold_chain_excursion', 'dock_to_dispatch', 'outlet_channel_changes',
     'order_value_by_source', 'missing_data_detection'])

dbutils.widgets.text('fiscal_year', 'FY26', 'Fiscal Year')
dbutils.widgets.text('fiscal_quarter', 'Q4', 'Fiscal Quarter')
dbutils.widgets.text('month', '2026-06', 'Month (YYYY-MM)')
dbutils.widgets.text('warehouse', 'ALL', 'Warehouse Code (or ALL)')

selected_query = dbutils.widgets.get('query')
fiscal_year = dbutils.widgets.get('fiscal_year')
fiscal_quarter = dbutils.widgets.get('fiscal_quarter')
month = dbutils.widgets.get('month')
warehouse = dbutils.widgets.get('warehouse')

print(f"Selected Query: {selected_query}")
print(f"Parameters -> Fiscal Year: {fiscal_year}, Fiscal Quarter: {fiscal_quarter}, Month: {month}, Warehouse: {warehouse}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query Dictionary

# COMMAND ----------

queries = {
    'gross_sales_by_channel': """
SELECT channel, fiscal_year, fiscal_quarter,
       SUM(gross_sales) as total_gross_sales,
       SUM(units_sold_eaches) as total_units_eaches,
       SUM(basket_count) as total_baskets,
       COUNT(DISTINCT outlet_code) as active_outlets
FROM kestrel_gold.daily_sales
WHERE fiscal_year = '{fiscal_year}' AND fiscal_quarter = '{fiscal_quarter}'
GROUP BY channel, fiscal_year, fiscal_quarter
ORDER BY total_gross_sales DESC
""",

    'finance_reconciliation': """
SELECT week_ending, channel,
       our_gross_sales, legacy_gross_sales_inr,
       gross_sales_variance, gross_sales_variance_pct,
       our_units_sold, legacy_units_sold,
       variance_explanation
FROM kestrel_gold.finance_reconciliation
ORDER BY week_ending, channel
""",

    'units_sold_eaches': """
SELECT sku_code, channel,
       SUM(units_sold_eaches) as total_eaches,
       SUM(gross_sales) as total_gross_sales
FROM kestrel_gold.daily_sales
WHERE business_date >= '{month}-01' 
  AND business_date < date_add('{month}-01', 31)
GROUP BY sku_code, channel
ORDER BY total_eaches DESC
LIMIT 50
""",

    'cold_chain_excursion': """
SELECT date_format(reading_date, 'yyyy-MM') as month,
       warehouse_code,
       SUM(total_readings) as total_readings,
       SUM(excursion_count) as total_excursions,
       ROUND(SUM(excursion_count) * 100.0 / NULLIF(SUM(total_readings), 0), 2) as excursion_rate_pct
FROM kestrel_gold.cold_chain_daily
GROUP BY date_format(reading_date, 'yyyy-MM'), warehouse_code
ORDER BY month, warehouse_code
""",

    'dock_to_dispatch': """
SELECT w.warehouse_code, wm.warehouse_name, wm.city, wm.region_name,
       COUNT(*) as total_orders,
       ROUND(AVG(w.avg_cycle_time_hours), 2) as avg_cycle_hours,
       ROUND(AVG(w.median_cycle_time_hours), 2) as median_cycle_hours,
       ROUND(AVG(w.p90_cycle_time_hours), 2) as p90_cycle_hours
FROM kestrel_gold.warehouse_cycle_time w
JOIN kestrel_bronze.ref_warehouse_master wm ON w.warehouse_code = wm.warehouse_code
GROUP BY w.warehouse_code, wm.warehouse_name, wm.city, wm.region_name
ORDER BY median_cycle_hours
""",

    'outlet_channel_changes': """
SELECT oh.outlet_code, oh.outlet_name,
       oh.channel as new_channel,
       oh.valid_from,
       LAG(oh.channel) OVER (PARTITION BY oh.outlet_code ORDER BY oh.valid_from) as previous_channel
FROM kestrel_silver.outlet_history oh
WHERE oh.__op = 'U'
QUALIFY LAG(oh.channel) OVER (PARTITION BY oh.outlet_code ORDER BY oh.valid_from) IS NOT NULL
  AND LAG(oh.channel) OVER (PARTITION BY oh.outlet_code ORDER BY oh.valid_from) != oh.channel
ORDER BY oh.valid_from
""",

    'order_value_by_source': """
SELECT source_system,
       COUNT(*) as order_count,
       ROUND(SUM(order_value_gross), 2) as total_gross_raw,
       ROUND(SUM(order_value_gross_adjusted), 2) as total_gross_adjusted,
       ROUND(AVG(order_value_gross), 2) as avg_gross_raw,
       ROUND(AVG(order_value_gross_adjusted), 2) as avg_gross_adjusted,
       ROUND((SUM(order_value_gross) - SUM(order_value_gross_adjusted)) / NULLIF(SUM(order_value_gross_adjusted), 0) * 100, 2) as inflation_pct,
       SUM(CASE WHEN is_freight_inflated THEN 1 ELSE 0 END) as freight_inflated_count
FROM kestrel_silver.sales_orders
GROUP BY source_system
ORDER BY total_gross_adjusted DESC
""",

    'missing_data_detection': """
WITH date_range AS (
  SELECT calendar_date FROM kestrel_bronze.ref_fiscal_calendar
),
pos_daily AS (
  SELECT business_date as dt, COUNT(*) as cnt FROM kestrel_silver.pos_transactions GROUP BY business_date
),
reefer_daily AS (
  SELECT reading_date as dt, COUNT(*) as cnt FROM kestrel_silver.reefer_telemetry GROUP BY reading_date
),
wms_daily AS (
  SELECT event_date as dt, COUNT(*) as cnt FROM kestrel_silver.wms_scan_events GROUP BY event_date
)
SELECT d.calendar_date,
       COALESCE(p.cnt, 0) as pos_rows,
       COALESCE(r.cnt, 0) as reefer_rows,
       COALESCE(w.cnt, 0) as wms_rows,
       CASE WHEN COALESCE(p.cnt, 0) = 0 THEN 'MISSING' 
            WHEN COALESCE(p.cnt, 0) < 500 THEN 'LOW' ELSE 'OK' END as pos_status,
       CASE WHEN COALESCE(r.cnt, 0) = 0 THEN 'MISSING'
            WHEN COALESCE(r.cnt, 0) < 500 THEN 'LOW' ELSE 'OK' END as reefer_status,
       CASE WHEN COALESCE(w.cnt, 0) = 0 THEN 'MISSING'
            WHEN COALESCE(w.cnt, 0) < 500 THEN 'LOW' ELSE 'OK' END as wms_status
FROM date_range d
LEFT JOIN pos_daily p ON d.calendar_date = p.dt
LEFT JOIN reefer_daily r ON d.calendar_date = r.dt
LEFT JOIN wms_daily w ON d.calendar_date = w.dt
WHERE COALESCE(p.cnt, 0) = 0 OR COALESCE(r.cnt, 0) = 0 OR COALESCE(w.cnt, 0) = 0
   OR COALESCE(p.cnt, 0) < 500 OR COALESCE(r.cnt, 0) < 500 OR COALESCE(w.cnt, 0) < 500
ORDER BY d.calendar_date
"""
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query Execution

# COMMAND ----------

# Get the raw SQL string
raw_sql = queries.get(selected_query)

if not raw_sql:
    print(f"Error: Query '{selected_query}' not found.")
else:
    # Substitute parameters using Python string formatting
    executable_sql = raw_sql.format(
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        month=month,
        warehouse=warehouse
    )
    
    print("-" * 80)
    print(f"EXECUTING QUERY: {selected_query}")
    print("-" * 80)
    print(executable_sql.strip())
    print("-" * 80)
    
    # Execute and display the query
    df_result = spark.sql(executable_sql)
    display(df_result)

# COMMAND ----------
