# DECISIONS

## What I Built

A medallion-architecture data pipeline (Bronze → Silver → Gold) on PySpark/Databricks that ingests four raw feeds, cleans them, and produces a queryable metric layer with nine defined KPIs.

**Bronze:** Raw Parquet → Delta Lake with schema merge and corrupt-file handling.  
**Silver:** Deduplication, timezone correction, temperature unit normalisation, CDC resolution, schema harmonisation, SCD Type-2 history.  
**Gold:** Pre-aggregated business metrics (daily sales, cold chain, cycle time, finance reconciliation, data quality).

Plus: a KPI catalogue (one definition per metric), a parameterised SQL query library, and an interactive query runner.

## What I Deliberately Did Not Build

- **A dashboard.** The CFO explicitly said "not a dashboard." The value is in agreed definitions and traceable numbers, not visualisation.
- **Real-time streaming.** The feeds are batch (nightly). Building streaming infra would be gold-plating a problem that doesn't exist yet.
- **An ML-powered NL-to-SQL interface.** The brief mentions "ask-anything." I built a parameterised query runner that shows its SQL. A proper NL layer needs an LLM, guardrails, and testing — a two-week project, not a two-day one.
- **Automated alerting on data quality.** I detect and report issues; I don't page anyone. That requires integration with on-call tooling.
- **dbt.** PySpark notebooks are more natural on Databricks Community Edition and sufficient at this scale. At production scale I'd use dbt-spark for the SQL-based transformations.

## Key Assumptions

1. **The Finance report is wrong, not the pipeline.** The legacy report groups by ingest date and includes duplicates. Reconciling exactly to it would mean reproducing two bugs. I reconcile *against* it and explain the variance.
2. **All POS records before Oct 2025 are in eaches.** The `uom` column doesn't exist before the schema drift. The generator confirms this.
3. **PARTNER_API freight inflation is 8.5%.** Derived from the generator (`* 1.085`). In production this multiplier would need to be validated against invoicing.
4. **Chilled excursion band is 2–8°C inclusive.** The feed contract says "above the band" for excursions, but physically readings below 2°C (freezing risk) are also problematic. I flag both directions.
5. **Missing SKUs in UOM conversion default to 1:1.** Flagged with `uom_conversion_missing = true` so downstream consumers can filter or investigate.
6. **All warehouse timestamps are Asia/Kolkata.** The warehouse master confirms all 8 sites are IST. POS `event_ts` is UTC (confirmed by the generator); I convert to IST.

## What I Would Do With Two More Weeks

1. **Point-in-time joins.** Use `outlet_history` SCD for historical POS analysis, joining on the outlet's channel *at the time of the transaction*, not today's channel.
2. **Incremental pipeline.** Currently runs full-refresh. Delta Lake supports `MERGE INTO` for incremental loads — critical for daily operations.
3. **Data contracts with schema validation.** Enforce expected schemas on ingest using Great Expectations or Delta Live Tables expectations.
4. **Service Level KPI.** Join WMS dispatch to sales orders to carrier SLA to measure on-time delivery.
5. **Automated reconciliation CI.** Run manifest vs actual counts as a nightly check; alert on drift.
6. **NL-to-SQL layer.** Wire up an LLM (e.g., Databricks AI Functions) against the gold tables, with the KPI catalogue as system context.

## What Breaks First in Production, and at What Volume

| What breaks | When | Why |
|------------|------|-----|
| Single-node Spark | ~50× scale (~500M rows) | Community Edition has no workers. Move to a proper cluster. |
| Full-refresh Silver | ~10× scale daily | Re-processing all history daily is wasteful. Switch to incremental MERGE. |
| Parquet glob reads in Bronze | ~100× scale | Reading thousands of small Parquet files is slow. Switch to Auto Loader or Delta Live Tables. |
| CDC window function | ~1000× CDC records | The `ROW_NUMBER()` over all history per key gets expensive. Move to incremental CDC with watermarks. |
| Gold aggregations | ~100× scale | Pre-aggregation is fine, but the daily_sales table at SKU×outlet grain becomes very wide. Consider OLAP cubes or materialized views. |

The architecture *shape* (medallion, Delta, defined metrics) scales. The *implementation* (full-refresh, single-node, glob reads) does not. That's the right trade-off for a take-home: prove the design, acknowledge the limits.
