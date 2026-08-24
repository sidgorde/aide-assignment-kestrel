# SQL Query Library

Parameterised SQL queries for the Kestrel Provisions KPI layer. Each query maps to a defined KPI in the [KPI Catalogue](../kpi_catalogue/kpi_catalogue.md).

## Queries

| # | File | KPI | Parameters |
|---|------|-----|------------|
| 1 | `kpi_gross_sales_by_channel.sql` | Gross Sales by Channel | `{fiscal_year}`, `{fiscal_quarter}` |
| 2 | `kpi_finance_reconciliation.sql` | Finance Report Reconciliation | None |
| 3 | `kpi_units_sold_in_eaches.sql` | Units Sold (Eaches) | `{month}` (YYYY-MM) |
| 4 | `kpi_cold_chain_excursion_rate.sql` | Cold Chain Excursion Rate | None |
| 5 | `kpi_dock_to_dispatch_cycle_time.sql` | Dock-to-Dispatch Cycle Time | None |
| 6 | `kpi_outlet_channel_changes.sql` | Outlet Channel Changes | None |
| 7 | `kpi_order_value_by_source.sql` | Order Value by Source System | None |
| 8 | `kpi_missing_data_detection.sql` | Missing Data Detection | None |

## Usage

### In Databricks
Use the interactive query runner notebook: `notebooks/08_query_runner.py`

### Standalone
Replace `{parameter}` placeholders with actual values:
```sql
-- Example: Gross sales for FY26 Q4
SELECT ... WHERE fiscal_year = 'FY26' AND fiscal_quarter = 'Q4'
```

## Data Sources

All queries run against the **Gold** or **Silver** layer tables, which have been cleaned, deduplicated, timezone-corrected, and normalised.

| Layer | Database | Description |
|-------|----------|-------------|
| Gold | `kestrel_gold` | Pre-aggregated business metrics |
| Silver | `kestrel_silver` | Cleaned, conformed transaction-level data |
| Bronze | `kestrel_bronze` | Raw ingest + reference tables |
