# KPI Catalogue — Kestrel Provisions

**Version:** 1.0 | **Last Updated:** August 2026  
**Purpose:** Single source of truth for all business metric definitions.  
**Audience:** Finance, Supply Chain, Data Engineering, Leadership

> Every metric has one definition, one formula, and one owner.  
> If it's not in this catalogue, it's not a KPI.

---

## 1. Gross Sales

| Attribute | Value |
|-----------|-------|
| **Business Name** | Gross Sales |
| **Definition** | Total revenue before discounts and taxes. Calculated as quantity sold × unit price at the point of sale. |
| **Formula** | `SUM(quantity × unit_price)` |
| **Unit** | INR |
| **Grain** | Daily × Channel × Outlet × SKU |
| **Source Feeds** | `pos_transactions` (deduplicated, event_ts converted UTC → IST) |
| **Joins** | Fiscal calendar on `business_date` |
| **Filters** | Duplicate POS rows removed (2.1% at-least-once delivery). Business date derived from IST timestamp, NOT ingest date. |
| **Exclusions** | None |
| **Owner** | Finance |
| **Gold Table** | `kestrel_gold.daily_sales.gross_sales` |
| **SQL Query** | `sql/kpi_gross_sales_by_channel.sql` |
| **Known Limitations** | POS covers Modern Trade and E-Commerce channels only. General Trade and HORECA orders flow through the ERP, not POS. |

---

## 2. Net Sales

| Attribute | Value |
|-----------|-------|
| **Business Name** | Net Sales |
| **Definition** | Revenue after discounts, before tax. |
| **Formula** | `SUM(quantity × unit_price - discount_amount)` |
| **Unit** | INR |
| **Grain** | Daily × Channel × Outlet × SKU |
| **Source Feeds** | `pos_transactions` |
| **Filters** | Same as Gross Sales |
| **Owner** | Finance |
| **Gold Table** | `kestrel_gold.daily_sales.net_sales` |
| **Known Limitations** | Same as Gross Sales |

---

## 3. Units Sold (Eaches)

| Attribute | Value |
|-----------|-------|
| **Business Name** | Units Sold in Eaches |
| **Definition** | Total quantity of product sold, converted to individual units ("eaches"). If sold in cases, multiplied by the case-pack size. |
| **Formula** | `SUM(CASE WHEN uom = 'CS' THEN quantity × eaches_per_case ELSE quantity END)` |
| **Unit** | Eaches (individual units) |
| **Grain** | Daily × Channel × Outlet × SKU |
| **Source Feeds** | `pos_transactions` + `reference/uom_conversion.csv` |
| **Joins** | UOM conversion on `sku_code` |
| **Filters** | Deduplicated. Schema harmonised (pre-Oct 2025 `qty` → post-Oct 2025 `quantity_units` unified). |
| **Owner** | Finance / Supply Chain |
| **Gold Table** | `kestrel_gold.daily_sales.units_sold_eaches` |
| **SQL Query** | `sql/kpi_units_sold_in_eaches.sql` |
| **Known Limitations** | ~4.2% of SKUs are missing from `uom_conversion.csv`. For these SKUs, `eaches_per_case` defaults to 1 (assumes already in eaches). Flag column `uom_conversion_missing` identifies affected rows. Before the Oct 2025 schema change, the `uom` column did not exist; all pre-drift records are assumed to be in eaches. |

---

## 4. Basket Count

| Attribute | Value |
|-----------|-------|
| **Business Name** | Basket Count |
| **Definition** | Number of unique shopping baskets (transactions) at the till. |
| **Formula** | `COUNT(DISTINCT basket_id)` |
| **Unit** | Count |
| **Grain** | Daily × Channel × Outlet |
| **Source Feeds** | `pos_transactions` |
| **Owner** | Finance |
| **Gold Table** | `kestrel_gold.daily_sales.basket_count` |
| **Known Limitations** | Same deduplication and date logic as Gross Sales |

---

## 5. Cold Chain Excursion Rate

| Attribute | Value |
|-----------|-------|
| **Business Name** | Cold Chain Excursion Rate |
| **Definition** | Proportion of temperature readings from refrigerated vehicles that fall outside the target band of 2–8°C. An excursion is any reading above 8°C or below 2°C. |
| **Formula** | `COUNT(readings WHERE temp_celsius > 8 OR temp_celsius < 2) / COUNT(all valid readings) × 100` |
| **Unit** | Percentage (%) |
| **Grain** | Daily × Warehouse × Vendor |
| **Source Feeds** | `reefer_telemetry` |
| **Joins** | Warehouse master, fiscal calendar |
| **Filters** | Sensor dropouts (null `temp_value`, ~0.6%) excluded from denominator. Duplicates removed (~3.2%). |
| **Owner** | Supply Chain Operations |
| **Gold Table** | `kestrel_gold.cold_chain_daily.excursion_rate` |
| **SQL Query** | `sql/kpi_cold_chain_excursion_rate.sql` |
| **Known Limitations** | (1) COLDEYE vendor reported Fahrenheit, converted to Celsius in silver layer. The business previously reported ~33% excursion rate because raw Fahrenheit values were compared against Celsius thresholds — after conversion, the true rate is significantly lower. (2) ~8% of readings had null `temp_unit`; inferred from vendor. (3) Firmware 2.1.4 devices had a +7h clock offset, corrected in silver layer. (4) Gateway GW-017 had an outage on 2026-02-11/12 — data is simply missing, not flagged. |

---

## 6. Dock-to-Dispatch Cycle Time

| Attribute | Value |
|-----------|-------|
| **Business Name** | Dock-to-Dispatch Cycle Time |
| **Definition** | Elapsed time from the first RECEIVE scan to the last DISPATCH scan for an order at a warehouse. Measures how quickly goods move through the DC. |
| **Formula** | `DISPATCH_timestamp - RECEIVE_timestamp` (in hours) |
| **Unit** | Hours |
| **Grain** | Daily × Warehouse (aggregated as median, mean, P90) |
| **Source Feeds** | `wms_scan_events` |
| **Joins** | Warehouse master |
| **Filters** | Only complete journeys (orders with both RECEIVE and DISPATCH scans). Excludes incomplete orders. |
| **Owner** | Supply Chain Operations |
| **Gold Table** | `kestrel_gold.warehouse_cycle_time` |
| **SQL Query** | `sql/kpi_dock_to_dispatch_cycle_time.sql` |
| **Known Limitations** | ~6.5% of WMS scan events were never emitted (handhelds lose signal in chilled chambers and occasionally drop the buffer). Cycle times are therefore indicative, not exact. Orders missing either RECEIVE or DISPATCH scans are excluded entirely. |

---

## 7. Finance Report Reconciliation

| Attribute | Value |
|-----------|-------|
| **Business Name** | Finance Report Reconciliation |
| **Definition** | Comparison of our pipeline's weekly sales figures against the legacy Finance weekly report published to the board. Quantifies and explains variances. |
| **Formula** | `our_gross_sales - legacy_gross_sales_inr` (absolute and %) |
| **Unit** | INR (variance), % (variance %) |
| **Grain** | Week ending (Sunday) × Channel |
| **Source Feeds** | `kestrel_gold.weekly_sales_by_channel` + `reference/legacy_finance_weekly_report.csv` |
| **Owner** | Finance / CFO |
| **Gold Table** | `kestrel_gold.finance_reconciliation` |
| **SQL Query** | `sql/kpi_finance_reconciliation.sql` |
| **Known Limitations** | The legacy report is wrong in two specific ways: (1) It groups by `ingest_date` (when data landed in the system) rather than `event_date` (when the sale actually happened). This means ~4.5% of late-arriving transactions are attributed to the wrong week. (2) It does not deduplicate the POS feed, which has ~2.1% duplicate rows from at-least-once delivery. Our pipeline corrects both issues. Reconciling exactly to the legacy report would require reproducing these bugs. |

---

## 8. Order Value by Source System

| Attribute | Value |
|-----------|-------|
| **Business Name** | Order Value by Source System |
| **Definition** | Total and average order values segmented by the three ERP source systems, with analysis of whether the sources are comparable. |
| **Formula** | `SUM(order_value_gross)` and `SUM(order_value_gross_adjusted)` per source |
| **Unit** | INR |
| **Grain** | Month × Source System |
| **Source Feeds** | `erp_cdc/sales_order_header` |
| **Owner** | Finance |
| **Gold Table** | `kestrel_gold.order_value_by_source` |
| **SQL Query** | `sql/kpi_order_value_by_source.sql` |
| **Known Limitations** | `PARTNER_API` source inflates `order_value_gross` by ~8.5% because freight is double-counted into the order value. The pipeline provides an `order_value_gross_adjusted` column that divides PARTNER_API values by 1.085. The three sources are NOT directly comparable without this adjustment. An open ticket from 2025 exists for this issue but was never resolved. |

---

## 9. Missing Data Detection

| Attribute | Value |
|-----------|-------|
| **Business Name** | Data Completeness Check |
| **Definition** | Identifies calendar dates where any feed has zero rows or abnormally low row counts, indicating potential gateway outages, ingestion failures, or data loss. |
| **Formula** | Row count per feed per date compared to the feed's daily average |
| **Unit** | Status flag (MISSING / VERY_LOW / LOW / OK) |
| **Grain** | Calendar date × Feed |
| **Source Feeds** | All silver tables + fiscal calendar |
| **Owner** | Data Engineering |
| **Gold Table** | `kestrel_gold.data_quality_report` |
| **SQL Query** | `sql/kpi_missing_data_detection.sql` |
| **Known Limitations** | Gateway outages are invisible to the ingestion job — missing data looks like low volume, not an error (ticket KP-3172). The manifest records what the ingestion job thinks it wrote, not what actually arrived. |

---

## Glossary

| Term | Definition |
|------|-----------|
| **Eaches** | Individual product units (as opposed to cases) |
| **Case Pack** | Number of eaches in one case (from `uom_conversion.csv`) |
| **Excursion** | Temperature reading outside the 2–8°C chilled band |
| **Business Date** | Date of the sale event in IST (Asia/Kolkata), derived from UTC `event_ts` |
| **Ingest Date** | Date the data file landed in the system (may differ from business date) |
| **SCD Type-2** | Slowly Changing Dimension Type 2 — tracks historical changes with valid_from/valid_to |
| **CDC** | Change Data Capture — incremental extract from ERP with I/U/D operations |
| **Fiscal Year** | April to March (e.g., FY26 = April 2025 – March 2026) |
