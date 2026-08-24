# Data Quality Findings — Kestrel Provisions

## Summary

18 data quality issues were identified across the four raw feeds and reference data. All are handled in the pipeline, documented in the KPI catalogue, and verified in the data quality notebook.

---

## POS Transactions

### 1. Duplicate Rows (~2.1%)
**Issue:** The partner collector uses at-least-once delivery, producing exact duplicate POS rows.  
**Impact:** Inflates sales figures by ~2.1%.  
**Resolution:** Deduplicate on `(txn_id, txn_line_no)` in the Silver layer.  
**Reference:** Ticket KP-3104.

### 2. Event Timestamp in UTC
**Issue:** `event_ts` is emitted in UTC, but the business day is Asia/Kolkata (UTC+5:30). Transactions occurring between midnight and 5:30 AM IST are attributed to the previous calendar day if not converted.  
**Impact:** ~4.5% of transactions appear on the wrong business date.  
**Resolution:** Convert UTC → IST using `from_utc_timestamp()` before deriving `business_date`.

### 3. Late Arrivals (~4.5%)
**Issue:** ~4.5% of rows land 1–3 days after the actual event date. `ingest_date ≠ business_date`.  
**Impact:** The legacy Finance report, which groups by `ingest_date`, attributes these to the wrong week.  
**Resolution:** Use `business_date` (from IST-converted `event_ts`) for all reporting. `ingest_date` is retained for audit.

### 4. Schema Drift at 2025-10-01
**Issue:** The partner upgraded their platform in Q4 2025. Before 2025-10-01: column is `qty`, no `uom`, no `loyalty_id`. After: `qty` renamed to `quantity_units`, `uom` and `loyalty_id` columns added.  
**Impact:** A naive read fails with schema mismatch. Partitions on either side of the boundary do not share a schema.  
**Resolution:** Read with `mergeSchema=true`; unify via `COALESCE(qty, quantity_units)` as `quantity`; default `uom` to `'EA'` for pre-drift records.  
**Reference:** Ticket KP-3119.

---

## Reefer Telemetry

### 5. Firmware Clock Offset (+7 hours)
**Issue:** Devices with firmware version 2.1.4 have a +7 hour device clock offset.  
**Impact:** Readings are attributed to the wrong time window; daily aggregations are skewed.  
**Resolution:** Subtract 7 hours from `reading_ts` where `firmware_version = '2.1.4'`. Flag with `clock_corrected = true`.

### 6. COLDEYE Vendor Reports Fahrenheit
**Issue:** COLDEYE devices report temperature in Fahrenheit; THERMLOG reports in Celsius. Both populate `temp_unit`, but the business threshold (2–8°C) assumes Celsius.  
**Impact:** Without conversion, COLDEYE readings (typically 35–45°F = 2–7°C) appear as "extreme excursions" when compared against the 2–8 Celsius threshold. This is likely why the business reports a ~33% excursion rate — it's a unit mismatch, not a cold chain failure.  
**Resolution:** Convert all readings to Celsius: `(temp_value - 32) × 5/9` for Fahrenheit readings.  
**Reference:** Ticket KP-3140.

### 7. Null Temperature Unit (~8%)
**Issue:** `temp_unit` is null on ~8% of readings.  
**Impact:** Cannot determine if the value is Celsius or Fahrenheit.  
**Resolution:** Infer from `telemetry_vendor`: COLDEYE → Fahrenheit, THERMLOG → Celsius. Column `temp_unit_resolved` captures the inferred value.

### 8. Sensor Dropouts (~0.6%)
**Issue:** ~0.6% of readings have null `temp_value` due to sensor malfunction.  
**Impact:** Missing data points in temperature analysis.  
**Resolution:** Flag with `is_sensor_dropout = true`; exclude from excursion rate denominator.

### 9. Duplicate Readings (~3.2%)
**Issue:** At-least-once delivery produces exact duplicate telemetry readings.  
**Resolution:** Remove exact duplicates using `dropDuplicates()` on all business columns.

### 10. Gateway Outage (GW-017)
**Issue:** Gateway GW-017 had an outage on 2026-02-11 and 2026-02-12. No data was received from vehicles routing through this gateway on those days.  
**Impact:** Data is simply missing — not flagged, not nulled. Looks like a low-volume day.  
**Resolution:** Detected in the missing data analysis. Documented. Cannot be recovered without re-transmission.  
**Reference:** Ticket KP-3172.

### 11. Truncated Parquet File
**Issue:** The file `dt=2025-07-14/part-00000.parquet` is truncated (only 72% of original bytes). Reading it fails with a Parquet footer error.  
**Impact:** Some data for 2025-07-14 is lost.  
**Resolution:** Bronze ingestion reads partitions individually with try/except; logs and skips corrupted files. The manifest shows the expected row count for reconciliation.

---

## WMS Scan Events

### 12. Missing Scan Events (~6.5%)
**Issue:** Handheld scanners buffer events when they lose signal in chilled chambers and occasionally drop the buffer entirely. ~6.5% of expected scan events never arrive.  
**Impact:** Cycle time calculations are incomplete. Some orders are missing one or more stage scans.  
**Resolution:** Flag orders with incomplete stage journeys (`is_complete_journey`). Only use complete journeys for cycle time KPI. Document that cycle times are indicative.

---

## ERP CDC

### 13. Late-Landing CDC Extracts (~1.6%)
**Issue:** ~1.6% of CDC records land in a later extract file than their `__op_ts` timestamp would suggest.  
**Impact:** If you take "latest record from the most recent file" (by `extract_date`), you get the wrong state.  
**Resolution:** Order by `(__op_ts, __seq)` instead of `extract_date`. The `__seq` column is monotonically increasing and never lies.

### 14. Duplicate Timestamps (Outlet Master)
**Issue:** ~1% of outlet CDC records share the same `__op_ts` value.  
**Impact:** If you break ties by `__op_ts` alone, you may pick the wrong version.  
**Resolution:** Use `__seq` as the tiebreaker (higher `__seq` = later record).

### 15. PARTNER_API Freight Inflation (~8.5%)
**Issue:** The PARTNER_API source system double-counts freight into `order_value_gross`, inflating it by ~8.5%.  
**Impact:** Order values from PARTNER_API are not comparable to SFA_MOBILE or ERP_WEB.  
**Resolution:** Add `order_value_gross_adjusted = order_value_gross / 1.085` for PARTNER_API. Flag with `is_freight_inflated = true`.  
**Reference:** Open Finance ticket from 2025.

### 16. Hard-Deleted Orders (~0.9%)
**Issue:** ~0.9% of orders are hard-deleted via `__op = 'D'` tombstone records.  
**Impact:** Including deleted orders inflates order counts and values.  
**Resolution:** Apply full CDC logic; exclude orders whose latest operation is a delete.

---

## Reference Data

### 17. Missing UOM Conversions (~4.2%)
**Issue:** The `uom_conversion.csv` reference table is missing ~4.2% of SKU codes.  
**Impact:** Cannot convert case quantities to eaches for those SKUs.  
**Resolution:** Flag with `uom_conversion_missing = true`; default `eaches_per_case = 1` as fallback. Downstream consumers should investigate flagged rows.

### 18. Legacy Finance Report — Two Bugs
**Issue:** The legacy weekly Finance report has two systematic errors:
1. Groups by `ingest_date` (when data landed) instead of `event_date` (when the sale happened)
2. Does not deduplicate the POS feed (which has ~2.1% duplicate rows)

**Impact:** The report the board sees is systematically wrong. Reconciling our pipeline exactly to it would require reproducing these bugs.  
**Resolution:** Our pipeline uses `event_date` and deduplicates. The `finance_reconciliation` gold table shows our numbers alongside the legacy report with variance explanations.
