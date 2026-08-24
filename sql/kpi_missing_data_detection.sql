-- =============================================================================
-- KPI: Missing Data Detection
-- =============================================================================
-- Business Definition: Identifies days where one or more data feeds have zero
--   or abnormally low row counts, indicating potential data gaps, gateway
--   outages, or ingestion failures.
-- Grain: Calendar date × Feed
-- Source: All silver layer tables + fiscal calendar
-- Owner: Data Engineering / Platform
-- Known Issues Detected:
--   - Gateway GW-017 outage on 2026-02-11 and 2026-02-12 (reefer telemetry)
--   - Truncated parquet file on 2025-07-14 (reefer telemetry)
--   - Manifest says no data was missing, but the outage is invisible to the
--     ingestion job
-- =============================================================================

WITH date_range AS (
    SELECT calendar_date
    FROM kestrel_bronze.ref_fiscal_calendar
),
pos_daily AS (
    SELECT business_date AS dt, COUNT(*) AS row_count
    FROM kestrel_silver.pos_transactions
    GROUP BY business_date
),
reefer_daily AS (
    SELECT reading_date AS dt, COUNT(*) AS row_count
    FROM kestrel_silver.reefer_telemetry
    GROUP BY reading_date
),
wms_daily AS (
    SELECT event_date AS dt, COUNT(*) AS row_count
    FROM kestrel_silver.wms_scan_events
    GROUP BY event_date
),
daily_averages AS (
    SELECT
        (SELECT AVG(row_count) FROM pos_daily)    AS avg_pos,
        (SELECT AVG(row_count) FROM reefer_daily) AS avg_reefer,
        (SELECT AVG(row_count) FROM wms_daily)    AS avg_wms
)
SELECT
    d.calendar_date,
    COALESCE(p.row_count, 0)    AS pos_rows,
    COALESCE(r.row_count, 0)    AS reefer_rows,
    COALESCE(w.row_count, 0)    AS wms_rows,
    CASE
        WHEN COALESCE(p.row_count, 0) = 0                       THEN 'MISSING'
        WHEN COALESCE(p.row_count, 0) < da.avg_pos * 0.3        THEN 'VERY_LOW'
        WHEN COALESCE(p.row_count, 0) < da.avg_pos * 0.5        THEN 'LOW'
        ELSE 'OK'
    END AS pos_status,
    CASE
        WHEN COALESCE(r.row_count, 0) = 0                       THEN 'MISSING'
        WHEN COALESCE(r.row_count, 0) < da.avg_reefer * 0.3     THEN 'VERY_LOW'
        WHEN COALESCE(r.row_count, 0) < da.avg_reefer * 0.5     THEN 'LOW'
        ELSE 'OK'
    END AS reefer_status,
    CASE
        WHEN COALESCE(w.row_count, 0) = 0                       THEN 'MISSING'
        WHEN COALESCE(w.row_count, 0) < da.avg_wms * 0.3        THEN 'VERY_LOW'
        WHEN COALESCE(w.row_count, 0) < da.avg_wms * 0.5        THEN 'LOW'
        ELSE 'OK'
    END AS wms_status
FROM date_range d
CROSS JOIN daily_averages da
LEFT JOIN pos_daily p    ON d.calendar_date = p.dt
LEFT JOIN reefer_daily r ON d.calendar_date = r.dt
LEFT JOIN wms_daily w    ON d.calendar_date = w.dt
WHERE COALESCE(p.row_count, 0) = 0
   OR COALESCE(r.row_count, 0) = 0
   OR COALESCE(w.row_count, 0) = 0
   OR COALESCE(p.row_count, 0) < da.avg_pos * 0.5
   OR COALESCE(r.row_count, 0) < da.avg_reefer * 0.5
   OR COALESCE(w.row_count, 0) < da.avg_wms * 0.5
ORDER BY d.calendar_date;
