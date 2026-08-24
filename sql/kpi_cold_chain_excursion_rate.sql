-- =============================================================================
-- KPI: Cold Chain Excursion Rate
-- =============================================================================
-- Business Definition: Proportion of temperature readings from chilled vehicles
--   that fall outside the target band of 2–8°C. An excursion is any reading
--   above 8°C or below 2°C.
-- Grain: Month × Warehouse
-- Source: kestrel_gold.cold_chain_daily (derived from silver reefer telemetry)
-- Data Quality Notes:
--   - COLDEYE vendor reports Fahrenheit; converted to Celsius in silver layer
--   - Firmware 2.1.4 had a +7h clock offset; corrected in silver layer
--   - ~8% of readings had null temp_unit; inferred from vendor
--   - ~0.6% sensor dropouts (null temp_value) excluded from rate calculation
--   - The business previously reported ~33% excursion rate; this was because
--     Fahrenheit readings were compared against Celsius thresholds without
--     conversion. After correction, the true rate is significantly lower.
-- Owner: Supply Chain Operations
-- =============================================================================

SELECT
    DATE_FORMAT(reading_date, 'yyyy-MM')    AS month,
    cd.warehouse_code,
    wm.warehouse_name,
    wm.region_name,
    SUM(cd.total_readings)                  AS total_readings,
    SUM(cd.excursion_count)                 AS total_excursions,
    ROUND(
        SUM(cd.excursion_count) * 100.0
        / NULLIF(SUM(cd.total_readings), 0),
        2
    )                                       AS excursion_rate_pct,
    ROUND(AVG(cd.avg_temp_celsius), 2)      AS avg_temp_celsius,
    MIN(cd.min_temp_celsius)                AS overall_min_temp,
    MAX(cd.max_temp_celsius)                AS overall_max_temp,
    SUM(cd.sensor_dropout_count)            AS total_sensor_dropouts,
    SUM(cd.door_open_count)                 AS total_door_opens
FROM kestrel_gold.cold_chain_daily cd
JOIN kestrel_bronze.ref_warehouse_master wm
    ON cd.warehouse_code = wm.warehouse_code
GROUP BY
    DATE_FORMAT(reading_date, 'yyyy-MM'),
    cd.warehouse_code,
    wm.warehouse_name,
    wm.region_name
ORDER BY month, cd.warehouse_code;
