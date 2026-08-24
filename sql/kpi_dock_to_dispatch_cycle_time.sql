-- =============================================================================
-- KPI: Dock-to-Dispatch Cycle Time
-- =============================================================================
-- Business Definition: Time elapsed from the first RECEIVE scan to the last
--   DISPATCH scan for an order at a warehouse. Measures warehouse throughput.
-- Grain: Warehouse × Date (daily aggregate)
-- Source: kestrel_gold.warehouse_cycle_time (from silver WMS order cycle time)
-- Only includes COMPLETE journeys (orders with both RECEIVE and DISPATCH scans)
-- Owner: Supply Chain Operations
-- Known Limitations:
--   - ~6.5% of scan events were never emitted by handhelds (signal loss in
--     chilled chambers); cycle times are therefore indicative
--   - Orders missing either RECEIVE or DISPATCH are excluded
-- =============================================================================

SELECT
    wct.warehouse_code,
    wm.warehouse_name,
    wm.city,
    wm.region_name,
    SUM(wct.order_count)                            AS total_orders,
    ROUND(AVG(wct.avg_cycle_time_hours), 2)         AS avg_cycle_time_hours,
    ROUND(AVG(wct.median_cycle_time_hours), 2)      AS median_cycle_time_hours,
    ROUND(AVG(wct.p90_cycle_time_hours), 2)         AS p90_cycle_time_hours,
    ROUND(MIN(wct.min_cycle_time_hours), 2)         AS overall_min_hours,
    ROUND(MAX(wct.max_cycle_time_hours), 2)         AS overall_max_hours
FROM kestrel_gold.warehouse_cycle_time wct
JOIN kestrel_bronze.ref_warehouse_master wm
    ON wct.warehouse_code = wm.warehouse_code
GROUP BY
    wct.warehouse_code,
    wm.warehouse_name,
    wm.city,
    wm.region_name
ORDER BY median_cycle_time_hours;
