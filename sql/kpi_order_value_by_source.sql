-- =============================================================================
-- KPI: Order Value by Source System
-- =============================================================================
-- Business Definition: Compares order values across the three ERP source systems
--   (SFA_MOBILE, ERP_WEB, PARTNER_API) to assess comparability and identify
--   discrepancies.
-- Key Finding: PARTNER_API orders inflate order_value_gross by ~8.5% due to
--   freight being double-counted into the order value.
-- Grain: Source System × Month
-- Source: kestrel_silver.sales_orders
-- Owner: Finance
-- Known Limitation:
--   - Ticket open since 2025 that PARTNER_API values don't match invoiced amounts
--   - Pipeline adds adjusted values (dividing PARTNER_API by 1.085) but the root
--     cause should be fixed at the source
-- =============================================================================

SELECT
    source_system,
    DATE_FORMAT(order_date, 'yyyy-MM')          AS month,
    COUNT(*)                                    AS order_count,
    ROUND(SUM(order_value_gross), 2)            AS total_gross_raw_inr,
    ROUND(SUM(order_value_gross_adjusted), 2)   AS total_gross_adjusted_inr,
    ROUND(AVG(order_value_gross), 2)            AS avg_gross_raw_inr,
    ROUND(AVG(order_value_gross_adjusted), 2)   AS avg_gross_adjusted_inr,
    ROUND(
        (SUM(order_value_gross) - SUM(order_value_gross_adjusted))
        / NULLIF(SUM(order_value_gross_adjusted), 0) * 100,
        2
    )                                           AS freight_inflation_pct,
    SUM(CASE WHEN is_freight_inflated THEN 1 ELSE 0 END) AS inflated_order_count
FROM kestrel_silver.sales_orders
GROUP BY source_system, DATE_FORMAT(order_date, 'yyyy-MM')
ORDER BY source_system, month;
