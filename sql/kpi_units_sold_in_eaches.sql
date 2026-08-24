-- =============================================================================
-- KPI: Units Sold in Eaches
-- =============================================================================
-- Business Definition: Total quantity of product sold, converted to individual
--   units ("eaches") regardless of whether sold as cases or eaches at the till.
-- Grain: SKU × Channel × Month
-- Source: kestrel_gold.daily_sales
-- Conversion: For UOM='CS', quantity is multiplied by eaches_per_case from
--   the UOM conversion reference. For UOM='EA', quantity is used as-is.
-- Owner: Supply Chain / Finance
-- Known Limitations:
--   - ~4.2% of SKUs missing from uom_conversion.csv; fallback assumes 1:1
--   - Before 2025-10-01 schema drift, all POS records assumed to be in eaches
-- =============================================================================

SELECT
    sku_code,
    channel,
    SUM(units_sold_eaches)              AS total_units_eaches,
    SUM(units_sold)                     AS total_units_raw,
    ROUND(SUM(gross_sales), 2)          AS total_gross_sales_inr,
    COUNT(DISTINCT business_date)       AS active_days,
    COUNT(DISTINCT outlet_code)         AS outlets_selling
FROM kestrel_gold.daily_sales
WHERE business_date >= '{month}-01'
  AND business_date < ADD_MONTHS('{month}-01', 1)
GROUP BY sku_code, channel
ORDER BY total_units_eaches DESC
LIMIT 100;
