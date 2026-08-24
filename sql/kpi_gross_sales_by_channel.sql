-- =============================================================================
-- KPI: Gross Sales by Channel
-- =============================================================================
-- Business Definition: Total gross sales (quantity × unit_price) aggregated by
--   sales channel for a given fiscal quarter. Excludes duplicates.
-- Grain: Channel × Fiscal Quarter
-- Source: kestrel_gold.daily_sales (derived from silver POS, deduplicated,
--         event_ts converted from UTC to IST for correct business_date)
-- Owner: Finance
-- Known Limitations:
--   - ~4.2% of SKUs missing UOM conversion; their eaches count uses fallback 1:1
--   - POS only covers Modern Trade and E-Commerce; GT and HORECA go through ERP orders
-- =============================================================================

SELECT
    channel,
    fiscal_year,
    fiscal_quarter,
    ROUND(SUM(gross_sales), 2)          AS total_gross_sales_inr,
    ROUND(SUM(net_sales), 2)            AS total_net_sales_inr,
    ROUND(SUM(total_discount), 2)       AS total_discount_inr,
    ROUND(SUM(total_tax), 2)            AS total_tax_inr,
    SUM(units_sold_eaches)              AS total_units_eaches,
    SUM(basket_count)                   AS total_baskets,
    COUNT(DISTINCT outlet_code)         AS active_outlets,
    COUNT(DISTINCT sku_code)            AS active_skus
FROM kestrel_gold.daily_sales
WHERE fiscal_year = '{fiscal_year}'          -- e.g. 'FY26'
  AND fiscal_quarter = '{fiscal_quarter}'    -- e.g. 'Q4'
GROUP BY channel, fiscal_year, fiscal_quarter
ORDER BY total_gross_sales_inr DESC;
