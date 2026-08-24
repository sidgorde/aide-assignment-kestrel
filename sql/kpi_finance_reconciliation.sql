-- =============================================================================
-- KPI: Finance Reconciliation
-- =============================================================================
-- Business Definition: Compares our pipeline's gross sales figures against the
--   legacy Finance weekly report published to the board.
-- Purpose: Prove (or explain) discrepancies between the two sets of numbers.
-- Grain: Week ending (Sunday) × Channel
-- Source: kestrel_gold.finance_reconciliation
-- Owner: Finance / CFO
-- Key Finding: The legacy report groups by ingest_date (when data landed) rather
--   than event_date (when the sale happened), AND does not remove duplicate rows
--   from the at-least-once POS collector. Our pipeline corrects both issues.
-- =============================================================================

SELECT
    week_ending,
    channel,
    ROUND(our_gross_sales, 2)          AS our_gross_sales_inr,
    ROUND(legacy_gross_sales_inr, 2)   AS legacy_gross_sales_inr,
    ROUND(gross_sales_variance, 2)     AS variance_inr,
    ROUND(gross_sales_variance_pct, 2) AS variance_pct,
    our_units_sold,
    legacy_units_sold,
    our_basket_count,
    legacy_basket_count,
    variance_explanation
FROM kestrel_gold.finance_reconciliation
ORDER BY week_ending, channel;
