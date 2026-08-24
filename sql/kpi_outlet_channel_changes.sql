-- =============================================================================
-- KPI: Outlet Channel Changes
-- =============================================================================
-- Business Definition: Identifies outlets whose channel classification (GT, MT,
--   HORECA, ECOM) changed during the reporting period. Uses SCD Type-2 history
--   from ERP CDC outlet master.
-- Grain: Outlet × Change event
-- Source: kestrel_silver.outlet_history (SCD Type-2 with valid_from/valid_to)
-- Owner: Sales / Distribution
-- Known Limitation:
--   - Historical reporting without SCD used today's channel for all historical
--     transactions (ticket KP-3155). Our outlet_history now supports point-in-time
--     lookups.
-- =============================================================================

WITH channel_changes AS (
    SELECT
        outlet_code,
        outlet_name,
        channel                     AS current_channel,
        valid_from                  AS changed_at,
        LAG(channel) OVER (
            PARTITION BY outlet_code
            ORDER BY valid_from, __seq
        )                           AS previous_channel,
        __op
    FROM kestrel_silver.outlet_history
)
SELECT
    outlet_code,
    outlet_name,
    previous_channel,
    current_channel,
    changed_at
FROM channel_changes
WHERE previous_channel IS NOT NULL
  AND previous_channel != current_channel
  AND __op = 'U'
ORDER BY changed_at DESC;
