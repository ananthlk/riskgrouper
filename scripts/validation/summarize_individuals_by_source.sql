/*
============================================================================
VALIDATION SCRIPT: Summarize Unique Individuals by Source
PURPOSE: This script calculates the number of unique individuals with IP or ED
         events, categorized by the data source (ADT, AUTH, HC, CLAIMS)
         and aggregated on a monthly basis.
============================================================================
*/

-- Set the target table to be validated
SET TARGET_TABLE = 'TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX';

-- Validation 9: Monthly unique individual counts by source category
SELECT
    YEAR(event_date) AS event_year,
    MONTH(event_date) AS event_month,
    CASE
        WHEN event_type = 'ADT' THEN 'ADT'
        WHEN event_type IN ('ZUS_AUTH', 'UHC_AUTH') THEN 'AUTH'
        WHEN event_type = 'HEALTH_CHECK' THEN 'HC'
        WHEN event_type LIKE 'CLAIM%' THEN 'CLAIMS'
    END AS source_category,
    COUNT(DISTINCT member_id) AS unique_individuals
FROM IDENTIFIER($TARGET_TABLE)
WHERE (event_type IN ('ADT', 'ZUS_AUTH', 'UHC_AUTH', 'HEALTH_CHECK') OR event_type LIKE 'CLAIM%')
  AND (is_ip_event OR is_ed_event)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
