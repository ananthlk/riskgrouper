/*
============================================================================
VERSION LOG:
- v1.0 (2025-08-19): Initial validation script.
- v1.1 (2025-08-19):
    - Consolidated IP/ED and unique member summaries into this script.
    - Corrected label validation logic to count distinct member-days instead of total events.
============================================================================
VALIDATION SCRIPT for EVENTS_WITH_LABELS_RX
PURPOSE: This script runs a series of checks to validate the integrity,
         correctness, and distribution of data in the final event table.
============================================================================
*/

-- Set the target table to be validated
SET TARGET_TABLE = 'TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX';

-- Validation 1: Check for total row count
SELECT COUNT(*) AS total_rows FROM IDENTIFIER($TARGET_TABLE);

-- Validation 2: Check for NULLs in critical columns
SELECT
    COUNT_IF(member_id IS NULL) AS null_member_ids,
    COUNT_IF(event_date IS NULL) AS null_event_dates
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 3: Check distribution of event_type
SELECT
    event_type,
    COUNT(*) AS count_per_type,
    MIN(event_date) AS min_event_date,
    MAX(event_date) AS max_event_date
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY 1
ORDER BY 2 DESC;

-- Validation 4: Check for future event dates
SELECT COUNT(*) AS future_event_dates
FROM IDENTIFIER($TARGET_TABLE)
WHERE event_date > CURRENT_DATE();

-- Validation 5: Check label generation (counting distinct days, not all events)
SELECT
    COUNT(DISTINCT CASE WHEN y_any_30d = 1 THEN member_id || '|' || event_date END) AS positive_30d_days,
    COUNT(DISTINCT CASE WHEN y_any_60d = 1 THEN member_id || '|' || event_date END) AS positive_60d_days,
    COUNT(DISTINCT CASE WHEN y_any_90d = 1 THEN member_id || '|' || event_date END) AS positive_90d_days,
    COUNT(DISTINCT member_id || '|' || event_date) AS total_unique_days
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 6: Check source flag generation for non-claim events
SELECT
    SUM(is_ip_adt) AS total_ip_adt,
    SUM(is_ip_auth) AS total_ip_auth,
    SUM(is_ip_hc) AS total_ip_hc,
    SUM(is_ed_adt) AS total_ed_adt,
    SUM(is_ed_auth) AS total_ed_auth,
    SUM(is_ed_hc) AS total_ed_hc
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 7: Check for events that didn't join to a member
SELECT COUNT(*) AS events_without_member_info
FROM IDENTIFIER($TARGET_TABLE)
WHERE market IS NULL OR engagement_group IS NULL;

-- Validation 8: Monthly IP/ED counts by source category
SELECT
    YEAR(event_date) AS event_year,
    MONTH(event_date) AS event_month,
    CASE
        WHEN event_type = 'ADT' THEN 'ADT'
        WHEN event_type IN ('ZUS_AUTH', 'UHC_AUTH') THEN 'AUTH'
        WHEN event_type = 'HEALTH_CHECK' THEN 'HC'
        WHEN event_type LIKE 'CLAIM%' THEN 'CLAIMS'
    END AS source_category,
    COUNT_IF(is_ip_event) AS total_ip_events,
    COUNT_IF(is_ed_event) AS total_ed_events
FROM IDENTIFIER($TARGET_TABLE)
WHERE (event_type IN ('ADT', 'ZUS_AUTH', 'UHC_AUTH', 'HEALTH_CHECK') OR event_type LIKE 'CLAIM%')
  AND (is_ip_event OR is_ed_event)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

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

-- Validation 10: Count of unique members by market and engagement group
SELECT
    market,
    engagement_group,
    COUNT(DISTINCT member_id) AS unique_member_count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY 1, 2
ORDER BY 1, 2;
