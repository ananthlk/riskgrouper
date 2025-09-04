/*
================================================================================
VALIDATION SCRIPT: MODELING_DATASET

PURPOSE:
This script runs a series of validation tests on the 'MODELING_DATASET' table
to ensure data integrity, consistency, and readiness for machine learning.

VALIDATION CHECKS:
1.  Primary Key Uniqueness: (MEMBER_ID, EVENT_DATE) should be unique.
2.  No NULLs in Key Columns: MEMBER_ID, EVENT_DATE, and dataset_split should not be NULL.
3.  Data Split Distribution: Checks the 'TRAIN' vs. 'TEST' split ratio.
4.  Feature Range Checks: Verifies that key features like age and one-hot encoded
    columns have logical values.
5.  Label Integrity: Ensures target variables (Y_ columns) are not NULL.
6.  No Future Event Dates: Confirms that EVENT_DATE is not in the future.
================================================================================
*/

-- Set the context to the appropriate database and schema.
USE DATABASE TRANSFORMED_DATA;
USE SCHEMA _TEMP;

-- Validation 1: Check for duplicate MEMBER_ID and EVENT_DATE pairs.
-- EXPECTATION: The query should return 0 rows.
SELECT
    MEMBER_ID,
    EVENT_DATE,
    COUNT(*)
FROM
    MODELING_DATASET
GROUP BY
    MEMBER_ID,
    EVENT_DATE
HAVING
    COUNT(*) > 1;

-- Validation 2: Check for NULLs in critical columns.
-- EXPECTATION: The query should return 0 rows.
SELECT
    COUNT(*) AS null_count
FROM
    MODELING_DATASET
WHERE
    MEMBER_ID IS NULL
    OR EVENT_DATE IS NULL
    OR dataset_split IS NULL;

-- Validation 3: Check the distribution of the TRAIN/TEST split.
-- EXPECTATION: The ratio should be approximately 0.8 for TRAIN.
SELECT
    dataset_split,
    COUNT(*) AS row_count,
    COUNT(*) / (SELECT COUNT(*) FROM MODELING_DATASET) AS percentage
FROM
    MODELING_DATASET
GROUP BY
    dataset_split;

-- Validation 4a: Check for any invalid age values.
-- EXPECTATION: The query should return 0 rows.
SELECT
    COUNT(*) AS invalid_age_count
FROM
    MODELING_DATASET
WHERE
    age < 0 OR age > 120;

-- Validation 4b: Check one-hot encoded columns for invalid values.
-- EXPECTATION: The query should return 0 rows.
SELECT
    'Logical Check: Gender Encoding' AS test_name,
    COUNT(*) AS invalid_record_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE (IS_MALE + IS_FEMALE + IS_GENDER_UNKNOWN) <> 1;

-- Validation 4c: Check for negative values in 'days since' features.
-- EXPECTATION: The query should return 0 rows.
SELECT
    COUNT(*) AS negative_days_since
FROM
    MODELING_DATASET
WHERE
    DAYS_SINCE_LAST_HEALTH_NOTE < 0
    OR DAYS_SINCE_LAST_RISK_NOTE < 0;

-- Validation 5: Check for NULLs in target label columns.
-- EXPECTATION: The query should return 0 rows.
SELECT
    COUNT(*) AS null_label_count
FROM
    MODELING_DATASET
WHERE
    Y_ANY_90D IS NULL;

-- Validation 6: Check for event dates in the future.
-- EXPECTATION: The query should return 0 rows.
SELECT
    COUNT(*) AS future_event_dates
FROM
    MODELING_DATASET
WHERE
    EVENT_DATE > CURRENT_DATE();

-- Summary of all feature columns to check for NULLs or strange values.
-- This provides a high-level overview of data quality.
SELECT
    COUNT(CASE WHEN age IS NULL THEN 1 END) AS null_age,
    COUNT(CASE WHEN NOTE_HEALTH_DELTA_30D IS NULL THEN 1 END) AS null_health_delta,
    COUNT(CASE WHEN CLAIMS_IN_LAST_30D_COUNT IS NULL THEN 1 END) AS null_claims_30d,
    COUNT(CASE WHEN NEW_RX_ANTIPSYCH_30D IS NULL THEN 1 END) AS null_new_rx,
    COUNT(CASE WHEN MONTHS_SINCE_FIRST_DIABETES IS NULL THEN 1 END) AS null_months_since
FROM
    MODELING_DATASET;

-- Validation 7: Check label prevalence per 1000 unique members.
-- EXPECTATION: Provides a baseline for label rates.
WITH member_counts AS (
    SELECT
        COUNT(DISTINCT MEMBER_ID) AS total_members,
        COUNT(DISTINCT CASE WHEN Y_ANY_90D = 1 THEN MEMBER_ID END) AS positive_any_90d,
        COUNT(DISTINCT CASE WHEN Y_ED_90D = 1 THEN MEMBER_ID END) AS positive_ed_90d,
        COUNT(DISTINCT CASE WHEN Y_IP_90D = 1 THEN MEMBER_ID END) AS positive_ip_90d,
        COUNT(DISTINCT CASE WHEN Y_ANY_60D = 1 THEN MEMBER_ID END) AS positive_any_60d,
        COUNT(DISTINCT CASE WHEN Y_ED_60D = 1 THEN MEMBER_ID END) AS positive_ed_60d,
        COUNT(DISTINCT CASE WHEN Y_IP_60D = 1 THEN MEMBER_ID END) AS positive_ip_60d,
        COUNT(DISTINCT CASE WHEN Y_ANY_30D = 1 THEN MEMBER_ID END) AS positive_any_30d,
        COUNT(DISTINCT CASE WHEN Y_ED_30D = 1 THEN MEMBER_ID END) AS positive_ed_30d,
        COUNT(DISTINCT CASE WHEN Y_IP_30D = 1 THEN MEMBER_ID END) AS positive_ip_30d
    FROM
        MODELING_DATASET
)
SELECT
    'ANY_90D' AS label, (positive_any_90d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'ED_90D' AS label, (positive_ed_90d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'IP_90D' AS label, (positive_ip_90d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'ANY_60D' AS label, (positive_any_60d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'ED_60D' AS label, (positive_ed_60d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'IP_60D' AS label, (positive_ip_60d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'ANY_30D' AS label, (positive_any_30d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'ED_30D' AS label, (positive_ed_30d / total_members) * 1000 AS rate_per_1000_members FROM member_counts UNION ALL
SELECT
    'IP_30D' AS label, (positive_ip_30d / total_members) * 1000 AS rate_per_1000_members FROM member_counts;
