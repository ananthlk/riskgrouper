/*
================================================================================
Data Validation and Quality Assurance Script
PURPOSE: This script contains a series of validation queries to ensure the
         integrity, accuracy, and quality of the data pipeline, from the
         daily aggregations to the final modeling dataset.

It includes checks for:
1.  **Claim Latency**: Analyzes the time difference between event dates and
    the current date to understand data freshness.
2.  **Duplicate Records**: Verifies that there are no duplicate member-day
    records in key tables.
3.  **Null Values**: Checks for unexpected nulls in critical columns.
4.  **Logical Consistency**: Ensures that data values are logical (e.g., age is
    not negative, one-hot encodings are correct).
5.  **Train/Test Split Integrity**: Confirms that members are not split across
    both training and testing sets.
6.  **Feature Variation Checks**: Ensures that key features, especially note
    scores and their deltas, have variation and are not constant.
================================================================================
*/

-- ================================================================================
-- Validation Check 1: Claim Latency Analysis
-- Purpose: To understand the distribution of delays between the actual event
--          date and when it appears in the dataset.
-- ================================================================================
SELECT
    'Claim Latency' AS test_name,
    MIN(DATEDIFF(day, EVENT_DATE, CURRENT_DATE())) AS min_days_latency,
    AVG(DATEDIFF(day, EVENT_DATE, CURRENT_DATE())) AS avg_days_latency,
    MAX(DATEDIFF(day, EVENT_DATE, CURRENT_DATE())) AS max_days_latency
FROM TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG;


-- ================================================================================
-- Validation Check 2: Duplicate Records Check
-- Purpose: To ensure that the aggregation steps have correctly resulted in one
--          unique record per member per day.
-- ================================================================================
-- Check on EVENTS_DAILY_AGG
SELECT
    'Duplicate Check on EVENTS_DAILY_AGG' AS test_name,
    MEMBER_ID,
    EVENT_DATE,
    COUNT(*) AS record_count
FROM TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG
GROUP BY MEMBER_ID, EVENT_DATE
HAVING COUNT(*) > 1;

-- Check on MODELING_DATASET
SELECT
    'Duplicate Check on MODELING_DATASET' AS test_name,
    MEMBER_ID,
    EVENT_DATE,
    COUNT(*) AS record_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
GROUP BY MEMBER_ID, EVENT_DATE
HAVING COUNT(*) > 1;


-- ================================================================================
-- Validation Check 3: Null Value Checks in Final Modeling Dataset
-- Purpose: To verify that critical columns in the modeling dataset do not
--          contain unexpected null values.
-- ================================================================================
SELECT
    'Null Check: MEMBER_ID' AS test_name,
    COUNT(*) AS null_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE MEMBER_ID IS NULL
UNION ALL
SELECT
    'Null Check: EVENT_DATE' AS test_name,
    COUNT(*) AS null_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE EVENT_DATE IS NULL
UNION ALL
SELECT
    'Null Check: dataset_split' AS test_name,
    COUNT(*) AS null_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE dataset_split IS NULL;


-- ================================================================================
-- Validation Check 4: Logical Consistency Checks
-- Purpose: To check for values that are not logically possible, such as
--          negative age or incorrect one-hot encodings.
-- ================================================================================
-- Check for negative age
SELECT
    'Logical Check: Negative Age' AS test_name,
    COUNT(*) AS invalid_record_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE age < 0;

-- Check gender one-hot encoding
SELECT
    'Logical Check: Gender Encoding' AS test_name,
    COUNT(*) AS invalid_record_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE (IS_MALE + IS_FEMALE) <> 1;

-- Check engagement group one-hot encoding
SELECT
    'Logical Check: Engagement Group Encoding' AS test_name,
    COUNT(*) AS invalid_record_count
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE (IS_ENGAGED + IS_SELECTED_NOT_ENGAGED + IS_NOT_SELECTED_FOR_ENGAGEMENT) <> 1;


-- ================================================================================
-- Validation Check 5: Train/Test Split Integrity
-- Purpose: To confirm that no member is present in both the TRAIN and TEST sets,
--          which would represent data leakage.
-- ================================================================================
SELECT
    'Train/Test Split Leakage' AS test_name,
    MEMBER_ID,
    COUNT(DISTINCT dataset_split) AS distinct_sets
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
GROUP BY MEMBER_ID
HAVING COUNT(DISTINCT dataset_split) > 1;


-- ================================================================================
-- Validation Check 6: Feature Variation Checks
-- Purpose: To ensure that key features, especially note scores and their deltas,
--          have variation and are not constant, which would make them useless
--          for modeling. A standard deviation of 0 indicates no variation.
-- ================================================================================
SELECT
    'Variation Check: Note Scores' AS test_name,
    STDDEV(NOTE_HEALTH_SCORE) AS stddev_health_score,
    STDDEV(NOTE_RISK_HARM_SCORE) AS stddev_risk_harm_score,
    STDDEV(NOTE_SOCIAL_STAB_SCORE) AS stddev_social_stab_score,
    STDDEV(NOTE_MED_ADHERENCE_SCORE) AS stddev_med_adherence_score,
    STDDEV(NOTE_CARE_ENGAGEMENT_SCORE) AS stddev_care_engagement_score,
    STDDEV(NOTE_PROGRAM_TRUST_SCORE) AS stddev_program_trust_score,
    STDDEV(NOTE_SELF_SCORE) AS stddev_self_score
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE NOTE_HEALTH_SCORE IS NOT NULL; -- Only for engaged members with notes

SELECT
    'Variation Check: Note Deltas' AS test_name,
    STDDEV(NOTE_HEALTH_DELTA_30D) AS stddev_health_delta,
    STDDEV(NOTE_RISK_HARM_DELTA_30D) AS stddev_risk_harm_delta,
    STDDEV(NOTE_SOCIAL_STAB_DELTA_30D) AS stddev_social_stab_delta,
    STDDEV(NOTE_MED_ADHERENCE_DELTA_30D) AS stddev_med_adherence_delta,
    STDDEV(NOTE_CARE_ENGAGEMENT_DELTA_30D) AS stddev_care_engagement_delta
FROM TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE NOTE_HEALTH_DELTA_30D IS NOT NULL;


-- ================================================================================
-- Validation Check 7: Claim Lag Validation
-- Purpose: To verify that the 4-month claim lag is being correctly applied,
--          ensuring no claim events are newer than the specified lag period.
-- ================================================================================
SELECT
    'Claim Lag Validation' AS test_name,
    COUNT(*) AS recent_claim_events_count
FROM TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG
WHERE event_type LIKE 'CLAIM_%'
  AND event_date > DATEADD('month', -4, CURRENT_DATE());
