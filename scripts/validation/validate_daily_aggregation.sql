/*
============================================================================
VERSION LOG:
- v1.0 (2025-08-20): Initial validation script.
- v2.0 (2025-08-20):
    - Refactored to report metrics by unique members instead of total days.
    - Optimized queries with APPROX_COUNT_DISTINCT to prevent timeouts.
    - Consolidated feature validation into a single, comprehensive query.
    - Removed slow queries (total row count, date gaps).
- v2.1 (2025-08-20):
    - Split the large feature validation query into two separate queries
      (averages and unique counts) to prevent timeouts.
- v2.2 (2025-08-20):
    - Changed feature average calculation to be per-member instead of a
      simple average across all rows, providing a more representative metric.
============================================================================
VALIDATION SCRIPT for EVENTS_DAILY_AGG
PURPOSE: This script runs a series of checks to validate the integrity,
         correctness, and distribution of data in the final aggregated
         member-day table.
============================================================================
*/

-- Set the target table to be validated
SET TARGET_TABLE = 'TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG';

-- Validation 1: Check for NULLs in critical columns
SELECT
    COUNT_IF(member_id IS NULL) AS null_member_ids,
    COUNT_IF(event_date IS NULL) AS null_event_dates,
    COUNT_IF(engagement_group IS NULL) AS null_engagement_groups
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 2: Check for future event dates
SELECT COUNT(*) AS future_event_dates
FROM IDENTIFIER($TARGET_TABLE)
WHERE event_date > CURRENT_DATE();

-- Validation 3: Check label distribution (unique members)
SELECT
    APPROX_COUNT_DISTINCT(IFF(y_ed_90d = 1, member_id, NULL)) AS unique_members_ed_90d,
    APPROX_COUNT_DISTINCT(IFF(y_ip_90d = 1, member_id, NULL)) AS unique_members_ip_90d,
    APPROX_COUNT_DISTINCT(IFF(y_any_90d = 1, member_id, NULL)) AS unique_members_any_90d,
    APPROX_COUNT_DISTINCT(member_id) AS total_unique_members
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 4: Check distribution of key demographic and grouping features
SELECT
    engagement_group,
    normalized_coverage_category,
    APPROX_COUNT_DISTINCT(member_id) AS unique_members
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY 1, 2
ORDER BY 1, 2;

-- Validation 5: Get basic statistics for key numeric features (Averages Per Member)
WITH member_max_features AS (
    SELECT
        member_id,
        MAX(paid_sum_90d) as max_paid_sum_90d,
        MAX(cnt_ed_visits_90d) as max_ed_visits_90d,
        MAX(cnt_ip_visits_90d) as max_ip_visits_90d,
        MAX(cnt_any_hcc_90d) as max_any_hcc_90d,
        MAX(paid_sum_180d) as max_paid_sum_180d,
        MAX(cnt_ed_visits_180d) as max_ed_visits_180d,
        MAX(cnt_ip_visits_180d) as max_ip_visits_180d,
        MAX(cnt_any_hcc_180d) as max_any_hcc_180d
    FROM IDENTIFIER($TARGET_TABLE)
    GROUP BY 1
)
SELECT
    'Feature Averages Per Member' AS feature_set,
    -- 90-Day Averages
    AVG(max_paid_sum_90d) AS avg_member_paid_sum_90d,
    AVG(max_ed_visits_90d) AS avg_member_ed_visits_90d,
    AVG(max_ip_visits_90d) AS avg_member_ip_visits_90d,
    AVG(max_any_hcc_90d) AS avg_member_any_hcc_90d,
    -- 180-Day Averages
    AVG(max_paid_sum_180d) AS avg_member_paid_sum_180d,
    AVG(max_ed_visits_180d) AS avg_member_ed_visits_180d,
    AVG(max_ip_visits_180d) AS avg_member_ip_visits_180d,
    AVG(max_any_hcc_180d) AS avg_member_any_hcc_180d
FROM member_max_features;

-- Validation 6: Get basic statistics for key numeric features (Unique Member Counts)
SELECT
    'Feature Unique Member Counts' AS feature_set,
    -- 90-Day Unique Member Counts
    APPROX_COUNT_DISTINCT(IFF(cnt_ed_visits_90d > 0, member_id, NULL)) as unique_members_with_ed_90d,
    APPROX_COUNT_DISTINCT(IFF(cnt_ip_visits_90d > 0, member_id, NULL)) as unique_members_with_ip_90d,
    APPROX_COUNT_DISTINCT(IFF(cnt_any_hcc_90d > 0, member_id, NULL)) as unique_members_with_hcc_90d,
    -- 180-Day Unique Member Counts
    APPROX_COUNT_DISTINCT(IFF(cnt_ed_visits_180d > 0, member_id, NULL)) as unique_members_with_ed_180d,
    APPROX_COUNT_DISTINCT(IFF(cnt_ip_visits_180d > 0, member_id, NULL)) as unique_members_with_ip_180d,
    APPROX_COUNT_DISTINCT(IFF(cnt_any_hcc_180d > 0, member_id, NULL)) as unique_members_with_hcc_180d
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 7: Check distribution of note scores
SELECT
    'Note Scores' AS feature_set,
    AVG(note_health_score) AS avg_health_score,
    MIN(note_health_score) AS min_health_score,
    MAX(note_health_score) AS max_health_score,
    AVG(note_risk_harm_score) AS avg_risk_harm_score,
    MIN(note_risk_harm_score) AS min_risk_harm_score,
    MAX(note_risk_harm_score) AS max_risk_harm_score
FROM IDENTIFIER($TARGET_TABLE)
WHERE note_health_score IS NOT NULL OR note_risk_harm_score IS NOT NULL;

-- Validation 8: Check inconsistency feature flags
SELECT
    SUM(inconsistency_med_noncompliance) AS total_med_noncompliance,
    SUM(inconsistency_appt_noncompliance) AS total_appt_noncompliance
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 9: Check for duplicate member-day records
SELECT
    'Duplicate Check' AS check_name,
    COUNT(*) AS duplicate_rows
FROM (
    SELECT
        member_id,
        event_date,
        COUNT(*)
    FROM IDENTIFIER($TARGET_TABLE)
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
);

-- Validation 10: Thematic Label Validation (Nulls and Invalid Values)
SELECT
    'Null Checks' AS validation_type,
    COUNT_IF(y_hiv_60d IS NULL) AS y_hiv_60d,
    COUNT_IF(y_malnutrition_60d IS NULL) AS y_malnutrition_60d,
    COUNT_IF(y_smi_60d IS NULL) AS y_smi_60d,
    COUNT_IF(y_chf_60d IS NULL) AS y_chf_60d,
    COUNT_IF(y_copd_60d IS NULL) AS y_copd_60d,
    COUNT_IF(y_sud_60d IS NULL) AS y_sud_60d,
    COUNT_IF(y_diabetes_60d IS NULL) AS y_diabetes_60d
FROM IDENTIFIER($TARGET_TABLE)
UNION ALL
SELECT
    'Invalid Value Checks (not 0 or 1)' AS validation_type,
    COUNT_IF(y_hiv_60d NOT IN (0, 1)) AS y_hiv_60d,
    COUNT_IF(y_malnutrition_60d NOT IN (0, 1)) AS y_malnutrition_60d,
    COUNT_IF(y_smi_60d NOT IN (0, 1)) AS y_smi_60d,
    COUNT_IF(y_chf_60d NOT IN (0, 1)) AS y_chf_60d,
    COUNT_IF(y_copd_60d NOT IN (0, 1)) AS y_copd_60d,
    COUNT_IF(y_sud_60d NOT IN (0, 1)) AS y_sud_60d,
    COUNT_IF(y_diabetes_60d NOT IN (0, 1)) AS y_diabetes_60d
FROM IDENTIFIER($TARGET_TABLE);

-- Validation 11: Thematic Label Distribution (Unique Members)
SELECT
    'Unique Members with Thematic Label' AS validation_type,
    APPROX_COUNT_DISTINCT(IFF(y_hiv_60d = 1, member_id, NULL)) AS unique_members_hiv_60d,
    APPROX_COUNT_DISTINCT(IFF(y_malnutrition_60d = 1, member_id, NULL)) AS unique_members_malnutrition_60d,
    APPROX_COUNT_DISTINCT(IFF(y_smi_60d = 1, member_id, NULL)) AS unique_members_smi_60d,
    APPROX_COUNT_DISTINCT(IFF(y_chf_60d = 1, member_id, NULL)) AS unique_members_chf_60d,
    APPROX_COUNT_DISTINCT(IFF(y_copd_60d = 1, member_id, NULL)) AS unique_members_copd_60d,
    APPROX_COUNT_DISTINCT(IFF(y_sud_60d = 1, member_id, NULL)) AS unique_members_sud_60d,
    APPROX_COUNT_DISTINCT(IFF(y_diabetes_60d = 1, member_id, NULL)) AS unique_members_diabetes_60d
FROM IDENTIFIER($TARGET_TABLE);
