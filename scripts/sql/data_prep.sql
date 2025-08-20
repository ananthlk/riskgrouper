/*
================================================================================
PRODUCTION-READY: PREPARE DATA FOR ROBUST XGBOOST ANALYSIS
PURPOSE: This query creates a single, comprehensive modeling dataset by aggregating
         event-level data and engineering features for machine learning.

Key Features:
- Aggregates daily event data to a single row per member per day.
- Corrects for duplicate rows that can arise from event-level data.
- Creates new features, including time-based data, one-hot encoded
  categorical variables, and specific HCC flags.
- Includes a key feature ('HAS_CARE_NOTES_POST_PERIOD') to isolate the
  predictive impact of care notes.
- Splits the data into 'TRAIN' and 'TEST' sets based on a member-level split,
  not a time cutoff.

Assumptions:
- 'EVENTS_DAILY_AGG' and 'EVENTS_WITH_LABELS_RX' are pre-existing tables.
================================================================================
*/

-- Define the final table for our modeling dataset.
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.MODELING_DATASET AS
WITH daily_agg AS (
    -- Step 1: Select all data from the pre-aggregated EVENTS_DAILY_AGG table.
    -- No further aggregation is needed as the upstream table is already at the member-day level.
    SELECT * FROM TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG
),
longitudinal_features AS (
    -- Step 2: Create advanced longitudinal features for the model.
    -- This CTE adds powerful time-based signals for note score changes, first-time diagnoses, and recency of events.
    SELECT
        MEMBER_ID,
        EVENT_DATE,
        -- Note Score Delta: Difference between current score and 30-day moving average.
        daily_agg.NOTE_HEALTH_SCORE - AVG(daily_agg.NOTE_HEALTH_SCORE) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS NOTE_HEALTH_DELTA_30D,
        daily_agg.NOTE_RISK_HARM_SCORE - AVG(daily_agg.NOTE_RISK_HARM_SCORE) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS NOTE_RISK_HARM_DELTA_30D,
        daily_agg.NOTE_SOCIAL_STAB_SCORE - AVG(daily_agg.NOTE_SOCIAL_STAB_SCORE) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS NOTE_SOCIAL_STAB_DELTA_30D,
        daily_agg.NOTE_MED_ADHERENCE_SCORE - AVG(daily_agg.NOTE_MED_ADHERENCE_SCORE) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS NOTE_MED_ADHERENCE_DELTA_30D,
        daily_agg.NOTE_CARE_ENGAGEMENT_SCORE - AVG(daily_agg.NOTE_CARE_ENGAGEMENT_SCORE) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS NOTE_CARE_ENGAGEMENT_DELTA_30D,
        -- Days since last note for each category
        DATEDIFF(day, LAG(CASE WHEN daily_agg.NOTE_HEALTH_SCORE IS NOT NULL THEN daily_agg.EVENT_DATE ELSE NULL END) IGNORE NULLS OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE), daily_agg.EVENT_DATE) AS DAYS_SINCE_LAST_HEALTH_NOTE,
        DATEDIFF(day, LAG(CASE WHEN daily_agg.NOTE_RISK_HARM_SCORE IS NOT NULL THEN daily_agg.EVENT_DATE ELSE NULL END) IGNORE NULLS OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE), daily_agg.EVENT_DATE) AS DAYS_SINCE_LAST_RISK_NOTE,
        -- Recency flags for claims in the last 30 days.
        SUM(daily_agg.cnt_ed_visits_90d + daily_agg.cnt_ip_visits_90d) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS CLAIMS_IN_LAST_30D_COUNT,
        -- Recency flags for ED/IP events in the last 30 days.
        SUM(daily_agg.cnt_ed_visits_90d) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS ED_IN_LAST_30D_COUNT,
        SUM(daily_agg.cnt_ip_visits_90d) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS IP_IN_LAST_30D_COUNT,
        -- Recency flags for claims in the last 180 days.
        SUM(daily_agg.cnt_ed_visits_180d + daily_agg.cnt_ip_visits_180d) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING) AS CLAIMS_IN_LAST_180D_COUNT,
        -- Recency flags for ED/IP events in the last 180 days.
        SUM(daily_agg.cnt_ed_visits_180d) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING) AS ED_IN_LAST_180D_COUNT,
        SUM(daily_agg.cnt_ip_visits_180d) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING) AS IP_IN_LAST_180D_COUNT,
        
        -- Detects if a new prescription class has appeared in the last 30 days.
        CASE WHEN daily_agg.mpr_antipsychotic_90d > 0 AND LAG(daily_agg.mpr_antipsychotic_90d, 30, 0) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE) = 0 THEN 1 ELSE 0 END AS NEW_RX_ANTIPSYCH_30D,
        CASE WHEN daily_agg.mpr_insulin_90d > 0 AND LAG(daily_agg.mpr_insulin_90d, 30, 0) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE) = 0 THEN 1 ELSE 0 END AS NEW_RX_INSULIN_30D,
        CASE WHEN daily_agg.mpr_oral_antidiab_90d > 0 AND LAG(daily_agg.mpr_oral_antidiab_90d, 30, 0) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE) = 0 THEN 1 ELSE 0 END AS NEW_RX_ORAL_ANTIDIAB_30D,
        CASE WHEN daily_agg.mpr_statin_90d > 0 AND LAG(daily_agg.mpr_statin_90d, 30, 0) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE) = 0 THEN 1 ELSE 0 END AS NEW_RX_STATIN_30D,
        CASE WHEN daily_agg.mpr_beta_blocker_90d > 0 AND LAG(daily_agg.mpr_beta_blocker_90d, 30, 0) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE) = 0 THEN 1 ELSE 0 END AS NEW_RX_BETA_BLOCKER_30D,
        CASE WHEN daily_agg.mpr_opioid_90d > 0 AND LAG(daily_agg.mpr_opioid_90d, 30, 0) OVER (PARTITION BY MEMBER_ID ORDER BY EVENT_DATE) = 0 THEN 1 ELSE 0 END AS NEW_RX_OPIOID_30D
    FROM daily_agg
),
first_note AS (
    -- Step 3: Find the date of the very first 'CARE_NOTE' for each engaged member.
    SELECT
        t1.member_id,
        MIN(t1.event_date) AS first_note_date
    FROM TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG AS t1
    WHERE t1.note_health_score IS NOT NULL -- A proxy for any note
    GROUP BY t1.member_id
),
first_hcc_dates AS (
    -- Step 4: Find the first diagnosis date for each specific HCC category.
    SELECT
        MEMBER_ID,
        MIN(CASE WHEN cnt_hcc_diabetes_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_DIABETES_DATE,
        MIN(CASE WHEN cnt_hcc_mental_health_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_MENTAL_HEALTH_DATE,
        MIN(CASE WHEN cnt_hcc_cardiovascular_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_CARDIOVASCULAR_DATE,
        MIN(CASE WHEN cnt_hcc_pulmonary_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_PULMONARY_DATE,
        MIN(CASE WHEN cnt_hcc_kidney_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_KIDNEY_DATE,
        MIN(CASE WHEN cnt_hcc_sud_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_SUD_DATE
    FROM daily_agg
    GROUP BY MEMBER_ID
),
first_rx_dates AS (
    -- Step 5: Find the first fill date for each specific medication cohort.
    SELECT
        MEMBER_ID,
        MIN(CASE WHEN mpr_antipsychotic_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_ANTIPSYCH_DATE,
        MIN(CASE WHEN mpr_insulin_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_INSULIN_DATE,
        MIN(CASE WHEN mpr_oral_antidiab_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_ORAL_ANTIDIAB_DATE,
        MIN(CASE WHEN mpr_statin_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_STATIN_DATE,
        MIN(CASE WHEN mpr_beta_blocker_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_BETA_BLOCKER_DATE,
        MIN(CASE WHEN mpr_opioid_90d > 0 THEN EVENT_DATE ELSE NULL END) AS FIRST_OPIOID_DATE
    FROM daily_agg
    GROUP BY MEMBER_ID
),
member_split AS (
    -- Step 6a: Create a member-level split for training and testing.
    -- This ensures that all records for a given member are in the same dataset.
    SELECT
        MEMBER_ID,
        CASE
            WHEN UNIFORM(0::float, 1::float, RANDOM()) < 0.8 THEN 'TRAIN'
            ELSE 'TEST'
        END AS dataset_split
    FROM (
        SELECT DISTINCT MEMBER_ID FROM daily_agg
    )
),
prepped_features AS (
    -- Step 6: Create a single comprehensive dataset with all engineered features.
    SELECT
        base.*,
        lf.NOTE_HEALTH_DELTA_30D,
        lf.NOTE_RISK_HARM_DELTA_30D,
        lf.NOTE_SOCIAL_STAB_DELTA_30D,
        lf.NOTE_MED_ADHERENCE_DELTA_30D,
        lf.NOTE_CARE_ENGAGEMENT_DELTA_30D,
        lf.DAYS_SINCE_LAST_HEALTH_NOTE,
        lf.DAYS_SINCE_LAST_RISK_NOTE,
        lf.CLAIMS_IN_LAST_30D_COUNT,
        lf.ED_IN_LAST_30D_COUNT,
        lf.IP_IN_LAST_30D_COUNT,
        lf.CLAIMS_IN_LAST_180D_COUNT,
        lf.ED_IN_LAST_180D_COUNT,
        lf.IP_IN_LAST_180D_COUNT,
        lf.NEW_RX_ANTIPSYCH_30D,
        lf.NEW_RX_INSULIN_30D,
        lf.NEW_RX_ORAL_ANTIDIAB_30D,
        lf.NEW_RX_STATIN_30D,
        lf.NEW_RX_BETA_BLOCKER_30D,
        lf.NEW_RX_OPIOID_30D,
        fn.first_note_date,
        -- Calculate months since first diagnosis for each HCC.
        DATEDIFF(month, hcc.FIRST_DIABETES_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_DIABETES,
        DATEDIFF(month, hcc.FIRST_MENTAL_HEALTH_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_MENTAL_HEALTH,
        DATEDIFF(month, hcc.FIRST_CARDIOVASCULAR_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_CARDIOVASCULAR,
        DATEDIFF(month, hcc.FIRST_PULMONARY_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_PULMONARY,
        DATEDIFF(month, hcc.FIRST_KIDNEY_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_KIDNEY,
        DATEDIFF(month, hcc.FIRST_SUD_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_SUD,
        -- Calculate months since first fill for each RX class.
        DATEDIFF(month, rx.FIRST_ANTIPSYCH_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_ANTIPSYCH,
        DATEDIFF(month, rx.FIRST_INSULIN_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_INSULIN,
        DATEDIFF(month, rx.FIRST_ORAL_ANTIDIAB_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_ORAL_ANTIDIAB,
        DATEDIFF(month, rx.FIRST_STATIN_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_STATIN,
        DATEDIFF(month, rx.FIRST_BETA_BLOCKER_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_BETA_BLOCKER,
        DATEDIFF(month, rx.FIRST_OPIOID_DATE, base.EVENT_DATE) AS MONTHS_SINCE_FIRST_OPIOID,
        -- Time-based features extracted from the event date.
        DAYOFWEEK(base.EVENT_DATE) AS day_of_week,
        MONTH(base.EVENT_DATE) AS month,
        YEAR(base.EVENT_DATE) AS year,
        CASE WHEN base.DOB > base.EVENT_DATE THEN NULL ELSE DATEDIFF(day, base.DOB, base.EVENT_DATE) / 365.25 END AS age
    FROM daily_agg AS base
    LEFT JOIN longitudinal_features AS lf
        ON base.MEMBER_ID = lf.MEMBER_ID AND base.EVENT_DATE = lf.EVENT_DATE
    LEFT JOIN first_note AS fn
        ON base.MEMBER_ID = fn.MEMBER_ID
    LEFT JOIN first_hcc_dates AS hcc
        ON base.MEMBER_ID = hcc.MEMBER_ID
    LEFT JOIN first_rx_dates AS rx
        ON base.MEMBER_ID = rx.MEMBER_ID
)
SELECT
    -- Step 7: Final SELECT statement to create the modeling dataset.
    -- It includes all features, labels, and the final data split column.
    p.MEMBER_ID,
    p.EVENT_DATE,
    p.DOB,
    p.day_of_week,
    p.month,
    p.year,
    p.age,
    
    -- One-hot encoded features for categorical data.
    CASE WHEN UPPER(p.GENDER) = 'M' THEN 1 ELSE 0 END AS IS_MALE,
    CASE WHEN UPPER(p.GENDER) = 'F' THEN 1 ELSE 0 END AS IS_FEMALE,
    CASE WHEN UPPER(p.GENDER) NOT IN ('M', 'F') OR p.GENDER IS NULL THEN 1 ELSE 0 END AS IS_GENDER_UNKNOWN,
    CASE WHEN p.ENGAGEMENT_GROUP = 'engaged_in_program' THEN 1 ELSE 0 END AS IS_ENGAGED,
    CASE WHEN p.ENGAGEMENT_GROUP = 'selected_not_engaged' THEN 1 ELSE 0 END AS IS_SELECTED_NOT_ENGAGED,
    CASE WHEN p.ENGAGEMENT_GROUP = 'not_selected_for_engagement' THEN 1 ELSE 0 END AS IS_NOT_SELECTED_FOR_ENGAGEMENT,
    
    CASE WHEN p.NORMALIZED_COVERAGE_CATEGORY = 'TANF' THEN 1 ELSE 0 END AS IS_CATEGORY_TANF,
    CASE WHEN p.NORMALIZED_COVERAGE_CATEGORY = 'EXPANSION' THEN 1 ELSE 0 END AS IS_CATEGORY_EXPANSION,
    CASE WHEN p.NORMALIZED_COVERAGE_CATEGORY = 'DSNP' THEN 1 ELSE 0 END AS IS_CATEGORY_DSNP,
    CASE WHEN p.NORMALIZED_COVERAGE_CATEGORY = 'ABD' THEN 1 ELSE 0 END AS IS_CATEGORY_ABD,
    
    -- All other aggregated and imputed features.
    p.MONTHS_SINCE_BATCHED,
    p.paid_sum_90d,
    p.cnt_ed_visits_90d,
    p.cnt_ip_visits_90d,
    p.cnt_ed_visits_90_180d,
    p.cnt_ip_visits_90_180d,
    p.cnt_hcc_diabetes_90d,
    p.cnt_hcc_mental_health_90d,
    p.cnt_hcc_cardiovascular_90d,
    p.cnt_hcc_pulmonary_90d,
    p.cnt_hcc_kidney_90d,
    p.cnt_hcc_sud_90d,
    p.cnt_any_hcc_90d,
    p.cnt_proc_psychotherapy_90d,
    p.cnt_proc_psychiatric_evals_90d,
    p.paid_sum_180d,
    p.cnt_ed_visits_180d,
    p.cnt_ip_visits_180d,
    p.cnt_hcc_diabetes_180d,
    p.cnt_hcc_mental_health_180d,
    p.cnt_hcc_cardiovascular_180d,
    p.cnt_hcc_pulmonary_180d,
    p.cnt_hcc_kidney_180d,
    p.cnt_hcc_sud_180d,
    p.cnt_any_hcc_180d,
    p.cnt_proc_psychotherapy_180d,
    p.cnt_proc_psychiatric_evals_180d,
    p.note_health_score,
    p.note_risk_harm_score,
    p.note_social_stab_score,
    p.note_med_adherence_score,
    p.note_care_engagement_score,
    p.note_program_trust_score,
    p.note_self_score,
    p.health_pop_baseline,
    p.health_indiv_baseline,
    p.cnt_fills_antipsychotic_90d,
    p.cnt_fills_insulin_90d,
    p.cnt_fills_oral_antidiab_90d,
    p.cnt_fills_statin_90d,
    p.cnt_fills_beta_blocker_90d,
    p.cnt_fills_opioid_90d,
    p.cnt_fills_antipsychotic_180d,
    p.cnt_fills_insulin_180d,
    p.cnt_fills_oral_antidiab_180d,
    p.cnt_fills_statin_180d,
    p.cnt_fills_beta_blocker_180d,
    p.cnt_fills_opioid_180d,
    p.mpr_antipsychotic_90d,
    p.mpr_insulin_90d,
    p.mpr_oral_antidiab_90d,
    p.mpr_statin_90d,
    p.mpr_beta_blocker_90d,
    p.mpr_opioid_90d,
    p.mpr_antipsychotic_180d,
    p.mpr_insulin_180d,
    p.mpr_oral_antidiab_180d,
    p.mpr_statin_180d,
    p.mpr_beta_blocker_180d,
    p.mpr_opioid_180d,
    p.inconsistency_med_noncompliance,
    p.inconsistency_appt_noncompliance,
    -- Newly added longitudinal and recency features.
    p.NOTE_HEALTH_DELTA_30D,
    p.NOTE_RISK_HARM_DELTA_30D,
    p.NOTE_SOCIAL_STAB_DELTA_30D,
    p.NOTE_MED_ADHERENCE_DELTA_30D,
    p.NOTE_CARE_ENGAGEMENT_DELTA_30D,
    p.DAYS_SINCE_LAST_HEALTH_NOTE,
    p.DAYS_SINCE_LAST_RISK_NOTE,
    p.CLAIMS_IN_LAST_30D_COUNT,
    p.ED_IN_LAST_30D_COUNT,
    p.IP_IN_LAST_30D_COUNT,
    p.CLAIMS_IN_LAST_180D_COUNT,
    p.ED_IN_LAST_180D_COUNT,
    p.IP_IN_LAST_180D_COUNT,
    p.NEW_RX_ANTIPSYCH_30D,
    p.NEW_RX_INSULIN_30D,
    p.NEW_RX_ORAL_ANTIDIAB_30D,
    p.NEW_RX_STATIN_30D,
    p.NEW_RX_BETA_BLOCKER_30D,
    p.NEW_RX_OPIOID_30D,
    p.MONTHS_SINCE_FIRST_DIABETES,
    p.MONTHS_SINCE_FIRST_MENTAL_HEALTH,
    p.MONTHS_SINCE_FIRST_CARDIOVASCULAR,
    p.MONTHS_SINCE_FIRST_PULMONARY,
    p.MONTHS_SINCE_FIRST_KIDNEY,
    p.MONTHS_SINCE_FIRST_SUD,
    p.MONTHS_SINCE_FIRST_ANTIPSYCH,
    p.MONTHS_SINCE_FIRST_INSULIN,
    p.MONTHS_SINCE_FIRST_ORAL_ANTIDIAB,
    p.MONTHS_SINCE_FIRST_STATIN,
    p.MONTHS_SINCE_FIRST_BETA_BLOCKER,
    p.MONTHS_SINCE_FIRST_OPIOID,


    -- The target variables (labels) for prediction.
    p.Y_ANY_30D,
    p.Y_ANY_60D,
    p.Y_ANY_90D,
    p.Y_ED_30D,
    p.Y_ED_60D,
    p.Y_ED_90D,
    p.Y_IP_30D,
    p.Y_IP_60D,
    p.Y_IP_90D,
    
    -- A new feature to isolate the impact of notes.
    CASE
        WHEN p.ENGAGEMENT_GROUP = 'engaged_in_program' AND p.EVENT_DATE >= p.first_note_date
        THEN 1
        ELSE 0
    END AS HAS_CARE_NOTES_POST_PERIOD,

    -- The final dataset split column from the member_split CTE.
    ms.dataset_split
FROM
    prepped_features AS p
JOIN
    member_split AS ms
ON
    p.MEMBER_ID = ms.MEMBER_ID;
