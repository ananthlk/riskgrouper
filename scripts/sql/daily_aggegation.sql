/*
============================================================================
PRODUCTION-READY: Creation of EVENTS_DAILY_AGG
PURPOSE: This script aggregates event-level data from the 'EVENTS_WITH_LABELS_RX'
         table into a single, comprehensive member-day aggregate. This is the
         final data preparation step before a model is trained.

Key Features:
- Aggregates all event-level data (claims, notes, RX) to a single row
  per member per day to prevent duplicates.
- Creates predictive features, including specific HCC flags and inconsistency scores.
- Generates 30/60/90-day look-ahead labels for model training.

Assumptions:
- The 'EVENTS_WITH_LABELS_RX' table is a pre-existing intermediate table.
- All FQN (Fully Qualified Name) variables are set in the session.
============================================================================
*/

-- === 0) Source FQNs (Full Qualified Names) ===
-- These variables define the location of all source tables.
SET MEMBERLIST_FQN = 'TRANSFORMED_DATA.PROD.FH_MEMBERS';
SET MEMBER_QUAL_FQN = 'TRANSFORMED_DATA.prod.fh_member_selection_qualification';
SET CLAIM_LINES_FQN = 'TRANSFORMED_DATA.PROD_CORE.CORE_MEDICAL_CLAIM_LINES';
SET CLAIM_DIAGNOSIS_FQN = 'TRANSFORMED_DATA.PROD_TRANSFORM.CORE_MEDICAL_CLAIM_DIAGNOSIS_LINE_ITEMS';
SET STRAT_NOTES_FQN = 'TRANSFORMED_DATA._TEMP.AL_AI_BASELINE_SCORING';
SET PHARMACY_CLAIMS_FQN = 'TRANSFORMED_DATA.PROD_CORE.CORE_PHARMACY_CLAIMS';
SET HCPCS_CATEGORIES_FQN = 'TRANSFORMED_DATA.DBT_SLOPEZ_BASE.HCPCS_CATEGORIES';
SET HCC_MAPPING_FQN = 'TRANSFORMED_DATA.PROD_STAGING.DIM_ICD_CMS_HCC_MAP';

-- === 0a) Targets ===
-- Set the database and schema where the final table will be created.
SET TARGET_DB = 'TRANSFORMED_DATA';
SET TARGET_SCHEMA = '_TEMP';

USE DATABASE IDENTIFIER($TARGET_DB);
USE SCHEMA IDENTIFIER($TARGET_SCHEMA);

/*
============================================================================
1) Build TEMP table: EVENTS_DAILY_AGG
This is the main body of the query, which uses a series of CTEs to aggregate
and prepare the data.
============================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG AS
WITH
-- 1) Members CTE: Extracts member demographic and engagement information.
-- This CTE is a pre-filter for the cohort of interest.
members AS (
 SELECT
 A.FH_ID AS member_id,
 A.MARKET AS market,
 A.PAT_DATE_OF_BIRTH AS dob,
 A.PAT_GENDER AS gender,
 -- Member engagement and selection status flags.
 A.has_ever_been_engaged,
 A.is_batched,
 -- Normalize coverage category to handle variations in source data.
 CASE UPPER(TRIM(A.fh_coverage_category))
 WHEN 'ABD W/SMI' THEN 'ABD'
 WHEN 'TANF W/SMI' THEN 'TANF'
 ELSE UPPER(TRIM(A.fh_coverage_category))
 END AS normalized_coverage_category,
 -- Calculates months since the member was batched into the program.
 DATEDIFF(month, A.batch_date, CURRENT_DATE) AS months_since_batched
 FROM IDENTIFIER($MEMBERLIST_FQN) A
 LEFT OUTER JOIN IDENTIFIER($MEMBER_QUAL_FQN) c ON a.fh_id = c.fh_id
 WHERE c.is_fh_clinically_qualified = 1
 AND UPPER(TRIM(A.fh_coverage_category)) NOT IN ('NULL', 'EXCLUDE')
),

-- 2) Base CTE: Serves as the starting point for aggregation by pulling from
-- the pre-built EVENTS_WITH_LABELS_RX intermediate table.
base AS (
 SELECT * FROM TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX
),
-- 3) member_groups CTE: Assigns a categorical engagement group to each member.
member_groups AS (
 SELECT DISTINCT
 member_id,
 CASE
 WHEN has_ever_been_engaged = 1 THEN 'engaged_in_program'
 WHEN is_batched = 1 THEN 'selected_not_engaged'
 ELSE 'not_selected_for_engagement'
 END AS engagement_group
 FROM members
),
-- 4) idx CTE: Creates a unique index of every member-day combination in the dataset.
-- This is crucial for the final joins to prevent reintroducing duplicates.
idx AS (
 SELECT DISTINCT member_id, event_date FROM base
),
-- 5) day_outpatient_lookback CTE: Calculates a 60-day outpatient claim count
-- using a window function, which is useful for 'inconsistency' features.
day_outpatient_lookback AS (
 SELECT
 member_id,
 event_date,
 COUNT(event_type) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS cnt_outpatient_60d
 FROM base
 WHERE NOT (is_ed_event OR is_ip_event) AND event_type LIKE 'CLAIM_%'
),
-- 6) day_claims CTE: Aggregates all claim-related features to a member-day level.
day_claims AS (
 SELECT
 member_id,
 event_date,
 COUNT_IF(event_type = 'CLAIM_DIAGNOSIS') AS cnt_claim_dx,
 COUNT_IF(event_type = 'CLAIM_PROCEDURE') AS cnt_claim_proc,
 COUNT_IF(event_type = 'CLAIM_REVENUE') AS cnt_claim_rev,
 COUNT_IF(event_type LIKE 'CLAIM_%') AS cnt_claim_events,
 MAX(IFF(is_ed_event,1,0)) AS any_ed_on_date,
 MAX(IFF(is_ip_event,1,0)) AS any_ip_on_date,
 SUM(COALESCE(paid_amount,0)) AS paid_sum,
 -- Aggregates specific HCC flags.
 SUM(CNT_HCC_DIABETES) AS CNT_HCC_DIABETES,
 SUM(CNT_HCC_MENTAL_HEALTH) AS CNT_HCC_MENTAL_HEALTH,
 SUM(CNT_HCC_CARDIOVASCULAR) AS CNT_HCC_CARDIOVASCULAR,
 SUM(CNT_HCC_PULMONARY) AS CNT_HCC_PULMONARY,
 SUM(CNT_HCC_KIDNEY) AS CNT_HCC_KIDNEY,
 SUM(CNT_HCC_SUD) AS CNT_HCC_SUD,
 SUM(IFF(hcc_category IS NOT NULL,1,0)) AS cnt_any_hcc,
 -- Aggregates procedures by category.
 COUNT_IF(hcpcs_category_short = 'psychotherapy') AS cnt_proc_psychotherapy,
 COUNT_IF(hcpcs_category_short = 'psychiatric_evals') AS cnt_proc_psychiatric_evals
 FROM base
 WHERE event_type LIKE 'CLAIM_%'
 GROUP BY member_id, event_date
),
-- 7) day_notes CTE: Aggregates all note-related features to a member-day level.
day_notes AS (
 SELECT
 member_id,
 event_date,
 MAX(IFF(event_type='CARE_NOTE' AND code='HEALTH', score, NULL)) AS note_health_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='RISK_HARM', score, NULL)) AS note_risk_harm_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='SOCIAL_STABILITY', score, NULL)) AS note_social_stab_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='MED_ADHERENCE', score, NULL)) AS note_med_adherence_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='CARE_ENGAGEMENT', score, NULL)) AS note_care_engagement_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='PROGRAM_TRUST', score, NULL)) AS note_program_trust_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='SELF', score, NULL)) AS note_self_score,
 MAX(IFF(event_type='CARE_NOTE' AND code='HEALTH', market_baseline, NULL)) AS health_market_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='HEALTH', population_baseline, NULL)) AS health_pop_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='HEALTH', individual_baseline, NULL)) AS health_indiv_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='RISK_HARM', market_baseline, NULL)) AS riskharm_market_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='SOCIAL_STABILITY', market_baseline, NULL)) AS social_market_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='MED_ADHERENCE', market_baseline, NULL)) AS medadh_market_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='CARE_ENGAGEMENT', market_baseline, NULL)) AS careeng_market_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='PROGRAM_TRUST', market_baseline, NULL)) AS progtrust_market_baseline,
 MAX(IFF(event_type='CARE_NOTE' AND code='SELF', market_baseline, NULL)) AS self_market_baseline
 FROM base
 GROUP BY member_id, event_date
),
-- 8) day_rx CTE: Aggregates all prescription-related features to a member-day level.
day_rx AS (
 SELECT
 member_id,
 event_date,
 MAX(rx_days_any) AS rx_days_any,
 MAX(rx_days_antipsych) AS rx_days_antipsych,
 MAX(rx_days_insulin) AS rx_days_insulin,
 MAX(rx_days_oral_antidiab) AS rx_days_oral_antidiab,
 MAX(rx_days_statin) AS rx_days_statin,
 MAX(rx_days_beta_blocker) AS rx_days_beta_blocker,
 MAX(rx_days_opioid) AS rx_days_opioid
 FROM base
 GROUP BY member_id, event_date
),
-- 9) day_labels CTE: Aggregates all look-ahead labels to a member-day level.
day_labels AS (
 SELECT
 member_id,
 event_date,
 MAX(y_ed_30d) AS y_ed_30d,
 MAX(y_ed_60d) AS y_ed_60d,
 MAX(y_ed_90d) AS y_ed_90d,
 MAX(y_ip_30d) AS y_ip_30d,
 MAX(y_ip_60d) AS y_ip_60d,
 MAX(y_ip_90d) AS y_ip_90d,
 MAX(y_any_30d) AS y_any_30d,
 MAX(y_any_60d) AS y_any_60d,
 MAX(y_any_90d) AS y_any_90d
 FROM base
 GROUP BY member_id, event_date
),
-- 10) day_features CTE: Combines daily-level features into a single CTE.
day_features AS (
    SELECT
        i.member_id,
        i.event_date,
        -- Correctly join member data
        m.dob,
        m.gender,
        g.engagement_group,
        m.normalized_coverage_category,
        m.months_since_batched,
        -- Claims features
        c.cnt_claim_dx,
        c.cnt_claim_proc,
        c.cnt_claim_rev,
        c.cnt_claim_events,
        c.any_ed_on_date,
        c.any_ip_on_date,
        c.paid_sum,
        c.cnt_hcc_diabetes,
        c.cnt_hcc_mental_health,
        c.cnt_hcc_cardiovascular,
        c.cnt_hcc_pulmonary,
        c.cnt_hcc_kidney,
        c.cnt_hcc_sud,
        c.cnt_any_hcc,
        c.cnt_proc_psychotherapy,
        c.cnt_proc_psychiatric_evals,
        -- Note features
        n.note_health_score,
        n.note_risk_harm_score,
        n.note_social_stab_score,
        n.note_med_adherence_score,
        n.note_care_engagement_score,
        n.note_program_trust_score,
        n.note_self_score,
        n.health_pop_baseline,
        n.health_indiv_baseline,
        n.health_market_baseline,
        n.riskharm_market_baseline,
        n.social_market_baseline,
        n.medadh_market_baseline,
        n.careeng_market_baseline,
        n.progtrust_market_baseline,
        n.self_market_baseline,
        -- RX features
        r.rx_days_any,
        r.rx_days_antipsych,
        r.rx_days_insulin,
        r.rx_days_oral_antidiab,
        r.rx_days_statin,
        r.rx_days_beta_blocker,
        r.rx_days_opioid,
        -- Outpatient lookback for inconsistency features
        lb.cnt_outpatient_60d,
        -- Labels
        l.y_ed_30d, l.y_ed_60d, l.y_ed_90d,
        l.y_ip_30d, l.y_ip_60d, l.y_ip_90d,
        l.y_any_30d, l.y_any_60d, l.y_any_90d
    FROM idx i
    LEFT JOIN members m ON m.member_id = i.member_id
    LEFT JOIN member_groups g ON g.member_id = i.member_id
    LEFT JOIN day_claims c ON c.member_id = i.member_id AND c.event_date = i.event_date
    LEFT JOIN day_notes n ON n.member_id = i.member_id AND n.event_date = i.event_date
    LEFT JOIN day_rx r ON r.member_id = i.member_id AND r.event_date = i.event_date
    LEFT JOIN day_labels l ON l.member_id = i.member_id AND l.event_date = i.event_date
    LEFT JOIN day_outpatient_lookback lb ON lb.member_id = i.member_id AND lb.event_date = i.event_date
),
-- 11) inconsistency_features CTE: Creates rolling features for non-compliance.
inconsistency_features AS (
    SELECT
        member_id,
        event_date,
        -- Inconsistency Feature 1: Medication Non-adherence (60-day window)
        -- Checks for high med adherence scores from notes vs. low RX days-in-hand.
        MAX(CASE WHEN (note_med_adherence_score IS NOT NULL AND note_med_adherence_score > 0.5) AND (rx_days_antipsych IS NOT NULL AND rx_days_antipsych < 7) THEN 1 ELSE 0 END)
            OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS inconsistency_med_noncompliance,

        -- Inconsistency Feature 2: Appointment Non-compliance (60-day window)
        -- Checks for high care engagement scores from notes vs. zero recent outpatient visits.
        MAX(CASE WHEN (note_care_engagement_score IS NOT NULL AND note_care_engagement_score > 0.5) AND (cnt_outpatient_60d = 0) THEN 1 ELSE 0 END)
            OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS inconsistency_appt_noncompliance
    FROM day_features
)
-- 12) Final SELECT: This is the main join of all CTEs. It ensures one row per member-day.
SELECT
    d.* EXCLUDE (inconsistency_med_noncompliance, inconsistency_appt_noncompliance),
    i.inconsistency_med_noncompliance,
    i.inconsistency_appt_noncompliance
FROM day_features d
LEFT JOIN inconsistency_features i
ON d.member_id = i.member_id AND d.event_date = i.event_date;
