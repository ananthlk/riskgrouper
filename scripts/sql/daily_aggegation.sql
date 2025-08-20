/*
============================================================================
PRODUCTION-READY: Creation of EVENTS_DAILY_AGG
PURPOSE: This script aggregates event-level data and raw pharmacy claims into a
         comprehensive, point-in-time correct, member-day table. This is the
         final data preparation step before a model is trained.

Key Features:
- **Hybrid Pharmacy Logic**:
  1. Aggregates simple pharmacy fill events from `EVENTS_WITH_LABELS_RX` into rolling counts (e.g., `cnt_fills_antipsychotic_90d`).
  2. Independently performs a recursive, point-in-time correct "days-in-hand" calculation directly from the raw pharmacy claims table.
- Aggregates all other event-level data (claims, notes) using rolling lookback windows (90/180 days) that respect data availability lags.
- Creates predictive features, including specific HCC flags and inconsistency scores.
- Joins final look-ahead labels for model training.

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
SET MEDICAL_CLAIM_LAG_MONTHS = 4;
SET PHARMACY_CLAIM_LAG_MONTHS = 2;

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
WITH RECURSIVE
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
-- UPDATED: This CTE now includes point-in-time correct rolling lookbacks.
day_claims AS (
    WITH claims_with_lag AS (
        -- Pre-filter claims based on the event date they are being joined to.
        -- This ensures point-in-time correctness.
        SELECT
            b1.member_id,
            b1.event_date AS anchor_date,
            b2.event_date AS claim_date,
            b2.paid_amount,
            b2.is_ed_event,
            b2.is_ip_event,
            b2.cnt_hcc_diabetes,
            b2.cnt_hcc_mental_health,
            b2.cnt_hcc_cardiovascular,
            b2.cnt_hcc_pulmonary,
            b2.cnt_hcc_kidney,
            b2.cnt_hcc_sud,
            b2.hcc_category,
            b2.hcpcs_category_short
        FROM idx b1 -- The timeline of member-days
        JOIN base b2 -- The stream of claim events
            ON b1.member_id = b2.member_id
            AND b2.event_type LIKE 'CLAIM_%'
            -- Join condition: claim must be before the anchor date
            AND b2.event_date < b1.anchor_date
            -- Point-in-Time Lag: claim must be old enough to be available
            AND b2.event_date <= DATEADD('month', -$MEDICAL_CLAIM_LAG_MONTHS, b1.anchor_date)
    )
    SELECT
        member_id,
        anchor_date AS event_date,
        -- 90-Day Lookback Features
        SUM(IFF(claim_date >= DATEADD(day, -90, anchor_date), paid_amount, 0)) AS paid_sum_90d,
        COUNT(DISTINCT IFF(is_ed_event AND claim_date >= DATEADD(day, -90, anchor_date), claim_date, NULL)) AS cnt_ed_visits_90d,
        COUNT(DISTINCT IFF(is_ip_event AND claim_date >= DATEADD(day, -90, anchor_date), claim_date, NULL)) AS cnt_ip_visits_90d,
        SUM(IFF(cnt_hcc_diabetes > 0 AND claim_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_hcc_diabetes_90d,
        SUM(IFF(cnt_hcc_mental_health > 0 AND claim_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_hcc_mental_health_90d,
        SUM(IFF(cnt_hcc_cardiovascular > 0 AND claim_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_hcc_cardiovascular_90d,
        SUM(IFF(cnt_hcc_pulmonary > 0 AND claim_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_hcc_pulmonary_90d,
        SUM(IFF(cnt_hcc_kidney > 0 AND claim_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_hcc_kidney_90d,
        SUM(IFF(cnt_hcc_sud > 0 AND claim_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_hcc_sud_90d,
        COUNT(IFF(hcc_category IS NOT NULL AND claim_date >= DATEADD(day, -90, anchor_date), 1, NULL)) AS cnt_any_hcc_90d,
        COUNT(IFF(hcpcs_category_short = 'psychotherapy' AND claim_date >= DATEADD(day, -90, anchor_date), 1, NULL)) AS cnt_proc_psychotherapy_90d,
        COUNT(IFF(hcpcs_category_short = 'psychiatric_evals' AND claim_date >= DATEADD(day, -90, anchor_date), 1, NULL)) AS cnt_proc_psychiatric_evals_90d,

        -- 180-Day Lookback Features
        SUM(IFF(claim_date >= DATEADD(day, -180, anchor_date), paid_amount, 0)) AS paid_sum_180d,
        COUNT(DISTINCT IFF(is_ed_event AND claim_date >= DATEADD(day, -180, anchor_date), claim_date, NULL)) AS cnt_ed_visits_180d,
        COUNT(DISTINCT IFF(is_ip_event AND claim_date >= DATEADD(day, -180, anchor_date), claim_date, NULL)) AS cnt_ip_visits_180d,
        SUM(IFF(cnt_hcc_diabetes > 0 AND claim_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_hcc_diabetes_180d,
        SUM(IFF(cnt_hcc_mental_health > 0 AND claim_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_hcc_mental_health_180d,
        SUM(IFF(cnt_hcc_cardiovascular > 0 AND claim_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_hcc_cardiovascular_180d,
        SUM(IFF(cnt_hcc_pulmonary > 0 AND claim_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_hcc_pulmonary_180d,
        SUM(IFF(cnt_hcc_kidney > 0 AND claim_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_hcc_kidney_180d,
        SUM(IFF(cnt_hcc_sud > 0 AND claim_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_hcc_sud_180d,
        COUNT(IFF(hcc_category IS NOT NULL AND claim_date >= DATEADD(day, -180, anchor_date), 1, NULL)) AS cnt_any_hcc_180d,
        COUNT(IFF(hcpcs_category_short = 'psychotherapy' AND claim_date >= DATEADD(day, -180, anchor_date), 1, NULL)) AS cnt_proc_psychotherapy_180d,
        COUNT(IFF(hcpcs_category_short = 'psychiatric_evals' AND claim_date >= DATEADD(day, -180, anchor_date), 1, NULL)) AS cnt_proc_psychiatric_evals_180d
    FROM claims_with_lag
    GROUP BY 1, 2
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
-- 8) day_rx_fills CTE: Aggregates pharmacy fill events from the base table.
day_rx_fills AS (
    WITH fills_with_lag AS (
        SELECT
            b1.member_id,
            b1.event_date AS anchor_date,
            b2.event_date AS fill_date,
            b2.is_fill_antipsychotic,
            b2.is_fill_insulin,
            b2.is_fill_oral_antidiab,
            b2.is_fill_statin,
            b2.is_fill_beta_blocker,
            b2.is_fill_opioid
        FROM idx b1
        JOIN base b2
            ON b1.member_id = b2.member_id
            AND b2.event_type = 'PHARMACY_FILL'
            AND b2.event_date < b1.anchor_date
            AND b2.event_date <= DATEADD('month', -$PHARMACY_CLAIM_LAG_MONTHS, b1.anchor_date)
    )
    SELECT
        member_id,
        anchor_date AS event_date,
        -- 90-Day Lookback Features
        SUM(IFF(is_fill_antipsychotic AND fill_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_fills_antipsychotic_90d,
        SUM(IFF(is_fill_insulin AND fill_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_fills_insulin_90d,
        SUM(IFF(is_fill_oral_antidiab AND fill_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_fills_oral_antidiab_90d,
        SUM(IFF(is_fill_statin AND fill_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_fills_statin_90d,
        SUM(IFF(is_fill_beta_blocker AND fill_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_fills_beta_blocker_90d,
        SUM(IFF(is_fill_opioid AND fill_date >= DATEADD(day, -90, anchor_date), 1, 0)) AS cnt_fills_opioid_90d,
        -- 180-Day Lookback Features
        SUM(IFF(is_fill_antipsychotic AND fill_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_fills_antipsychotic_180d,
        SUM(IFF(is_fill_insulin AND fill_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_fills_insulin_180d,
        SUM(IFF(is_fill_oral_antidiab AND fill_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_fills_oral_antidiab_180d,
        SUM(IFF(is_fill_statin AND fill_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_fills_statin_180d,
        SUM(IFF(is_fill_beta_blocker AND fill_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_fills_beta_blocker_180d,
        SUM(IFF(is_fill_opioid AND fill_date >= DATEADD(day, -180, anchor_date), 1, 0)) AS cnt_fills_opioid_180d
    FROM fills_with_lag
    GROUP BY 1, 2
),

-- 9) RX Days-in-Hand Calculation (Self-Contained Logic)
-- This block reads from the raw pharmacy table to calculate continuous medication coverage.
rx_fills AS (
 SELECT
 r.FH_ID AS member_id,
 TRY_TO_DATE(r.FILLED_DATE) AS filled_date,
 COALESCE(
 NULLIF(r.NDC_CODE_11_DIGIT,''),
 NULLIF(r.NDC_CODE,''),
 NULLIF(r.PRIMARY_AGENT_DESC,''),
 NULLIF(r.ACTIVE_INGREDIENTS_NAME,'')
 ) AS drug_key,
 r.DAYS_SUPPLY AS days_supply,
 IFF(COALESCE(r.IS_FH_ANTIPSYCHOTIC, FALSE) OR COALESCE(r.IS_ANTIPSYCH_MED, FALSE), TRUE, FALSE) AS cohort_antipsych,
 COALESCE(r.IS_INSULIN, FALSE) AS cohort_insulin,
 COALESCE(r.IS_ORAL_ANTIDIABETIC, FALSE) AS cohort_oral_antidiab,
 COALESCE(r.IS_STATIN, FALSE) AS cohort_statin,
 COALESCE(r.IS_BETA_BLOCKER, FALSE) AS cohort_beta_blocker,
 COALESCE(r.IS_OPIATE_AGONISTS, FALSE) AS cohort_opioid
 FROM IDENTIFIER($PHARMACY_CLAIMS_FQN) r
 WHERE TRY_TO_DATE(r.FILLED_DATE) IS NOT NULL
 AND r.DAYS_SUPPLY > 0
 AND drug_key IS NOT NULL
),
rx_seq AS (
 SELECT
 member_id, drug_key, filled_date, days_supply,
 DATEADD('day', days_supply - 1, filled_date) AS naive_runout,
 cohort_antipsych, cohort_insulin, cohort_oral_antidiab, cohort_statin, cohort_beta_blocker, cohort_opioid,
 ROW_NUMBER() OVER (PARTITION BY member_id, drug_key ORDER BY filled_date) AS rn
 FROM rx_fills
),
rx_chain AS (
 SELECT
 member_id, drug_key, rn, filled_date, days_supply,
 naive_runout AS episode_end,
 cohort_antipsych, cohort_insulin, cohort_oral_antidiab, cohort_statin, cohort_beta_blocker, cohort_opioid
 FROM rx_seq
 WHERE rn = 1
 UNION ALL
 SELECT
 n.member_id, n.drug_key, n.rn, n.filled_date, n.days_supply,
 CASE
 WHEN n.filled_date <= DATEADD('day', 1, p.episode_end)
 THEN DATEADD('day', n.days_supply, p.episode_end)
 ELSE DATEADD('day', n.days_supply - 1, n.filled_date)
 END AS episode_end,
 n.cohort_antipsych, n.cohort_insulin, n.cohort_oral_antidiab, n.cohort_statin, n.cohort_beta_blocker, n.cohort_opioid
 FROM rx_chain p
 JOIN rx_seq n
 ON n.member_id = p.member_id
 AND n.drug_key = p.drug_key
 AND n.rn = p.rn + 1
),
rx_chain_latest AS (
 SELECT *
 FROM (
 SELECT
 member_id, drug_key, rn, filled_date, days_supply, episode_end,
 cohort_antipsych, cohort_insulin, cohort_oral_antidiab, cohort_statin, cohort_beta_blocker, cohort_opioid,
 ROW_NUMBER() OVER (PARTITION BY member_id, drug_key, rn ORDER BY episode_end DESC) AS rnk
 FROM rx_chain
 )
 WHERE rnk = 1
),
rx_chain_with_prev AS (
 SELECT
 c.*,
 LAG(c.episode_end) OVER (PARTITION BY c.member_id, c.drug_key ORDER BY c.rn) AS prev_episode_end
 FROM rx_chain_latest c
),
rx_episodes AS (
    SELECT
        *,
        SUM(start_flag) OVER (PARTITION BY member_id, drug_key ORDER BY rn) AS episode_id
    FROM (
        SELECT
            *,
            IFF(rn = 1 OR filled_date > DATEADD('day', 1, prev_episode_end), 1, 0) AS start_flag
        FROM rx_chain_with_prev
    )
),
rx_episode_bounds AS (
    SELECT
        member_id,
        drug_key,
        episode_id,
        MIN(filled_date) AS coverage_start,
        MAX(episode_end) AS coverage_end,
        MAX(IFF(cohort_antipsych, 1, 0)) AS epi_antipsych,
        MAX(IFF(cohort_insulin, 1, 0)) AS epi_insulin,
        MAX(IFF(cohort_oral_antidiab, 1, 0)) AS epi_oral_antidiab,
        MAX(IFF(cohort_statin, 1, 0)) AS epi_statin,
        MAX(IFF(cohort_beta_blocker, 1, 0)) AS epi_beta_blocker,
        MAX(IFF(cohort_opioid, 1, 0)) AS epi_opioid
    FROM rx_episodes
    GROUP BY member_id, drug_key, episode_id
),
day_rx_days_in_hand AS (
    SELECT
        i.member_id,
        i.event_date AS anchor_date,
        MAX( IFF(i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_any,
        MAX( IFF(r.epi_antipsych = 1 AND i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_antipsych,
        MAX( IFF(r.epi_insulin = 1 AND i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_insulin,
        MAX( IFF(r.epi_oral_antidiab = 1 AND i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_oral_antidiab,
        MAX( IFF(r.epi_statin = 1 AND i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_statin,
        MAX( IFF(r.epi_beta_blocker = 1 AND i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_beta_blocker,
        MAX( IFF(r.epi_opioid = 1 AND i.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', i.event_date, r.coverage_end) + 1, 0) ) AS rx_days_opioid
    FROM idx i
    LEFT JOIN rx_episode_bounds r
        ON i.member_id = r.member_id
        -- Point-in-Time Lag: The start of the medication episode must be old enough to be available
        AND r.coverage_start <= DATEADD('month', -$PHARMACY_CLAIM_LAG_MONTHS, i.event_date)
    GROUP BY 1, 2
),

-- NEW) day_realtime_events CTE: Aggregates event flags from non-claim, real-time sources.
-- These sources (ADT, Auth, Health Check) have no data lag.
day_realtime_events AS (
    WITH realtime_events AS (
        -- Select real-time events directly, as there is no lag to consider.
        SELECT
            b1.member_id,
            b1.event_date AS anchor_date,
            b2.event_date AS event_date,
            b2.is_ip_adt,
            b2.is_ip_auth,
            b2.is_ip_hc,
            b2.is_ed_adt,
            b2.is_ed_auth,
            b2.is_ed_hc
        FROM idx b1
        JOIN base b2
            ON b1.member_id = b2.member_id
            AND b2.event_date < b1.anchor_date
            -- No lag for these sources
            AND b2.event_type IN ('ADT_EVENT', 'AUTH_EVENT', 'HEALTH_CHECK_EVENT')
    )
    SELECT
        member_id,
        anchor_date AS event_date,
        -- 90-Day Lookback Features
        COUNT(DISTINCT IFF(is_ip_adt AND event_date >= DATEADD(day, -90, anchor_date), event_date, NULL)) AS cnt_ip_adt_90d,
        COUNT(DISTINCT IFF(is_ip_auth AND event_date >= DATEADD(day, -90, anchor_date), event_date, NULL)) AS cnt_ip_auth_90d,
        COUNT(DISTINCT IFF(is_ip_hc AND event_date >= DATEADD(day, -90, anchor_date), event_date, NULL)) AS cnt_ip_hc_90d,
        COUNT(DISTINCT IFF(is_ed_adt AND event_date >= DATEADD(day, -90, anchor_date), event_date, NULL)) AS cnt_ed_adt_90d,
        COUNT(DISTINCT IFF(is_ed_auth AND event_date >= DATEADD(day, -90, anchor_date), event_date, NULL)) AS cnt_ed_auth_90d,
        COUNT(DISTINCT IFF(is_ed_hc AND event_date >= DATEADD(day, -90, anchor_date), event_date, NULL)) AS cnt_ed_hc_90d,
        -- 180-Day Lookback Features
        COUNT(DISTINCT IFF(is_ip_adt AND event_date >= DATEADD(day, -180, anchor_date), event_date, NULL)) AS cnt_ip_adt_180d,
        COUNT(DISTINCT IFF(is_ip_auth AND event_date >= DATEADD(day, -180, anchor_date), event_date, NULL)) AS cnt_ip_auth_180d,
        COUNT(DISTINCT IFF(is_ip_hc AND event_date >= DATEADD(day, -180, anchor_date), event_date, NULL)) AS cnt_ip_hc_180d,
        COUNT(DISTINCT IFF(is_ed_adt AND event_date >= DATEADD(day, -180, anchor_date), event_date, NULL)) AS cnt_ed_adt_180d,
        COUNT(DISTINCT IFF(is_ed_auth AND event_date >= DATEADD(day, -180, anchor_date), event_date, NULL)) AS cnt_ed_auth_180d,
        COUNT(DISTINCT IFF(is_ed_hc AND event_date >= DATEADD(day, -180, anchor_date), event_date, NULL)) AS cnt_ed_hc_180d
    FROM realtime_events
    GROUP BY 1, 2
),

-- 10) day_labels CTE: Aggregates all look-ahead labels to a member-day level.
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
-- 11) day_features CTE: Combines daily-level features into a single CTE.
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
        -- Rolling Claim Features (90d)
        c.paid_sum_90d,
        -- Combined ED/IP Counts (Claims + Real-time)
        (c.cnt_ed_visits_90d + rt.cnt_ed_adt_90d + rt.cnt_ed_auth_90d + rt.cnt_ed_hc_90d) AS cnt_ed_visits_90d,
        (c.cnt_ip_visits_90d + rt.cnt_ip_adt_90d + rt.cnt_ip_auth_90d + rt.cnt_ip_hc_90d) AS cnt_ip_visits_90d,
        c.cnt_hcc_diabetes_90d,
        c.cnt_hcc_mental_health_90d,
        c.cnt_hcc_cardiovascular_90d,
        c.cnt_hcc_pulmonary_90d,
        c.cnt_hcc_kidney_90d,
        c.cnt_hcc_sud_90d,
        c.cnt_any_hcc_90d,
        c.cnt_proc_psychotherapy_90d,
        c.cnt_proc_psychiatric_evals_90d,
        -- Rolling Claim Features (180d)
        c.paid_sum_180d,
        -- Combined ED/IP Counts (Claims + Real-time)
        (c.cnt_ed_visits_180d + rt.cnt_ed_adt_180d + rt.cnt_ed_auth_180d + rt.cnt_ed_hc_180d) AS cnt_ed_visits_180d,
        (c.cnt_ip_visits_180d + rt.cnt_ip_adt_180d + rt.cnt_ip_auth_180d + rt.cnt_ip_hc_180d) AS cnt_ip_visits_180d,
        c.cnt_hcc_diabetes_180d,
        c.cnt_hcc_mental_health_180d,
        c.cnt_hcc_cardiovascular_180d,
        c.cnt_hcc_pulmonary_180d,
        c.cnt_hcc_kidney_180d,
        c.cnt_hcc_sud_180d,
        c.cnt_any_hcc_180d,
        c.cnt_proc_psychotherapy_180d,
        c.cnt_proc_psychiatric_evals_180d,
        -- Real-time Event Source Features (90d)
        rt.cnt_ip_adt_90d,
        rt.cnt_ip_auth_90d,
        rt.cnt_ip_hc_90d,
        rt.cnt_ed_adt_90d,
        rt.cnt_ed_auth_90d,
        rt.cnt_ed_hc_90d,
        -- Real-time Event Source Features (180d)
        rt.cnt_ip_adt_180d,
        rt.cnt_ip_auth_180d,
        rt.cnt_ip_hc_180d,
        rt.cnt_ed_adt_180d,
        rt.cnt_ed_auth_180d,
        rt.cnt_ed_hc_180d,
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
        -- RX Fill Count Features (90d)
        rf.cnt_fills_antipsychotic_90d,
        rf.cnt_fills_insulin_90d,
        rf.cnt_fills_oral_antidiab_90d,
        rf.cnt_fills_statin_90d,
        rf.cnt_fills_beta_blocker_90d,
        rf.cnt_fills_opioid_90d,
        -- RX Fill Count Features (180d)
        rf.cnt_fills_antipsychotic_180d,
        rf.cnt_fills_insulin_180d,
        rf.cnt_fills_oral_antidiab_180d,
        rf.cnt_fills_statin_180d,
        rf.cnt_fills_beta_blocker_180d,
        rf.cnt_fills_opioid_180d,
        -- RX Days-in-Hand Features
        rd.rx_days_any,
        rd.rx_days_antipsych,
        rd.rx_days_insulin,
        rd.rx_days_oral_antidiab,
        rd.rx_days_statin,
        rd.rx_days_beta_blocker,
        rd.rx_days_opioid,
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
    LEFT JOIN day_rx_fills rf ON rf.member_id = i.member_id AND rf.event_date = i.event_date
    LEFT JOIN day_rx_days_in_hand rd ON rd.member_id = i.member_id AND rd.anchor_date = i.event_date
    LEFT JOIN day_realtime_events rt ON rt.member_id = i.member_id AND rt.event_date = i.event_date
    LEFT JOIN day_labels l ON l.member_id = i.member_id AND l.event_date = i.event_date
    LEFT JOIN day_outpatient_lookback lb ON lb.member_id = i.member_id AND lb.event_date = i.event_date
),
-- 12) inconsistency_features CTE: Creates rolling features for non-compliance.
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
-- 13) Final SELECT: This is the main join of all CTEs. It ensures one row per member-day.
SELECT
    d.*,
    i.inconsistency_med_noncompliance,
    i.inconsistency_appt_noncompliance
FROM day_features d
LEFT JOIN inconsistency_features i
ON d.member_id = i.member_id AND d.event_date = i.event_date;
