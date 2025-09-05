/*
============================================================================
PRODUCTION-READY: Creation of EVENTS_WITH_LABELS_RX
PURPOSE: This script transforms granular event data into a preliminary, event-level
         table with all necessary features, flags, and look-ahead labels. This table
         serves as the source for the final daily aggregation.

Key Features:
- Combines data from claims, notes, and pharmacy tables.
- Correctly joins to HCC V28 mapping to create specific disease flags.
- Uses recursive CTEs to calculate medication days-in-hand.
- Generates 30/60/90-day look-ahead labels for model training.

Assumptions:
- All FQN (Fully Qualified Name) variables are set in the session.
============================================================================
*/

-- === 0) Source FQNs (Full Qualified Name) ===
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
1) Build TEMP table: EVENTS_WITH_LABELS_RX
This CTE block prepares all event-level data before daily aggregation.
============================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX AS
WITH RECURSIVE
-- Members CTE: Extracts member demographic and engagement info for the cohort.
members AS (
 SELECT
 A.FH_ID AS member_id,
 A.MARKET AS market,
 A.PAT_DATE_OF_BIRTH AS dob,
 A.PAT_GENDER AS gender,
 A.has_ever_been_engaged,
 A.is_batched,
 -- Normalize coverage category to handle variations.
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

-- member_groups CTE: NEWLY ADDED to classify members into engagement groups.
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

-- Claim Lines CTE: Core claim line information.
lines AS (
 SELECT
 l.FH_ID AS member_id,
 l.URSA_CLAIM_ID AS claim_id,
 TRY_TO_DATE(l.CLAIM_START_DATE) AS service_date,
 TRY_TO_DATE(l.CLAIM_END_DATE) AS service_end_date,
 l.CLAIM_LINE_NUMBER AS claim_line_number,
 UPPER(TRIM(l.CMS_PLACE_OF_SERVICE_CODE)) AS place_of_service,
 UPPER(TRIM(l.CMS_REVENUE_CENTER_CODE)) AS revenue_code,
 UPPER(TRIM(l.HCPCS_CODE)) AS hcpcs_code,
 l.CLAIM_PLAN_PAID_AMOUNT AS paid_amount
 FROM IDENTIFIER($CLAIM_LINES_FQN) l
),

-- Diagnosis CTE: Joins with HCC mapping for clinical categorization.
diag_long AS (
 SELECT
 d.FH_ID AS member_id,
 d.URSA_CLAIM_ID AS claim_id,
 UPPER(TRIM(d.ICD10CM_CODE)) AS diagnosis_code,
 UPPER(TRIM('ICD-10')) AS diagnosis_code_type,
 IFF(d.ICD10CM_CODE IS NULL, NULL, SUBSTR(d.ICD10CM_CODE,1,3)) AS dx_prefix3,
 d.DX_LINE_NUMBER AS diagnosis_sequence,
 hcc.HCC_CODE AS hcc_category,
 -- Create specific HCC flags using V28 codes for accuracy.
 CASE WHEN hcc.HCC_CODE IN (36, 37, 38) THEN 1 ELSE 0 END AS CNT_HCC_DIABETES,
 CASE WHEN hcc.HCC_CODE IN (151, 152, 153, 154, 155) THEN 1 ELSE 0 END AS CNT_HCC_MENTAL_HEALTH,
 CASE WHEN hcc.HCC_CODE IN (222, 223, 224, 249, 263, 264) THEN 1 ELSE 0 END AS CNT_HCC_CARDIOVASCULAR,
 CASE WHEN hcc.HCC_CODE IN (277, 279, 280) THEN 1 ELSE 0 END AS CNT_HCC_PULMONARY,
 CASE WHEN hcc.HCC_CODE IN (326, 327, 328, 329) THEN 1 ELSE 0 END AS CNT_HCC_KIDNEY,
 CASE WHEN hcc.HCC_CODE IN (135, 136, 137, 139) THEN 1 ELSE 0 END AS CNT_HCC_SUD,
 CASE WHEN hcc.HCC_CODE IS NOT NULL AND hcc.HCC_CODE NOT IN (36, 37, 38, 151, 152, 153, 154, 155, 222, 223, 224, 249, 263, 264, 277, 279, 280, 326, 327, 328, 329, 135, 136, 137, 139) THEN 1 ELSE 0 END AS CNT_HCC_OTHER_COMPLEX
 FROM IDENTIFIER($CLAIM_DIAGNOSIS_FQN) d
 LEFT JOIN IDENTIFIER($HCC_MAPPING_FQN) hcc
 ON d.ICD10CM_CODE = hcc.DIAGNOSIS_CODE
 WHERE d.ICD10CM_CODE IS NOT NULL
 AND hcc.MODEL_VERSION = 'CMS-HCC-V28'
 AND hcc.PAYMENT_YEAR = 2025
),

-- Stratified Notes CTE: Unstructured data from care notes.
notes_long AS (
 SELECT
 n.FH_ID AS member_id,
 TRY_TO_DATE(n.SOURCE_INTERACTION_DATE) AS note_date,
 UPPER(TRIM(n.CATEGORY)) AS category,
 n.SCORE AS score,
 n.CONFIDENCE AS confidence,
 n.POPULATION_BASELINE AS population_baseline,
 n.MARKET_BASELINE AS market_baseline,
 n.INDIVIDUAL_BASELINE AS individual_baseline,
 n.EVIDENCE,
 n.COMBINED_NOTES,
 n.RAW_RESPONSE
 FROM IDENTIFIER($STRAT_NOTES_FQN) n
),

-- Pharmacy Fills CTE: Prepare pharmacy claims for Days-in-Hand calculation.
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
 r.QUANTITY_DISPENSED AS qty,
 IFF(COALESCE(r.IS_FH_ANTIPSYCHOTIC, FALSE) OR COALESCE(r.IS_ANTIPSYCH_MED, FALSE), TRUE, FALSE) AS cohort_antipsych,
 COALESCE(r.IS_INSULIN, FALSE) AS cohort_insulin,
 COALESCE(r.IS_ORAL_ANTIDIABETIC, FALSE) AS cohort_oral_antidiab,
 COALESCE(r.IS_STATIN, FALSE) AS cohort_statin,
 COALESCE(r.IS_BETA_BLOCKER, FALSE) AS cohort_beta_blocker,
 COALESCE(r.IS_OPIATE_AGONISTS, FALSE) AS cohort_opioid
 FROM IDENTIFIER($PHARMACY_CLAIMS_FQN) r
 WHERE TRY_TO_DATE(r.FILLED_DATE) IS NOT NULL
 AND r.DAYS_SUPPLY > 0
 AND COALESCE(
 NULLIF(r.NDC_CODE_11_DIGIT,''),
 NULLIF(r.NDC_CODE,''),
 NULLIF(r.PRIMARY_AGENT_DESC,''),
 NULLIF(r.ACTIVE_INGREDIENTS_NAME,'')
 ) IS NOT NULL
),

-- RX Fills (Recursive CTE): Calculates medication stockpiling (Days-in-Hand).
-- rx_seq: Ranks each prescription fill for a given member and drug.
rx_seq AS (
 SELECT
 member_id, drug_key, filled_date, days_supply,
 DATEADD('day', days_supply - 1, filled_date) AS naive_runout,
 cohort_antipsych, cohort_insulin, cohort_oral_antidiab, cohort_statin, cohort_beta_blocker, cohort_opioid,
 ROW_NUMBER() OVER (PARTITION BY member_id, drug_key ORDER BY filled_date) AS rn
 FROM rx_fills
),
-- rx_chain: A recursive CTE that chains prescription fills together to
-- calculate continuous coverage episodes.
rx_chain AS (
 -- Anchor: First fill of each medication for each member.
 SELECT
 member_id, drug_key, rn, filled_date, days_supply,
 naive_runout AS episode_end,
 cohort_antipsych, cohort_insulin, cohort_oral_antidiab, cohort_statin, cohort_beta_blocker, cohort_opioid
 FROM rx_seq
 WHERE rn = 1

 UNION ALL

 -- Recursive: Extends episode end if next fill is before or on the previous episode end.
 SELECT
 n.member_id,
 n.drug_key,
 n.rn,
 n.filled_date,
 n.days_supply,
 CASE
 WHEN n.filled_date <= DATEADD('day', 1, p.episode_end)
 THEN DATEADD('day', n.days_supply, p.episode_end) -- Stockpile
 ELSE DATEADD('day', n.days_supply - 1, n.filled_date) -- Restart
 END AS episode_end,
 n.cohort_antipsych, n.cohort_insulin, n.cohort_oral_antidiab, n.cohort_statin, n.cohort_beta_blocker, n.cohort_opioid
 FROM rx_chain p
 JOIN rx_seq n
 ON n.member_id = p.member_id
 AND n.drug_key = p.drug_key
 AND n.rn = p.rn + 1
),
-- rx_chain_latest: Selects the final episode end date for each fill.
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
-- rx_chain_with_prev: Adds the previous episode's end date to help identify new episodes.
rx_chain_with_prev AS (
 SELECT
 c.*,
 LAG(c.episode_end) OVER (
 PARTITION BY c.member_id, c.drug_key
 ORDER BY c.rn
 ) AS prev_episode_end
 FROM rx_chain_latest c
),
-- rx_episodes: This CTE identifies the start and end of each unique medication episode.
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
-- rx_episode_bounds: This CTE summarizes the full duration and flags for each episode.
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
-- 7) Claim Events CTE: Standardizes all claims into a single event table.
claim_events AS (
 -- Diagnosis events with HCC categories.
 SELECT
 ln.member_id,
 COALESCE(ln.service_date, ln.service_end_date) AS event_date,
 'CLAIM_DIAGNOSIS' AS event_type,
 ln.claim_id,
 ln.claim_line_number,
 ln.place_of_service,
 ln.revenue_code,
 'ICD10' AS code_type,
 dx.diagnosis_code AS code,
 dx.dx_prefix3 AS code_family,
 dx.hcc_category AS hcc_category,
 -- UPDATED: Adding the new HCC flags here
 dx.CNT_HCC_DIABETES,
 dx.CNT_HCC_MENTAL_HEALTH,
 dx.CNT_HCC_CARDIOVASCULAR,
 dx.CNT_HCC_PULMONARY,
 dx.CNT_HCC_KIDNEY,
 dx.CNT_HCC_SUD,
 dx.CNT_HCC_OTHER_COMPLEX,
 NULL::STRING AS hcpcs_category,
 NULL::STRING AS hcpcs_category_short,
 ln.hcpcs_code AS hcpcs_code,
 ln.paid_amount AS paid_amount,
 CASE WHEN ln.place_of_service = '23' OR LEFT(COALESCE(ln.revenue_code,''),3) = '045' THEN TRUE ELSE FALSE END AS is_ed_event,
 CASE WHEN ln.place_of_service = '21' OR REGEXP_LIKE(ln.revenue_code,'^01[0-9]') OR REGEXP_LIKE(ln.revenue_code,'^02[0-1]') THEN TRUE ELSE FALSE END AS is_ip_event,
 NULL::NUMBER AS score,
 NULL::NUMBER AS confidence,
 NULL::NUMBER AS population_baseline,
 NULL::NUMBER AS market_baseline,
 NULL::NUMBER AS individual_baseline,
 NULL::STRING AS evidence,
 NULL::STRING AS combined_notes,
 NULL::VARCHAR AS raw_response
 FROM lines ln
 JOIN diag_long dx
 ON dx.claim_id = ln.claim_id
 
 UNION ALL

 -- Revenue events.
 SELECT
 ln.member_id,
 COALESCE(ln.service_date, ln.service_end_date) AS event_date,
 'CLAIM_REVENUE' AS event_type,
 ln.claim_id,
 ln.claim_line_number,
 ln.place_of_service,
 ln.revenue_code,
 'REV' AS code_type,
 ln.revenue_code AS code,
 SUBSTR(ln.revenue_code,1,3) AS code_family,
 NULL::STRING AS hcc_category,
 -- UPDATED: Adding the new HCC flags here as NULL
 NULL::NUMBER AS CNT_HCC_DIABETES,
 NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
 NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
 NULL::NUMBER AS CNT_HCC_PULMONARY,
 NULL::NUMBER AS CNT_HCC_KIDNEY,
 NULL::NUMBER AS CNT_HCC_SUD,
 NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
 NULL::STRING AS hcpcs_category,
 NULL::STRING AS hcpcs_category_short,
 ln.hcpcs_code AS hcpcs_code,
 ln.paid_amount AS paid_amount,
 CASE WHEN ln.place_of_service = '23' OR LEFT(COALESCE(ln.revenue_code,''),3) = '045' THEN TRUE ELSE FALSE END AS is_ed_event,
 CASE WHEN ln.place_of_service = '21' OR REGEXP_LIKE(ln.revenue_code,'^01[0-9]') OR REGEXP_LIKE(ln.revenue_code,'^02[0-1]') THEN TRUE ELSE FALSE END AS is_ip_event,
 NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER,
 NULL::STRING, NULL::STRING, NULL::VARCHAR
 FROM lines ln
 WHERE ln.revenue_code IS NOT NULL

 UNION ALL

 -- Procedure events with HCPCS categories.
 SELECT
 ln.member_id,
 COALESCE(ln.service_date, ln.service_end_date) AS event_date,
 'CLAIM_PROCEDURE' AS event_type,
 ln.claim_id,
 ln.claim_line_number,
 ln.place_of_service,
 ln.revenue_code,
 'HCPCS' AS code_type,
 ln.hcpcs_code AS code,
 CASE WHEN ln.hcpcs_code IS NOT NULL AND REGEXP_LIKE(ln.hcpcs_code,'^[A-Z]') THEN SUBSTR(ln.hcpcs_code,1,1) ELSE NULL END AS code_family,
 NULL::STRING AS hcc_category,
 -- UPDATED: Adding the new HCC flags here as NULL
 NULL::NUMBER AS CNT_HCC_DIABETES,
 NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
 NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
 NULL::NUMBER AS CNT_HCC_PULMONARY,
 NULL::NUMBER AS CNT_HCC_KIDNEY,
 NULL::NUMBER AS CNT_HCC_SUD,
 NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
 hpcscat.HCPCS_CATEGORY AS hcpcs_category,
 hpcscat.HCPCS_CATEGORY_SHORT AS hpcscat_category_short,
 ln.hcpcs_code AS hcpcs_code,
 ln.paid_amount AS paid_amount,
 CASE WHEN ln.place_of_service = '23' OR LEFT(COALESCE(ln.revenue_code,''),3) = '045' THEN TRUE ELSE FALSE END AS is_ed_event,
 CASE WHEN ln.place_of_service = '21' OR REGEXP_LIKE(ln.revenue_code,'^01[0-9]') OR REGEXP_LIKE(ln.revenue_code,'^02[0-1]') THEN TRUE ELSE FALSE END AS is_ip_event,
 NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER,
 NULL::STRING, NULL::STRING, NULL::VARCHAR
 FROM lines ln
 LEFT JOIN IDENTIFIER($HCPCS_CATEGORIES_FQN) hpcscat
 ON UPPER(TRIM(ln.HCPCS_CODE)) = UPPER(TRIM(hpcscat.HCPCS_CODE))
 WHERE ln.hcpcs_code IS NOT NULL
),

-- 8) Care Note Events CTE: Align with the claim events structure.
note_events AS (
 SELECT
 member_id,
 n.note_date AS event_date,
 'CARE_NOTE' AS event_type,
 NULL::STRING AS claim_id,
 NULL::STRING AS claim_line_number,
 NULL::STRING AS place_of_service,
 NULL::STRING AS revenue_code,
 'NOTE_CATEGORY' AS code_type,
 n.category AS code,
 NULL::STRING AS code_family,
 NULL::STRING AS hcc_category,
 -- UPDATED: Adding the new HCC flags here as NULL
 NULL::NUMBER AS CNT_HCC_DIABETES,
 NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
 NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
 NULL::NUMBER AS CNT_HCC_PULMONARY,
 NULL::NUMBER AS CNT_HCC_KIDNEY,
 NULL::NUMBER AS CNT_HCC_SUD,
 NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
 NULL::STRING AS hcpcs_category,
 NULL::STRING AS hpcscat_category_short,
 NULL::STRING AS hcpcs_code,
 NULL::FLOAT AS paid_amount,
 FALSE AS is_ed_event,
 FALSE AS is_ip_event,
 n.score,
 n.confidence,
 n.POPULATION_BASELINE,
 n.MARKET_BASELINE,
 n.INDIVIDUAL_BASELINE,
 n.EVIDENCE,
 n.COMBINED_NOTES,
 TO_VARCHAR(n.RAW_RESPONSE) AS raw_response
 FROM notes_long n
),

-- 9) All Events with Context: Combines all event types with member demographics.
events_ctx AS (
 SELECT e.*
 FROM (
 SELECT * FROM claim_events
 UNION ALL
 SELECT * FROM note_events
 ) e
),

-- 10) Daily Signal CTE: Identifies ED/IP events on each day.
daily_signal AS (
 SELECT
 member_id,
 event_date,
 MAX(IFF(is_ed_event,1,0)) AS any_ed_on_date,
 MAX(IFF(is_ip_event,1,0)) AS any_ip_on_date
 FROM events_ctx
 GROUP BY member_id, event_date
),

-- 11) Labels CTE: Creates 30/60/90 day look-ahead labels for ED/IP events.
labels AS (
 SELECT
 ds.member_id,
 ds.event_date,
 MAX(ds.any_ed_on_date) OVER (PARTITION BY ds.member_id ORDER BY ds.event_date ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) AS y_ed_30d,
 MAX(ds.any_ed_on_date) OVER (PARTITION BY ds.member_id ORDER BY ds.event_date ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING) AS y_ed_60d,
 MAX(ds.any_ed_on_date) OVER (PARTITION BY ds.member_id ORDER BY ds.event_date ROWS BETWEEN 1 FOLLOWING AND 90 FOLLOWING) AS y_ed_90d,
 MAX(ds.any_ip_on_date) OVER (PARTITION BY ds.member_id ORDER BY ds.event_date ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) AS y_ip_30d,
 MAX(ds.any_ip_on_date) OVER (PARTITION BY ds.member_id ORDER BY ds.event_date ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING) AS y_ip_60d,
 MAX(ds.any_ip_on_date) OVER (PARTITION BY ds.member_id ORDER BY ds.event_date ROWS BETWEEN 1 FOLLOWING AND 90 FOLLOWING) AS y_ip_90d
 FROM daily_signal ds
),

-- 12) RX Days-in-Hand CTE: Calculates the number of days of medication remaining on any given day.
-- The following CTEs are a corrected pipeline for handling medication episodes.
rx_days_at_event AS (
 SELECT
 e.member_id,
 e.event_date,
 MAX( IFF(e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_any,
 MAX( IFF(r.epi_antipsych = 1 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_antipsych,
 MAX( IFF(r.epi_insulin = 1 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_insulin,
 MAX( IFF(r.epi_oral_antidiab = 1 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_oral_antidiab,
 MAX( IFF(r.epi_statin = 1 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_statin,
 MAX( IFF(r.epi_beta_blocker = 1 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_beta_blocker,
 MAX( IFF(r.epi_opioid = 1 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end, DATEDIFF('day', e.event_date, r.coverage_end) + 1, 0) ) AS rx_days_opioid
 FROM events_ctx e
 LEFT JOIN rx_episode_bounds r
 ON r.member_id = e.member_id
 AND e.event_date BETWEEN r.coverage_start AND r.coverage_end
 GROUP BY e.member_id, e.event_date
)

-- 13) Final SELECT: Joins all CTEs into the final flattened events table.
SELECT
 e.member_id,
 m.market,
 m.dob,
 m.gender,
 COALESCE(g.engagement_group, m.normalized_coverage_category) AS engagement_group,
 m.normalized_coverage_category,
 m.months_since_batched,
 e.event_date,
 e.event_type,
 e.claim_id,
 e.claim_line_number,
 e.place_of_service,
 e.revenue_code,
 e.code_type,
 e.code,
 e.code_family,
 e.hcc_category,
 e.cnt_hcc_diabetes,
 e.cnt_hcc_mental_health,
 e.cnt_hcc_cardiovascular,
 e.cnt_hcc_pulmonary,
 e.cnt_hcc_kidney,
 e.cnt_hcc_sud,
 e.cnt_hcc_other_complex,
 e.hcpcs_category,
 e.hcpcs_category_short,
 e.hcpcs_code,
 e.paid_amount,
 e.is_ed_event,
 e.is_ip_event,
 e.score,
 e.confidence,
 e.population_baseline,
 e.market_baseline,
 e.individual_baseline,
 e.evidence,
 e.combined_notes,
 e.raw_response,
 l.y_ed_30d, l.y_ed_60d, l.y_ed_90d,
 l.y_ip_30d, l.y_ip_60d, l.y_ip_90d,
 GREATEST(COALESCE(l.y_ed_30d,0), COALESCE(l.y_ip_30d,0)) AS y_any_30d,
 GREATEST(COALESCE(l.y_ed_60d,0), COALESCE(l.y_ip_60d,0)) AS y_any_60d,
 GREATEST(COALESCE(l.y_ed_90d,0), COALESCE(l.y_ip_90d,0)) AS y_any_90d,
 rx.rx_days_any,
 rx.rx_days_antipsych,
 rx.rx_days_insulin,
 rx.rx_days_oral_antidiab,
 rx.rx_days_statin,
 rx.rx_days_beta_blocker,
 rx.rx_days_opioid
FROM events_ctx e
LEFT OUTER JOIN MEMBERS m on e.member_id = m.member_id
LEFT JOIN member_groups g on g.member_id = m.member_id
LEFT JOIN labels l
 ON l.member_id = e.member_id
 AND l.event_date = e.event_date
LEFT JOIN rx_days_at_event rx
 ON rx.member_id = e.member_id
 AND rx.event_date = e.event_date;
