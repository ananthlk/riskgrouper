    /*
    ============================================================================
    VERSION LOG:
    - v1.0 (2025-08-19): Initial production-ready script.
    - v1.1 (2025-08-19):
        - Corrected major data leakage by adding INNER JOIN to 'members' CTE in all event source CTEs.
        - Integrated new data sources: ADT, ZUS Auths, UHC Auths, and Health Checks.
        - Added source flags (e.g., is_ip_adt, is_ed_auth) to track origin of IP/ED events.
        - Implemented COALESCE for 'engagement_group' to handle NULLs, defaulting to 'not_selected_for_engagement'.
        - Refined label generation logic to be based on a daily signal, preventing duplicate counting.
    ============================================================================
    PRODUCTION-READY: Creation of EVENTS_WITH_LABELS_RX
    PURPOSE: This script transforms granular event data into a preliminary, event-level
            table with all necessary features, flags, and look-ahead labels. This table
            serves as the source for the final daily aggregation.

    Key Features:
    - Combines data from claims, care notes, and simple pharmacy fill events.
    - Correctly joins to HCC V28 mapping to create specific disease flags.
    - Creates boolean flags for different types of pharmacy fills (e.g., antipsychotics).
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
    SET MEDICAL_CLAIM_LAG_MONTHS = 4;
    SET PHARMACY_CLAIM_LAG_MONTHS = 2;

    USE DATABASE IDENTIFIER($TARGET_DB);
    USE SCHEMA IDENTIFIER($TARGET_SCHEMA);

    /*
    ============================================================================
    1) Build TEMP table: EVENTS_WITH_LABELS_RX
    This CTE block prepares all event-level data before daily aggregation.
    ============================================================================
    */
    CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX AS
    WITH
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
    LEFT JOIN IDENTIFIER($MEMBER_QUAL_FQN) c ON a.fh_id = c.fh_id
    WHERE c.is_fh_clinically_qualified = 1
    AND A.MARKET IS NOT NULL
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
    DATE(l.CLAIM_START_DATE) AS service_date,
    DATE(l.CLAIM_END_DATE) AS service_end_date,
    l.CLAIM_LINE_NUMBER AS claim_line_number,
    UPPER(TRIM(l.CMS_PLACE_OF_SERVICE_CODE)) AS place_of_service,
    UPPER(TRIM(l.CMS_REVENUE_CENTER_CODE)) AS revenue_code,
    UPPER(TRIM(l.HCPCS_CODE)) AS hcpcs_code,
    l.CLAIM_PLAN_PAID_AMOUNT AS paid_amount
    FROM IDENTIFIER($CLAIM_LINES_FQN) l
    JOIN members m ON l.FH_ID = m.member_id
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
    CASE WHEN hcc.HCC_CODE IS NOT NULL AND hcc.HCC_CODE NOT IN (36, 37, 38, 151, 152, 153, 154, 155, 222, 223, 224, 249, 263, 264, 277, 279, 280, 326, 327, 328, 329, 135, 136, 137, 139) THEN 1 ELSE 0 END AS CNT_HCC_OTHER_COMPLEX,
    -- NEW Thematic flags for label generation
    CASE WHEN hcc.HCC_CODE IN (1) THEN 1 ELSE 0 END AS has_hiv,
    CASE WHEN hcc.HCC_CODE IN (47) THEN 1 ELSE 0 END AS has_malnutrition,
    CASE WHEN hcc.HCC_CODE IN (151, 152, 153, 154, 155) THEN 1 ELSE 0 END AS has_smi,
    CASE WHEN hcc.HCC_CODE IN (226) THEN 1 ELSE 0 END AS has_chf,
    CASE WHEN hcc.HCC_CODE IN (264) THEN 1 ELSE 0 END AS has_copd,
    CASE WHEN hcc.HCC_CODE IN (135, 136, 137, 139) THEN 1 ELSE 0 END AS has_sud_thematic,
    CASE WHEN hcc.HCC_CODE IN (36, 37, 38) THEN 1 ELSE 0 END AS has_diabetes
    FROM IDENTIFIER($CLAIM_DIAGNOSIS_FQN) d
    JOIN members m ON d.FH_ID = m.member_id
    LEFT JOIN IDENTIFIER($HCC_MAPPING_FQN) hcc
    ON REPLACE(d.ICD10CM_CODE, '.', '') = REPLACE(hcc.DIAGNOSIS_CODE, '.', '')
    AND hcc.MODEL_VERSION = 'CMS-HCC-V28'
    AND hcc.PAYMENT_YEAR = 2024
    WHERE d.ICD10CM_CODE IS NOT NULL
    ),-- Stratified Notes CTE: Unstructured data from care notes.
    notes_long AS (
    SELECT
    n.FH_ID AS member_id,
    DATE(n.SOURCE_INTERACTION_DATE) AS note_date,
    UPPER(TRIM(n.CATEGORY)) AS category,
    n.SCORE AS score,
    n.CONFIDENCE AS confidence,
    n.POPULATION_BASELINE AS market_baseline,
    n.INDIVIDUAL_BASELINE AS individual_baseline,
    n.EVIDENCE,
    n.COMBINED_NOTES,
    n.RAW_RESPONSE
    FROM IDENTIFIER($STRAT_NOTES_FQN) n
    JOIN members m ON n.FH_ID = m.member_id
    ),

    -- 7) Claim Events CTE: Standardizes all claims into a single event table.
        claim_events AS (
        -- Aggregate by claim line, dropping diagnosis columns and deduplicating
        SELECT
            ln.member_id,
            COALESCE(ln.service_date, ln.service_end_date) AS event_date,
            'CLAIM_DIAGNOSIS' AS event_type,
            ln.claim_id,
            ln.claim_line_number,
            ln.place_of_service,
            ln.revenue_code,
            'ICD10' AS code_type,
            NULL::STRING AS code, -- drop diagnosis_code
            NULL::STRING AS code_family, -- drop dx_prefix3
            MAX(dx.hcc_category) AS hcc_category,
            -- Aggregate HCC flags
            MAX(dx.CNT_HCC_DIABETES) AS CNT_HCC_DIABETES,
            MAX(dx.CNT_HCC_MENTAL_HEALTH) AS CNT_HCC_MENTAL_HEALTH,
            MAX(dx.CNT_HCC_CARDIOVASCULAR) AS CNT_HCC_CARDIOVASCULAR,
            MAX(dx.CNT_HCC_PULMONARY) AS CNT_HCC_PULMONARY,
            MAX(dx.CNT_HCC_KIDNEY) AS CNT_HCC_KIDNEY,
            MAX(dx.CNT_HCC_SUD) AS CNT_HCC_SUD,
            MAX(dx.CNT_HCC_OTHER_COMPLEX) AS CNT_HCC_OTHER_COMPLEX,
            -- Aggregate thematic flags
            MAX(dx.has_hiv) AS has_hiv,
            MAX(dx.has_malnutrition) AS has_malnutrition,
            MAX(dx.has_smi) AS has_smi,
            MAX(dx.has_chf) AS has_chf,
            MAX(dx.has_copd) AS has_copd,
            MAX(dx.has_sud_thematic) AS has_sud_thematic,
            MAX(dx.has_diabetes) AS has_diabetes,
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
            NULL::VARCHAR AS raw_response,
            -- NEW: Add NULL placeholders for pharmacy fill flags
            NULL::BOOLEAN AS is_fill_antipsychotic,
            NULL::BOOLEAN AS is_fill_insulin,
            NULL::BOOLEAN AS is_fill_oral_antidiab,
            NULL::BOOLEAN AS is_fill_statin,
            NULL::BOOLEAN AS is_fill_beta_blocker,
            NULL::BOOLEAN AS is_fill_opioid,
            -- NEW: Add NULL placeholders for non-claim event source flags
            FALSE AS is_ip_adt,
            FALSE AS is_ip_auth,
            FALSE AS is_ip_hc,
            FALSE AS is_ed_adt,
            FALSE AS is_ed_auth,
            FALSE AS is_ed_hc
        FROM lines ln
        JOIN diag_long dx
            ON dx.claim_id = ln.claim_id
            AND dx.member_id = ln.member_id
        GROUP BY
            ln.member_id,
            COALESCE(ln.service_date, ln.service_end_date),
            ln.claim_id,
            ln.claim_line_number,
            ln.place_of_service,
            ln.revenue_code,
            ln.hcpcs_code,
            ln.paid_amount

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
    -- NEW Thematic flags
    NULL::NUMBER AS has_hiv,
    NULL::NUMBER AS has_malnutrition,
    NULL::NUMBER AS has_smi,
    NULL::NUMBER AS has_chf,
    NULL::NUMBER AS has_copd,
    NULL::NUMBER AS has_sud_thematic,
    NULL::NUMBER AS has_diabetes,
    NULL::STRING AS hcpcs_category,
    NULL::STRING AS hcpcs_category_short,
    ln.hcpcs_code AS hcpcs_code,
    ln.paid_amount AS paid_amount,
    CASE WHEN ln.place_of_service = '23' OR LEFT(COALESCE(ln.revenue_code,''),3) = '045' THEN TRUE ELSE FALSE END AS is_ed_event,
    CASE WHEN ln.place_of_service = '21' OR REGEXP_LIKE(ln.revenue_code,'^01[0-9]') OR REGEXP_LIKE(ln.revenue_code,'^02[0-1]') THEN TRUE ELSE FALSE END AS is_ip_event,
    NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER,
    NULL::STRING, NULL::STRING, NULL::VARCHAR,
    -- NEW: Add NULL placeholders for pharmacy fill flags
    NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN,
    -- NEW: Add NULL placeholders for non-claim event source flags
    FALSE, FALSE, FALSE, FALSE, FALSE, FALSE
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
    -- NEW Thematic flags
    NULL::NUMBER AS has_hiv,
    NULL::NUMBER AS has_malnutrition,
    NULL::NUMBER AS has_smi,
    NULL::NUMBER AS has_chf,
    NULL::NUMBER AS has_copd,
    NULL::NUMBER AS has_sud_thematic,
    NULL::NUMBER AS has_diabetes,
    hpcscat.HCPCS_CATEGORY AS hcpcs_category,
    hpcscat.HCPCS_CATEGORY_SHORT AS hpcscat_category_short,
    ln.hcpcs_code AS hcpcs_code,
    ln.paid_amount AS paid_amount,
    CASE WHEN ln.place_of_service = '23' OR LEFT(COALESCE(ln.revenue_code,''),3) = '045' THEN TRUE ELSE FALSE END AS is_ed_event,
    CASE WHEN ln.place_of_service = '21' OR REGEXP_LIKE(ln.revenue_code,'^01[0-9]') OR REGEXP_LIKE(ln.revenue_code,'^02[0-1]') THEN TRUE ELSE FALSE END AS is_ip_event,
    NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER, NULL::NUMBER,
    NULL::STRING, NULL::STRING, NULL::VARCHAR,
    -- NEW: Add NULL placeholders for pharmacy fill flags
    NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN,
    -- NEW: Add NULL placeholders for non-claim event source flags
    FALSE, FALSE, FALSE, FALSE, FALSE, FALSE
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
    -- NEW Thematic flags
    NULL::NUMBER AS has_hiv,
    NULL::NUMBER AS has_malnutrition,
    NULL::NUMBER AS has_smi,
    NULL::NUMBER AS has_chf,
    NULL::NUMBER AS has_copd,
    NULL::NUMBER AS has_sud_thematic,
    NULL::NUMBER AS has_diabetes,
    NULL::STRING AS hcpcs_category,
    NULL::STRING AS hpcscat_category_short,
    NULL::STRING AS hcpcs_code,
    NULL::FLOAT AS paid_amount,
    FALSE AS is_ed_event,
    FALSE AS is_ip_event,
    n.score,
    n.confidence,
    NULL::NUMBER AS population_baseline,
    n.market_baseline,
    n.individual_baseline,
    n.EVIDENCE,
    n.COMBINED_NOTES,
    TO_VARCHAR(n.RAW_RESPONSE) AS raw_response,
    -- NEW: Add NULL placeholders for pharmacy fill flags
    NULL::BOOLEAN AS is_fill_antipsychotic,
    NULL::BOOLEAN AS is_fill_insulin,
    NULL::BOOLEAN AS is_fill_oral_antidiab,
    NULL::BOOLEAN AS is_fill_statin,
    NULL::BOOLEAN AS is_fill_beta_blocker,
    NULL::BOOLEAN AS is_fill_opioid,
    -- NEW: Add NULL placeholders for non-claim event source flags
    FALSE AS is_ip_adt,
    FALSE AS is_ip_auth,
    FALSE AS is_ip_hc,
    FALSE AS is_ed_adt,
    FALSE AS is_ed_auth,
    FALSE AS is_ed_hc
    FROM notes_long n
    ),

    -- NEW: Pharmacy Fill Events CTE
    pharmacy_events AS (
        SELECT
            r.FH_ID AS member_id,
            DATE(r.FILLED_DATE) AS event_date,
            'PHARMACY_FILL' AS event_type,
            NULL::STRING AS claim_id,
            NULL::STRING AS claim_line_number,
            NULL::STRING AS place_of_service,
            NULL::STRING AS revenue_code,
            'NDC' AS code_type,
            COALESCE(
                NULLIF(r.NDC_CODE_11_DIGIT,''),
                NULLIF(r.NDC_CODE,''),
                NULLIF(r.PRIMARY_AGENT_DESC,''),
                NULLIF(r.ACTIVE_INGREDIENTS_NAME,'')
            ) AS code,
            NULL::STRING AS code_family,
            NULL::STRING AS hcc_category,
            NULL::NUMBER AS CNT_HCC_DIABETES,
            NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
            NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
            NULL::NUMBER AS CNT_HCC_PULMONARY,
            NULL::NUMBER AS CNT_HCC_KIDNEY,
            NULL::NUMBER AS CNT_HCC_SUD,
            NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
            -- NEW Thematic flags
            NULL::NUMBER AS has_hiv,
            NULL::NUMBER AS has_malnutrition,
            NULL::NUMBER AS has_smi,
            NULL::NUMBER AS has_chf,
            NULL::NUMBER AS has_copd,
            NULL::NUMBER AS has_sud_thematic,
            NULL::NUMBER AS has_diabetes,
            NULL::STRING AS hcpcs_category,
            NULL::STRING AS hpcscat_category_short,
            NULL::STRING AS hcpcs_code,
            NULL::FLOAT AS paid_amount,
            FALSE AS is_ed_event,
            FALSE AS is_ip_event,
            NULL::NUMBER AS score,
            NULL::NUMBER AS confidence,
            NULL::NUMBER AS POPULATION_BASELINE,
            NULL::NUMBER AS MARKET_BASELINE,
            NULL::NUMBER AS INDIVIDUAL_BASELINE,
            NULL::STRING AS EVIDENCE,
            NULL::STRING AS COMBINED_NOTES,
            NULL::VARCHAR AS raw_response,
            -- Pharmacy fill flags
            IFF(COALESCE(r.IS_FH_ANTIPSYCHOTIC, FALSE) OR COALESCE(r.IS_ANTIPSYCH_MED, FALSE), TRUE, FALSE) AS is_fill_antipsychotic,
            COALESCE(r.IS_INSULIN, FALSE) AS is_fill_insulin,
            COALESCE(r.IS_ORAL_ANTIDIABETIC, FALSE) AS is_fill_oral_antidiab,
            COALESCE(r.IS_STATIN, FALSE) AS is_fill_statin,
            COALESCE(r.IS_BETA_BLOCKER, FALSE) AS is_fill_beta_blocker,
            COALESCE(r.IS_OPIATE_AGONISTS, FALSE) AS is_fill_opioid,
            -- NEW: Add NULL placeholders for non-claim event source flags
            FALSE AS is_ip_adt,
            FALSE AS is_ip_auth,
            FALSE AS is_ip_hc,
            FALSE AS is_ed_adt,
            FALSE AS is_ed_auth,
            FALSE AS is_ed_hc
        FROM IDENTIFIER($PHARMACY_CLAIMS_FQN) r
        JOIN members m ON r.FH_ID = m.member_id
        WHERE DATE(r.FILLED_DATE) IS NOT NULL AND r.DAYS_SUPPLY > 0
    ),

    -- NEW: Health Check Events CTE
    health_check_events AS (
        SELECT
            fh_id AS member_id,
            COALESCE(DATE(admission_date), DATE(created_at)) AS event_date,
            'HEALTH_CHECK' AS event_type,
            NULL::STRING AS claim_id,
            NULL::STRING AS claim_line_number,
            NULL::STRING AS place_of_service,
            NULL::STRING AS revenue_code,
            'HEALTH_CHECK_TYPE' AS code_type,
            HOSPITALIZATION_TYPE AS code,
            NULL::STRING AS code_family,
            NULL::STRING AS hcc_category,
            NULL::NUMBER AS CNT_HCC_DIABETES,
            NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
            NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
            NULL::NUMBER AS CNT_HCC_PULMONARY,
            NULL::NUMBER AS CNT_HCC_KIDNEY,
            NULL::NUMBER AS CNT_HCC_SUD,
            NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
            -- NEW Thematic flags
            NULL::NUMBER AS has_hiv,
            NULL::NUMBER AS has_malnutrition,
            NULL::NUMBER AS has_smi,
            NULL::NUMBER AS has_chf,
            NULL::NUMBER AS has_copd,
            NULL::NUMBER AS has_sud_thematic,
            NULL::NUMBER AS has_diabetes,
            NULL::STRING AS hcpcs_category,
            NULL::STRING AS hcpcs_category_short,
            NULL::STRING AS hcpcs_code,
            NULL::FLOAT AS paid_amount,
            LOWER(HOSPITALIZATION_TYPE) IN ('ed', 'emergency') AS is_ed_event,
            LOWER(HOSPITALIZATION_TYPE) IN ('ip', 'inpatient', 'in-patient') AS is_ip_event,
            NULL::NUMBER AS score,
            NULL::NUMBER AS confidence,
            NULL::NUMBER AS population_baseline,
            NULL::NUMBER AS market_baseline,
            NULL::NUMBER AS individual_baseline,
            NULL::STRING AS evidence,
            NULL::STRING AS combined_notes,
            NULL::VARCHAR AS raw_response,
            NULL::BOOLEAN AS is_fill_antipsychotic,
            NULL::BOOLEAN AS is_fill_insulin,
            NULL::BOOLEAN AS is_fill_oral_antidiab,
            NULL::BOOLEAN AS is_fill_statin,
            NULL::BOOLEAN AS is_fill_beta_blocker,
            NULL::BOOLEAN AS is_fill_opioid,
            -- Source flags
            FALSE AS is_ip_adt,
            FALSE AS is_ip_auth,
            LOWER(HOSPITALIZATION_TYPE) IN ('ip', 'inpatient', 'in-patient') AS is_ip_hc,
            FALSE AS is_ed_adt,
            FALSE AS is_ed_auth,
            LOWER(HOSPITALIZATION_TYPE) IN ('ed', 'emergency') AS is_ed_hc
        FROM PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.COMMUNITY_TEAM_HEALTH_CHECKS hc
        JOIN members m ON hc.fh_id = m.member_id
        WHERE has_recent_hospitalization = TRUE
        AND COALESCE(DATE(admission_date), DATE(created_at)) IS NOT NULL
    ),

    -- NEW: ZUS Auth Events CTE
    zus_auth_events AS (
        SELECT
            fh_id AS member_id,
            DATE(admit_date) AS event_date,
            'ZUS_AUTH' AS event_type,
            NULL::STRING AS claim_id,
            NULL::STRING AS claim_line_number,
            NULL::STRING AS place_of_service,
            NULL::STRING AS revenue_code,
            'ZUS_AUTH_TYPE' AS code_type,
            TREATMENT_SETTING AS code,
            NULL::STRING AS code_family,
            NULL::STRING AS hcc_category,
            NULL::NUMBER AS CNT_HCC_DIABETES,
            NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
            NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
            NULL::NUMBER AS CNT_HCC_PULMONARY,
            NULL::NUMBER AS CNT_HCC_KIDNEY,
            NULL::NUMBER AS CNT_HCC_SUD,
            NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
            -- NEW Thematic flags
            NULL::NUMBER AS has_hiv,
            NULL::NUMBER AS has_malnutrition,
            NULL::NUMBER AS has_smi,
            NULL::NUMBER AS has_chf,
            NULL::NUMBER AS has_copd,
            NULL::NUMBER AS has_sud_thematic,
            NULL::NUMBER AS has_diabetes,
            NULL::STRING AS hcpcs_category,
            NULL::STRING AS hcpcs_category_short,
            NULL::STRING AS hcpcs_code,
            NULL::FLOAT AS paid_amount,
            LOWER(TREATMENT_SETTING) IN ('ed', 'emergency') AS is_ed_event,
            LOWER(TREATMENT_SETTING) IN ('ip', 'inpatient') AS is_ip_event,
            NULL::NUMBER AS score,
            NULL::NUMBER AS confidence,
            NULL::NUMBER AS population_baseline,
            NULL::NUMBER AS market_baseline,
            NULL::NUMBER AS individual_baseline,
            NULL::STRING AS evidence,
            NULL::STRING AS combined_notes,
            NULL::VARCHAR AS raw_response,
            NULL::BOOLEAN AS is_fill_antipsychotic,
            NULL::BOOLEAN AS is_fill_insulin,
            NULL::BOOLEAN AS is_fill_oral_antidiab,
            NULL::BOOLEAN AS is_fill_statin,
            NULL::BOOLEAN AS is_fill_beta_blocker,
            NULL::BOOLEAN AS is_fill_opioid,
            -- Source flags
            FALSE AS is_ip_adt,
            LOWER(TREATMENT_SETTING) IN ('ip', 'inpatient') AS is_ip_auth,
            FALSE AS is_ip_hc,
            FALSE AS is_ed_adt,
            LOWER(TREATMENT_SETTING) IN ('ed', 'emergency') AS is_ed_auth,
            FALSE AS is_ed_hc
        FROM TRANSFORMED_DATA.PROD_MARTS.ZUS_AUTH_ADMITS za
        JOIN members m ON za.fh_id = m.member_id
        WHERE admit_date IS NOT NULL
    ),

    -- NEW: UHC Auth Events CTE
    uhc_auth_events AS (
        SELECT
            a.fh_id AS member_id,
            DATE(admit_date) AS event_date,
            'UHC_AUTH' AS event_type,
            NULL::STRING AS claim_id,
            NULL::STRING AS claim_line_number,
            NULL::STRING AS place_of_service,
            NULL::STRING AS revenue_code,
            'UHC_AUTH_TYPE' AS code_type,
            TREATMENT_SETTING AS code,
            NULL::STRING AS code_family,
            NULL::STRING AS hcc_category,
            NULL::NUMBER AS CNT_HCC_DIABETES,
            NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
            NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
            NULL::NUMBER AS CNT_HCC_PULMONARY,
            NULL::NUMBER AS CNT_HCC_KIDNEY,
            NULL::NUMBER AS CNT_HCC_SUD,
            NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
            -- NEW Thematic flags
            NULL::NUMBER AS has_hiv,
            NULL::NUMBER AS has_malnutrition,
            NULL::NUMBER AS has_smi,
            NULL::NUMBER AS has_chf,
            NULL::NUMBER AS has_copd,
            NULL::NUMBER AS has_sud_thematic,
            NULL::NUMBER AS has_diabetes,
            NULL::STRING AS hcpcs_category,
            NULL::STRING AS hcpcs_category_short,
            NULL::STRING AS hcpcs_code,
            NULL::FLOAT AS paid_amount,
            LOWER(TREATMENT_SETTING) IN ('ed', 'emergency') AS is_ed_event,
            LOWER(TREATMENT_SETTING) IN ('ip', 'inpatient') AS is_ip_event,
            NULL::NUMBER AS score,
            NULL::NUMBER AS confidence,
            NULL::NUMBER AS population_baseline,
            NULL::NUMBER AS market_baseline,
            NULL::NUMBER AS individual_baseline,
            NULL::STRING AS evidence,
            NULL::STRING AS combined_notes,
            NULL::VARCHAR AS raw_response,
            NULL::BOOLEAN AS is_fill_antipsychotic,
            NULL::BOOLEAN AS is_fill_insulin,
            NULL::BOOLEAN AS is_fill_oral_antidiab,
            NULL::BOOLEAN AS is_fill_statin,
            NULL::BOOLEAN AS is_fill_beta_blocker,
            NULL::BOOLEAN AS is_fill_opioid,
            -- Source flags
            FALSE AS is_ip_adt,
            LOWER(TREATMENT_SETTING) IN ('ip', 'inpatient') AS is_ip_auth,
            FALSE AS is_ip_hc,
            FALSE AS is_ed_adt,
            LOWER(TREATMENT_SETTING) IN ('ed', 'emergency') AS is_ed_auth,
            FALSE AS is_ed_hc
        FROM TRANSFORMED_DATA.PROD_BASE.UHC_INPATIENT_AUTHORIZATIONS a
        JOIN members b ON a.fh_id = b.member_id
        WHERE admit_date IS NOT NULL
    ),

    -- NEW: ADT Events CTE
    adt_events AS (
        SELECT
            fh_id AS member_id,
            DATE(admit_date) AS event_date,
            'ADT' AS event_type,
            NULL::STRING AS claim_id,
            NULL::STRING AS claim_line_number,
            NULL::STRING AS place_of_service,
            NULL::STRING AS revenue_code,
            'ADT_TYPE' AS code_type,
            TREATMENT_SETTING AS code,
            NULL::STRING AS code_family,
            NULL::STRING AS hcc_category,
            NULL::NUMBER AS CNT_HCC_DIABETES,
            NULL::NUMBER AS CNT_HCC_MENTAL_HEALTH,
            NULL::NUMBER AS CNT_HCC_CARDIOVASCULAR,
            NULL::NUMBER AS CNT_HCC_PULMONARY,
            NULL::NUMBER AS CNT_HCC_KIDNEY,
            NULL::NUMBER AS CNT_HCC_SUD,
            NULL::NUMBER AS CNT_HCC_OTHER_COMPLEX,
            -- NEW Thematic flags
            NULL::NUMBER AS has_hiv,
            NULL::NUMBER AS has_malnutrition,
            NULL::NUMBER AS has_smi,
            NULL::NUMBER AS has_chf,
            NULL::NUMBER AS has_copd,
            NULL::NUMBER AS has_sud_thematic,
            NULL::NUMBER AS has_diabetes,
            NULL::STRING AS hcpcs_category,
            NULL::STRING AS hcpcs_category_short,
            NULL::STRING AS hcpcs_code,
            NULL::FLOAT AS paid_amount,
            LOWER(TREATMENT_SETTING) IN ('ed', 'emergency') AS is_ed_event,
            LOWER(TREATMENT_SETTING) IN ('ip', 'inpatient') AS is_ip_event,
            NULL::NUMBER AS score,
            NULL::NUMBER AS confidence,
            NULL::NUMBER AS population_baseline,
            NULL::NUMBER AS market_baseline,
            NULL::NUMBER AS individual_baseline,
            NULL::STRING AS evidence,
            NULL::STRING AS combined_notes,
            NULL::VARCHAR AS raw_response,
            NULL::BOOLEAN AS is_fill_antipsychotic,
            NULL::BOOLEAN AS is_fill_insulin,
            NULL::BOOLEAN AS is_fill_oral_antidiab,
            NULL::BOOLEAN AS is_fill_statin,
            NULL::BOOLEAN AS is_fill_beta_blocker,
            NULL::BOOLEAN AS is_fill_opioid,
            -- Source flags
            LOWER(TREATMENT_SETTING) IN ('ip', 'inpatient') AS is_ip_adt,
            FALSE AS is_ip_auth,
            FALSE AS is_ip_hc,
            LOWER(TREATMENT_SETTING) IN ('ed', 'emergency') AS is_ed_adt,
            FALSE AS is_ed_auth,
            FALSE AS is_ed_hc
        FROM PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.HOSPITAL_ADMISSIONS ha
        JOIN members m ON ha.fh_id = m.member_id
        WHERE source = 'adt' AND admit_date IS NOT NULL
    ),

    -- 9) All Events with Context: Combines all event types with member demographics.
    events_ctx AS (
    SELECT e.*
    FROM (
    SELECT * FROM claim_events
    UNION ALL
    SELECT * FROM note_events
    UNION ALL
    SELECT * FROM pharmacy_events
    UNION ALL
    SELECT * FROM health_check_events
    UNION ALL
    SELECT * FROM zus_auth_events
    UNION ALL
    SELECT * FROM uhc_auth_events
    UNION ALL
    SELECT * FROM adt_events
    ) e
    ),

    -- 10) Daily Signal CTE: Identifies ED/IP events on each day from ALL sources.
    daily_signal AS (
    SELECT
    member_id,
    event_date,
    MAX(IFF(is_ed_event,1,0)) AS any_ed_on_date,
    MAX(IFF(is_ip_event,1,0)) AS any_ip_on_date,
    MAX(IFF(is_ip_adt, 1, 0)) AS is_ip_adt,
    MAX(IFF(is_ip_auth, 1, 0)) AS is_ip_auth,
    MAX(IFF(is_ip_hc, 1, 0)) AS is_ip_hc,
    MAX(IFF(is_ed_adt, 1, 0)) AS is_ed_adt,
    MAX(IFF(is_ed_auth, 1, 0)) AS is_ed_auth,
    MAX(IFF(is_ed_hc, 1, 0)) AS is_ed_hc
    FROM events_ctx
    GROUP BY member_id, event_date
    ),

    -- NEW (v1.7): Unified Claim Signal CTE
    -- This CTE is now solely responsible for identifying days with thematic diagnoses from claims.
    -- ED/IP signals have been removed to avoid incorrect calculations and are handled exclusively
    -- in the 'daily_signal' CTE, which is the single source of truth.
    unified_claim_signal AS (
        SELECT
            member_id,
            event_date,
            MAX(IFF(has_hiv > 0, 1, 0)) AS has_hiv,
            MAX(IFF(has_malnutrition > 0, 1, 0)) AS has_malnutrition,
            MAX(IFF(has_smi > 0, 1, 0)) AS has_smi,
            MAX(IFF(has_chf > 0, 1, 0)) AS has_chf,
            MAX(IFF(has_copd > 0, 1, 0)) AS has_copd,
            MAX(IFF(has_sud_thematic > 0, 1, 0)) AS has_sud,
            MAX(IFF(has_diabetes > 0, 1, 0)) AS has_diabetes
        FROM events_ctx
        WHERE event_type = 'CLAIM_DIAGNOSIS'
        GROUP BY member_id, event_date
    ),

    -- NEW (v1.7): Combined Signal CTE
    -- This CTE combines all daily signals (from all event sources) with the claim-based thematic
    -- diagnosis signals. A FULL OUTER JOIN is critical to ensure that days with only non-claim
    -- events (like ADT) or only claim events are preserved for label calculation.
    combined_signal AS (
        SELECT
            COALESCE(ds.member_id, ucs.member_id) AS member_id,
            COALESCE(ds.event_date, ucs.event_date) AS event_date,
            COALESCE(ds.any_ed_on_date, 0) AS any_ed_on_date,
            COALESCE(ds.any_ip_on_date, 0) AS any_ip_on_date,
            COALESCE(ucs.has_hiv, 0) AS has_hiv,
            COALESCE(ucs.has_malnutrition, 0) AS has_malnutrition,
            COALESCE(ucs.has_smi, 0) AS has_smi,
            COALESCE(ucs.has_chf, 0) AS has_chf,
            COALESCE(ucs.has_copd, 0) AS has_copd,
            COALESCE(ucs.has_sud, 0) AS has_sud,
            COALESCE(ucs.has_diabetes, 0) AS has_diabetes
        FROM daily_signal ds
        FULL OUTER JOIN unified_claim_signal ucs ON ds.member_id = ucs.member_id AND ds.event_date = ucs.event_date
    ),

    -- 11) Labels CTE: Creates 30/60/90 day look-ahead labels for ED/IP events.
    -- UPDATED (v1.7): Corrected thematic label logic to use the new combined_signal CTE.
    labels AS (
            SELECT
                member_id,
                event_date,
                -- Standard ED/IP Labels
                MAX(any_ed_on_date) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) AS y_ed_30d,
                MAX(any_ed_on_date) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING) AS y_ed_60d,
                MAX(any_ed_on_date) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 1 FOLLOWING AND 90 FOLLOWING) AS y_ed_90d,
                MAX(any_ip_on_date) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) AS y_ip_30d,
                MAX(any_ip_on_date) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING) AS y_ip_60d,
                MAX(any_ip_on_date) OVER (PARTITION BY member_id ORDER BY event_date ROWS BETWEEN 1 FOLLOWING AND 90 FOLLOWING) AS y_ip_90d,

                -- Thematic Labels: Only positive on the actual IP/ED event date with the diagnosis
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_hiv = 1, 1, 0) AS y_hiv_60d,
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_malnutrition = 1, 1, 0) AS y_malnutrition_60d,
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_smi = 1, 1, 0) AS y_smi_60d,
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_chf = 1, 1, 0) AS y_chf_60d,
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_copd = 1, 1, 0) AS y_copd_60d,
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_sud = 1, 1, 0) AS y_sud_60d,
                IFF((any_ed_on_date = 1 OR any_ip_on_date = 1) AND has_diabetes = 1, 1, 0) AS y_diabetes_60d
            FROM combined_signal
        )

    -- 13) Final SELECT: Joins all CTEs into the final flattened events table.
        SELECT
        e.member_id,
        m.market,
        m.dob,
        m.gender,
        COALESCE(g.engagement_group, 'not_selected_for_engagement') AS engagement_group,
        m.normalized_coverage_category,
        m.months_since_batched,
        m.has_ever_been_engaged,
        m.is_batched,
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
        e.is_fill_antipsychotic,
        e.is_fill_insulin,
        e.is_fill_oral_antidiab,
        e.is_fill_statin,
        e.is_fill_beta_blocker,
        e.is_fill_opioid,
        l.y_ed_30d, l.y_ed_60d, l.y_ed_90d,
        l.y_ip_30d, l.y_ip_60d, l.y_ip_90d,
        GREATEST(COALESCE(l.y_ed_30d,0), COALESCE(l.y_ip_30d,0)) AS y_any_30d,
        GREATEST(COALESCE(l.y_ed_60d,0), COALESCE(l.y_ip_60d,0)) AS y_any_60d,
        GREATEST(COALESCE(l.y_ed_90d,0), COALESCE(l.y_ip_90d,0)) AS y_any_90d,
        -- NEW Thematic labels
        l.y_hiv_60d,
        l.y_malnutrition_60d,
        l.y_smi_60d,
        l.y_chf_60d,
        l.y_copd_60d,
        l.y_sud_60d,
        l.y_diabetes_60d,
        -- NEW: Add source flags to final output
        ds.is_ip_adt,
        ds.is_ip_auth,
        ds.is_ip_hc,
        ds.is_ed_adt,
        ds.is_ed_auth,
        ds.is_ed_hc
        FROM events_ctx e
        INNER JOIN members m on e.member_id = m.member_id
        LEFT JOIN member_groups g on g.member_id = m.member_id
        LEFT JOIN labels l
        ON l.member_id = e.member_id
        AND l.event_date = e.event_date
        LEFT JOIN daily_signal ds
        ON ds.member_id = e.member_id
        AND ds.event_date = e.event_date;
