/* =====================================================================
   Pipeline updated to densify baseline data, ensuring all categories
   are present for every individual.
   ===================================================================== */
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_AI_AGENTIC_SCORING AS
WITH
-- Step A: Cohort
MMS AS (
    SELECT 
        A.FH_ID, A.PAT_AGE, A.PAT_GENDER, A.Market, A.has_ever_been_engaged,
        DATEDIFF(month, A.batch_date, CURRENT_DATE) AS months_since_batched,
        FH_COVERAGE_CATEGORY, ELATION_MRN
    FROM TRANSFORMED_DATA.PROD.FH_MEMBERS  A
    left outer join  TRANSFORMED_DATA.prod.fh_member_selection_qualification c on a.fh_id = c.fh_id
    WHERE A.is_batched = TRUE AND A.HAS_EVER_BEEN_ENGAGED = TRUE AND A.HAS_EVER_BEEN_ON_XWALK = TRUE 
    and c.is_fh_clinically_qualified = 1
    --AND batch_date >= '2024-07-01'
    --and market = 'tacoma'
--    LIMIT 500 -- Use a larger limit for the initial seed
),
-- Step B: Notes (deduped union)
NOTES_DEDUPED AS (
  SELECT DISTINCT * FROM (
    SELECT TO_NUMBER(A.FH_ID) AS FH_ID, DATE(A.CREATED_AT) AS CREATED_DATE, MMS.MARKET, A.CONTENTS AS NOTES
    FROM MMS LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.NOTES A ON A.FH_ID = MMS.FH_ID
    UNION ALL
    SELECT TO_NUMBER(A.FH_ID), DATE(A.CREATED_AT), MMS.MARKET, A.CONTENT
    FROM MMS LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.NON_VISIT_NOTES A ON A.FH_ID = MMS.FH_ID
    UNION ALL
    SELECT A.FH_ID, DATE(C.QUESTIONNAIRE_COMPLETED_AT), A.MARKET,
           CONCAT('question: ', C.QUESTION_NAME, ' ; ', 'score : ', C.SCORE)
    FROM MMS A LEFT JOIN TRANSFORMED_DATA.PROD.STG_ELATION_CLINICAL_FORM_ANSWERS C
           ON A.ELATION_MRN = C.ELATION_PATIENT_ID
    WHERE QUESTIONNAIRE_NAME = 'PHQ-9 Questionnaire'
    UNION ALL
    SELECT TO_NUMBER(A.FH_ID), IFNULL(DATE(A._FIVETRAN_SYNCED),'2001-01-01'), MMS.MARKET,
           CONCAT('HAS_RECENT_HOSPITALIZATION:',IFNULL(TO_VARCHAR(HAS_RECENT_HOSPITALIZATION),' '),',',
                  'NOTES:',IFNULL(NOTES,' '),',',
                  'IS_HEALTH_DECLINING:',IFNULL(TO_VARCHAR(IS_HEALTH_DECLINING),' '),',',
                  'INDIVIDUAL_HAS_HEALTH_CONCERNS',IFNULL(TO_VARCHAR(INDIVIDUAL_HAS_HEALTH_CONCERNS),' '),',',
                  'INDIVIDUAL_HAS_MEDICATION_CONCERNS:',IFNULL(TO_VARCHAR(INDIVIDUAL_HAS_MEDICATION_CONCERNS),' --'),',')
    FROM MMS LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.COMMUNITY_TEAM_HEALTH_CHECKS A ON A.FH_ID = MMS.FH_ID
    UNION ALL
    SELECT MMS.FH_ID, DATE(A.CREATED_AT), MMS.MARKET,
           CONCAT(LABEL,' : ', IFF(ANSWER = 'np','not present','present'))
    FROM MMS
    LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.ASSESSMENTS A ON MMS.FH_ID = A.FH_ID
    LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.ASSESSMENT_ANSWERS B ON A.ID = B.ASSESSMENT_ID
    LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.POM_QUESTIONS C ON C.ID = B.QUESTION_ID
  )
),
-- Step C: Aggregate notes and add a flag for oversized inputs
NOTES_AGG AS (
  SELECT
    MARKET, CREATED_DATE, FH_ID,
    LISTAGG(COALESCE(NOTES, ''), ' ') WITHIN GROUP (ORDER BY NOTES) AS COMBINED_NOTES_FULL,
    SUBSTR(COMBINED_NOTES_FULL, 1, 6000) AS COMBINED_NOTES,
    LENGTH(COMBINED_NOTES_FULL) > 6000 AS IS_OVERSIZED
  FROM NOTES_DEDUPED
  WHERE CREATED_DATE IS NOT NULL AND NOTES IS NOT NULL AND TRIM(NOTES) <> ''
  GROUP BY MARKET, CREATED_DATE, FH_ID
),
-- =====================================================================
--  START OF AI PIPELINE (only runs on notes that are NOT oversized)
-- =====================================================================
NOTES_TO_PROCESS AS (
    SELECT * FROM NOTES_AGG WHERE NOT IS_OVERSIZED
),
SEED_PROMPT AS (
  SELECT $$Return ONLY a JSON object with key "sentiment_categories" holding an array of objects. Do not include any prose before or after the JSON object.$$ AS prompt_txt
),
SEED_SCORES AS (
  SELECT
    A.MARKET, A.CREATED_DATE, A.FH_ID, A.COMBINED_NOTES,
    REGEXP_SUBSTR(
      AI_COMPLETE(
        MODEL => 'mistral-large',
        PROMPT => CONCAT(CAST(SEED_PROMPT.prompt_txt AS STRING), '\nINTERACTION_DATE: ', CAST(A.CREATED_DATE AS STRING), '\n', CAST(A.COMBINED_NOTES AS STRING))
      ),
      '\\{.*\\}', 1, 1, 's'
    ) AS LLM_SEED_JSON
  FROM NOTES_TO_PROCESS A, SEED_PROMPT
),
SEED_FLAT AS (
  SELECT
    s.MARKET, s.CREATED_DATE, s.FH_ID, s.COMBINED_NOTES,
    v.value:category::STRING AS category,
    TRY_TO_NUMBER(TO_VARCHAR(v.value:score)) AS score
  FROM SEED_SCORES s,
       LATERAL FLATTEN(input => TRY_PARSE_JSON(s.LLM_SEED_JSON):sentiment_categories) v
  WHERE v.value IS NOT NULL AND LOWER(v.value:category::STRING) IN (
    'health','program_trust','self','risk_harm','social_stability','med_adherence','care_engagement'
  )
),
-- New CTE to create a complete scaffold of every person and every category
ALL_PERSONS_AND_CATEGORIES AS (
    SELECT DISTINCT
        sf.FH_ID,
        sf.MARKET,
        c.name AS category
    FROM SEED_FLAT sf
    CROSS JOIN (
        SELECT 'Health' AS name UNION ALL SELECT 'Program_Trust' UNION ALL SELECT 'Self' UNION ALL
        SELECT 'Risk_Harm' UNION ALL SELECT 'Social_Stability' UNION ALL SELECT 'Med_Adherence' UNION ALL SELECT 'Care_Engagement'
    ) c
),
-- The BASELINES CTE is now built from the complete scaffold, ensuring no missing categories.
BASELINES AS (
  SELECT DISTINCT
    scaffold.MARKET,
    scaffold.FH_ID,
    scaffold.category,
    AVG(sf.score) OVER (PARTITION BY scaffold.category) AS population_baseline_by_category,
    AVG(sf.score) OVER (PARTITION BY scaffold.MARKET, scaffold.category) AS market_baseline_by_category,
    AVG(sf.score) OVER (PARTITION BY scaffold.FH_ID, scaffold.category) AS individual_baseline_by_category
  FROM ALL_PERSONS_AND_CATEGORIES scaffold
  LEFT JOIN SEED_FLAT sf ON scaffold.FH_ID = sf.FH_ID AND scaffold.category = sf.category
),
BASELINES_JSON AS (
  SELECT
    FH_ID,
    OBJECT_CONSTRUCT(
      'individual_baselines', OBJECT_AGG(category, individual_baseline_by_category),
      'market_baselines', OBJECT_AGG(category, market_baseline_by_category),
      'population_baselines', OBJECT_AGG(category, population_baseline_by_category)
    ) AS baseline_obj
  FROM BASELINES
  GROUP BY FH_ID
),
PROMPTS AS (
  SELECT prompt_text AS rescore_prompt
  FROM TRANSFORMED_DATA._TEMP.AL_AI_PROMPTS
  WHERE prompt_name='rescore_prompt'
),
RESCORE AS (
  SELECT
    n.MARKET, n.CREATED_DATE, n.FH_ID, n.COMBINED_NOTES,
    p.rescore_prompt, bj.baseline_obj,
    REGEXP_SUBSTR(
      AI_COMPLETE(
        MODEL => 'mistral-large',
        PROMPT => CONCAT(
          CAST(p.rescore_prompt AS STRING),
          '\nINTERACTION_DATE: ', CAST(n.CREATED_DATE AS STRING),
          '\nBASELINES:\n', CAST(TO_JSON(bj.baseline_obj) AS STRING),
          '\nNOTES:\n', CAST(n.COMBINED_NOTES AS STRING)
        )
      ),
      '\\{.*\\}', 1, 1, 's'
    ) AS FINAL_JSON
  FROM NOTES_TO_PROCESS n
  JOIN BASELINES_JSON bj ON n.FH_ID = bj.FH_ID
  CROSS JOIN PROMPTS p
),
PROCESSED_FLAT AS (
  SELECT
    'PROCESSED' AS PROCESSING_STATUS,
    r.MARKET,
    r.CREATED_DATE AS INTERACTION_DATE,
    r.FH_ID,
    r.COMBINED_NOTES,
    r.FINAL_JSON,
    NULL AS CRITIQUE_JSON,
    TRY_TO_NUMBER(TO_VARCHAR(v.value:score)) AS score,
    TRY_TO_NUMBER(TO_VARCHAR(v.value:confidence)) AS confidence,
    v.value:category::STRING AS category,
    v.value:evidence::STRING AS evidence,
    v.value:interaction_date::DATE AS interaction_date_parsed,
    CAST(TO_JSON(r.baseline_obj) AS STRING) AS baselines_json
  FROM RESCORE r,
       LATERAL FLATTEN(input => TRY_PARSE_JSON(r.FINAL_JSON):sentiment_categories) v
  WHERE v.value IS NOT NULL AND LOWER(v.value:category::STRING) IN (
    'health','program_trust','self','risk_harm','social_stability','med_adherence','care_engagement'
  )
),
-- =====================================================================
--  END OF AI PIPELINE
-- =====================================================================

FLAGGED_DATA AS (
    SELECT
        'SKIPPED_OVERSIZED' AS PROCESSING_STATUS,
        MARKET,
        CREATED_DATE AS INTERACTION_DATE,
        FH_ID,
        COMBINED_NOTES_FULL AS COMBINED_NOTES,
        NULL AS FINAL_JSON,
        NULL AS CRITIQUE_JSON,
        NULL AS score,
        NULL AS confidence,
        c.name AS category,
        'NOTE WAS TOO LONG TO PROCESS' AS evidence,
        CREATED_DATE AS interaction_date_parsed,
        NULL AS baselines_json
    FROM NOTES_AGG
    CROSS JOIN (SELECT 'Health' as name UNION ALL SELECT 'Program_Trust' UNION ALL SELECT 'Self' UNION ALL SELECT 'Risk_Harm' UNION ALL SELECT 'Social_Stability' UNION ALL SELECT 'Med_Adherence' UNION ALL SELECT 'Care_Engagement') c
    WHERE IS_OVERSIZED
)

-- Final SELECT combines the processed results with the flagged data
SELECT
    PROCESSING_STATUS,
    MARKET,
    INTERACTION_DATE,
    FH_ID,
    COMBINED_NOTES,
    FINAL_JSON AS RESCORE_JSON,
    CRITIQUE_JSON,
    score,
    confidence,
    category,
    evidence,
    interaction_date_parsed,
    baselines_json
FROM PROCESSED_FLAT
UNION ALL
SELECT
    PROCESSING_STATUS,
    MARKET,
    INTERACTION_DATE,
    FH_ID,
    COMBINED_NOTES,
    FINAL_JSON AS RESCORE_JSON,
    CRITIQUE_JSON,
    score,
    confidence,
    category,
    evidence,
    interaction_date_parsed,
    baselines_json
FROM FLAGGED_DATA;