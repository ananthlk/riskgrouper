/*
============================================================================
PRODUCTION-READY: Creation of AI Baseline Scoring Table
PURPOSE: This script uses a pre-defined LLM prompt to score care team notes,
         then calculates baselines for each score across different cohorts.

         The process involves:
         1.  Collecting and deduplicating notes from various sources.
         2.  Aggregating notes by member and date.
         3.  Fetching a pre-defined prompt from the 'AL_AI_PROMPTS' table.
         4.  Calling the LLM to generate scores for each note.
         5.  Flattening the LLM's JSON output into a structured table.
         6.  Calculating population, market, and individual baselines for each score.
============================================================================
*/

-- Step 0: Get Member Cohort
CREATE OR REPLACE TEMPORARY TABLE MEMBER_COHORT AS
SELECT
    A.FH_ID,
    A.PAT_AGE,
    A.PAT_GENDER,
    A.Market,
    A.has_ever_been_engaged,
    DATEDIFF(month, A.batch_date, CURRENT_DATE) AS months_since_batched,
    A.FH_COVERAGE_CATEGORY,
    ELATION_MRN
FROM TRANSFORMED_DATA.PROD.FH_MEMBERS A
LEFT OUTER JOIN TRANSFORMED_DATA.PROD.FH_MEMBER_SELECTION_QUALIFICATION c
    ON a.fh_id = c.fh_id
WHERE A.is_batched = TRUE
    AND c.is_fh_clinically_qualified = 1
    --AND batch_date >= '2024-07-01'
    --AND market = 'tacoma'
;

-- Step 1: Collect and Deduplicate Notes
WITH NOTES_DEDUPED AS (
    SELECT DISTINCT * FROM (
        -- guide notes
        SELECT TO_NUMBER(A.FH_ID) AS FH_ID, DATE(A.CREATED_AT) AS CREATED_DATE, MMS.MARKET, A.CONTENTS AS NOTES
        FROM MEMBER_COHORT MMS
        LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.NOTES A ON A.FH_ID = MMS.FH_ID
        UNION ALL
        -- non-visit notes
        SELECT TO_NUMBER(A.FH_ID), DATE(A.CREATED_AT), MMS.MARKET, A.CONTENT
        FROM MEMBER_COHORT MMS
        LEFT JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.NON_VISIT_NOTES A ON A.FH_ID = MMS.FH_ID
        UNION ALL
        -- phq9
        SELECT A.FH_ID, DATE(C.QUESTIONNAIRE_COMPLETED_AT), A.MARKET, CONCAT('question: ', C.QUESTION_NAME, ' ; ', 'score : ', C.SCORE)
        FROM MEMBER_COHORT A
        LEFT JOIN TRANSFORMED_DATA.PROD.STG_ELATION_CLINICAL_FORM_ANSWERS C ON A.ELATION_MRN = C.ELATION_PATIENT_ID
        WHERE QUESTIONNAIRE_NAME = 'PHQ-9 Questionnaire'
        UNION ALL
        -- healthchecks
        SELECT to_number(A.fh_id) as fh_id, IFNULL(DATE(A._FIVETRAN_SYNCED),'2001-01-01') as created_date, MMS.MARKET,CONCAT(
        'HAS_RECENT_HOSPITALIZATION:',IFNULL(to_varchar(HAS_RECENT_HOSPITALIZATION),' '),',', 'NOTES:',IFNULL(NOTES,' '),',',
        'IS_HEALTH_DECLINING:',IFNULL(to_varchar(IS_HEALTH_DECLINING),' '),',',
        'INDIVIDUAL_HAS_HEALTH_CONCERNS',IFNULL(to_varchar(INDIVIDUAL_HAS_HEALTH_CONCERNS),' '),',',
        'INDIVIDUAL_HAS_MEDICATION_CONCERNS:',IFNULL(to_varchar(INDIVIDUAL_HAS_MEDICATION_CONCERNS),' --'),',') AS NOTES
        FROM MEMBER_COHORT MMS
        LEFT OUTER JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.COMMUNITY_TEAM_HEALTH_CHECKS A ON a.fh_id = mms.fh_id
        UNION ALL
        -- POMS
        SELECT
        MMS.fh_id, date(a.created_at) as created_date,MMS.MARKET, CONCAT (LABEL,' : ', IFF(ANSWER = 'np','not present','present')) as notes
        FROM MEMBER_COHORT MMS
        LEFT OUTER JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.ASSESSMENTS A ON mms.fh_id = a.fh_id
        LEFT OUTER JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.ASSESSMENT_ANSWERS b ON a.id = b.ASSESSMENT_ID
        LEFT OUTER JOIN PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.POM_QUESTIONS c ON c.id = b.question_id
        LEFT OUTER JOIN TRANSFORMED_DATA.PROD_MARTS.MARTS_ID_STRAT_ANALYTICS e ON mms.fh_id = e.fh_id
    )
),

-- Step 2: Aggregate Notes by Member and Date
NOTES_AGG AS (
    SELECT
        MARKET,
        CREATED_DATE,
        FH_ID,
        LISTAGG(COALESCE(NOTES, ''), ' ') WITHIN GROUP (ORDER BY NOTES) AS COMBINED_NOTES
    FROM NOTES_DEDUPED
    WHERE CREATED_DATE IS NOT NULL AND NOTES IS NOT NULL AND TRIM(NOTES) <> ''
    GROUP BY MARKET, CREATED_DATE, FH_ID
),

-- Step 3: Fetch the LLM Generator Prompt from the Prompts Table
GENERATOR_PROMPT AS (
    SELECT
        prompt_text AS prompt_template
    FROM TRANSFORMED_DATA._TEMP.AL_AI_PROMPTS
    WHERE prompt_name = 'generator_prompt'
),

-- Step 4: Call the Generator Agent
INITIAL_ANALYSIS AS (
    SELECT
        A.MARKET,
        A.CREATED_DATE,
        A.COMBINED_NOTES,
        A.FH_ID,
        GENERATOR_PROMPT.prompt_template,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            REPLACE(REPLACE(GENERATOR_PROMPT.prompt_template, '{{INTERACTION_DATE}}', A.CREATED_DATE::STRING), '{{NOTES}}', A.COMBINED_NOTES)
        ) AS LLM_GENERATED_RESPONSE
    FROM NOTES_AGG A
    LEFT JOIN GENERATOR_PROMPT ON 1=1
),

-- Step 5: Flatten the AI's JSON Response
FLATTENED_SCORES AS (
    SELECT
        r.MARKET,
        r.FH_ID,
        r.CREATED_DATE AS source_interaction_date,
        f.value:category::STRING AS category,
        TRY_CAST(f.value:score::STRING AS NUMBER) AS score,
        TRY_CAST(f.value:confidence::STRING AS NUMBER) AS confidence,
        f.value:evidence::STRING AS evidence,
        r.COMBINED_NOTES,
        r.LLM_GENERATED_RESPONSE AS raw_response
    FROM INITIAL_ANALYSIS r,
         LATERAL FLATTEN(input => TRY_PARSE_JSON(r.LLM_GENERATED_RESPONSE)) AS f
),

-- Step 6: Calculate Baselines Using Window Functions
OUTPUT_WITH_BASELINES AS (
    SELECT
        s.*,
        -- Population Baseline: The average score for each category across all markets.
        AVG(s.score) OVER (PARTITION BY s.category) AS population_baseline,
        -- Market Baseline: The average score for each category within each market.
        AVG(s.score) OVER (PARTITION BY s.MARKET, s.category) AS market_baseline,
        -- Individual Baseline: The average score for each category for each individual.
        AVG(s.score) OVER (PARTITION BY s.FH_ID, s.category) AS individual_baseline

    FROM
        FLATTENED_SCORES s
)

-- Final SELECT to build the table
SELECT * FROM OUTPUT_WITH_BASELINES;
