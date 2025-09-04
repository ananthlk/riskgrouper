WITH visit_events AS (
    -- Step 1: Identify all ED and IP claim events from the core claim lines table.
    SELECT
        claim_id,
        (CMS_PLACE_OF_SERVICE_CODE = '23' OR CMS_REVENUE_CENTER_CODE IN ('0450', '0451', '0452', '0456', '0459', '0981')) AS is_ed_event,
        (CMS_PLACE_OF_SERVICE_CODE = '21' OR CMS_REVENUE_CENTER_CODE IN ('0100', '0101', '0110', '0111', '0112', '0113', '0114', '0120', '0121', '0122', '0123', '0124', '0130', '0131', '0132', '0133', '0134', '0140', '0141', '0142', '0143', '0144', '0150', '0151', '0152', '0153', '0154', '0160', '0164', '0170', '0171', '0172', '0173', '0174', '0179', '0200', '0201', '0202', '0203', '0204', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0214', '0219')) AS is_ip_event
    FROM TRANSFORMED_DATA.PROD_CORE.CORE_MEDICAL_CLAIM_LINES
    WHERE is_ed_event OR is_ip_event
),
diagnoses AS (
    -- Step 2: Join visit events with their corresponding diagnoses and map ICD codes to HCCs.
    SELECT
        v.claim_id,
        hcc.HCC_CODE,
        d.ICD10CM_CODE,
        d.ICD10CM_DESC,
        v.is_ed_event,
        v.is_ip_event
    FROM visit_events v
    JOIN TRANSFORMED_DATA.PROD_TRANSFORM.CORE_MEDICAL_CLAIM_DIAGNOSIS_LINE_ITEMS d
        ON v.claim_id = d.URSA_CLAIM_ID
    JOIN TRANSFORMED_DATA.PROD_STAGING.DIM_ICD_CMS_HCC_MAP hcc
        ON d.ICD10CM_CODE = hcc.DIAGNOSIS_CODE
    WHERE hcc.HCC_CODE IS NOT NULL
),
RankedHCCs AS (
    -- Step 3: Rank HCCs by frequency for ED and IP visits separately.
    SELECT
        HCC_CODE,
        ROW_NUMBER() OVER (PARTITION BY visit_type ORDER BY COUNT(*) DESC) as rn
    FROM (
        SELECT HCC_CODE, 'ED' AS visit_type FROM diagnoses WHERE is_ed_event
        UNION ALL
        SELECT HCC_CODE, 'IP' AS visit_type FROM diagnoses WHERE is_ip_event
    ) AS visit_diagnoses
    GROUP BY HCC_CODE, visit_type
),
TopHCCs AS (
    -- Step 4: Select the top 5 HCCs for each visit type.
    SELECT DISTINCT HCC_CODE
    FROM RankedHCCs
    WHERE rn <= 5
),
RankedICDs AS (
    -- Step 5: For the top HCCs, find the most frequent associated ICD codes.
    SELECT
        d.HCC_CODE,
        d.ICD10CM_CODE,
        d.ICD10CM_DESC,
        COUNT(*) as icd_count,
        ROW_NUMBER() OVER(PARTITION BY d.HCC_CODE ORDER BY COUNT(*) DESC) as rn
    FROM diagnoses d
    JOIN TopHCCs t ON d.HCC_CODE = t.HCC_CODE
    GROUP BY 1, 2, 3
),
Diabetes_HCCs AS (
    -- Find HCC codes related to a specific condition by searching ICD-10 descriptions.
    -- This query is for: Diabetes
    SELECT DISTINCT
        hcc.HCC_CODE
    FROM TRANSFORMED_DATA.PROD_STAGING.DIM_ICD_CMS_HCC_MAP hcc
    JOIN TRANSFORMED_DATA.PROD_TRANSFORM.CORE_MEDICAL_CLAIM_DIAGNOSIS_LINE_ITEMS d
        ON hcc.DIAGNOSIS_CODE = d.ICD10CM_CODE
    WHERE UPPER(d.ICD10CM_DESC) LIKE '%DIABETES%'
)
-- Final Step: Display the top 5 ICD codes and their descriptions for each of the top HCCs.
SELECT
    HCC_CODE,
    ICD10CM_CODE,
    ICD10CM_DESC,
    icd_count
FROM RankedICDs
WHERE rn <= 5
ORDER BY HCC_CODE, rn;
