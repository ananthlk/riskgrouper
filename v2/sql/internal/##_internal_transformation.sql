-- Set database and schema context for temp table creation
USE DATABASE TRANSFORMED_DATA;
USE SCHEMA _TEMP;


-- Define source tables for attribution and member months
SET attribution_table = 'TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS';
SET member_months_table = 'TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MONTHS';

-- Set the analysis cutoff: only include data up to this month
SET global_max_month = '2024-12-01';


-- Step 1: Build ordered member months for each individual
-- Includes insurance product flags for future SDoH enrichment
CREATE OR REPLACE TEMP TABLE INT_MEMBER_MONTHS_ORDERED AS
SELECT
  a.FH_ID,
  m.EFFECTIVE_MONTH_START,
  m.EFFECTIVE_MONTH_END,
  -- Enrollment status: TRUE if continuously enrolled, FALSE otherwise
  COALESCE(m.IS_CONTINUOUSLY_ENROLLED, FALSE) AS IS_ENROLLED
FROM IDENTIFIER($attribution_table) a
JOIN IDENTIFIER($member_months_table) m ON a.FH_ID = m.FH_ID
WHERE m.EFFECTIVE_MONTH_START <= $global_max_month
ORDER BY a.FH_ID, m.EFFECTIVE_MONTH_START;


-- Step 1: Ordered member months for each individual
-- Step 2: Identify eligibility spans and disenrollment periods
-- Flags members as disenrolled if their last available month is before the global max
CREATE OR REPLACE TEMP TABLE INT_ELIGIBILITY_SPANS AS
WITH last_months AS (
  SELECT FH_ID, MAX(EFFECTIVE_MONTH_START) AS last_month, 
    CASE WHEN last_month < $global_max_month THEN 1 ELSE 0 END AS is_disenrolled
  FROM INT_MEMBER_MONTHS_ORDERED
  GROUP BY FH_ID
),
spans AS (
  SELECT
    m.FH_ID,
    m.EFFECTIVE_MONTH_START,
    m.EFFECTIVE_MONTH_END,
    m.IS_ENROLLED,
    -- Reinstatement: member was previously disenrolled and is now enrolled
    CASE WHEN m.IS_ENROLLED = TRUE AND LAG(m.IS_ENROLLED) OVER (PARTITION BY m.FH_ID ORDER BY m.EFFECTIVE_MONTH_START) = FALSE
         THEN 1 ELSE 0 END AS IS_REINSTATED,
    -- Disenrollment: member is not enrolled, or last available month is before global max
    CASE 
      WHEN (m.IS_ENROLLED = FALSE OR m.IS_ENROLLED IS NULL) THEN 1
      WHEN m.EFFECTIVE_MONTH_START = lm.last_month THEN lm.is_disenrolled
      ELSE 0
    END AS IS_DISENROLLED
  FROM INT_MEMBER_MONTHS_ORDERED m
  JOIN last_months lm ON m.FH_ID = lm.FH_ID
)
SELECT
  FH_ID,
  EFFECTIVE_MONTH_START,
  EFFECTIVE_MONTH_END,
  IS_ENROLLED,
  IS_REINSTATED,
  IS_DISENROLLED
FROM spans;


/*
===============================================================================
    STATIC MARKET FEATURE ENGINEERING
-------------------------------------------------------------------------------
    This block creates binary columns for each market in TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS,
    plus an 'other' column for null or unknown markets.
===============================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES AS
SELECT
  FH_ID,
  MARKET,
  IFF(MARKET = 'canton', 1, 0) AS is_market_canton,
  IFF(MARKET = 'youngstown', 1, 0) AS is_market_youngstown,
  IFF(MARKET = 'memphis', 1, 0) AS is_market_memphis,
  IFF(MARKET = 'dayton', 1, 0) AS is_market_dayton,
  IFF(MARKET = 'richmond', 1, 0) AS is_market_richmond,
  IFF(MARKET = 'cleveland', 1, 0) AS is_market_cleveland,
  IFF(MARKET = 'chattanooga', 1, 0) AS is_market_chattanooga,
  IFF(MARKET = 'detroit', 1, 0) AS is_market_detROIT,
  IFF(MARKET = 'orlando', 1, 0) AS is_market_orlando,
  IFF(MARKET = 'toledo', 1, 0) AS is_market_toledo,
  IFF(MARKET = 'upper east tn', 1, 0) AS is_market_upper_east_tn,
  IFF(MARKET = 'akron', 1, 0) AS is_market_akron,
  IFF(MARKET = 'columbus', 1, 0) AS is_market_columbus,
  IFF(MARKET = 'nashville', 1, 0) AS is_market_nashville,
  IFF(MARKET = 'jacksonville', 1, 0) AS is_market_jacksonville,
  IFF(MARKET = 'cincinnati', 1, 0) AS is_market_cincinnati,
  IFF(MARKET = 'fairfax', 1, 0) AS is_market_fairfax,
  IFF(MARKET = 'support', 1, 0) AS is_market_support,
  IFF(MARKET = 'tacoma', 1, 0) AS is_market_tacoma,
  IFF(MARKET = 'miami', 1, 0) AS is_market_miami,
  IFF(MARKET = 'knoxville', 1, 0) AS is_market_knoxville,
  IFF(MARKET = 'southwest virginia', 1, 0) AS is_market_southwest_virginia,
  IFF(MARKET IS NULL OR MARKET NOT IN (
    'canton','youngstown','memphis','dayton','richmond','cleveland','chattanooga','detROIT','orlando','toledo',
    'upper east tn','akron','columbus','nashville','jacksonville','cincinnati','fairfax','support','tacoma','miami','knoxville','southwest virginia'
  ), 1, 0) AS is_market_other
FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS;


/*
===============================================================================
    INSURANCE PRODUCT MAPPING
-------------------------------------------------------------------------------
    This block creates binary columns for insurance product flags in the member table.
    ABD, TANF, EXPANSION, DSNP, OTHER are mapped from fh_coverage_category.
===============================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS AS
SELECT
  *,
  IFF(LEFT(UPPER(TRIM(FH_COVERAGE_CATEGORY)), 3) = 'ABD', 1, 0) AS IS_ABD,
  IFF(LEFT(UPPER(TRIM(FH_COVERAGE_CATEGORY)), 4) = 'TANF', 1, 0) AS IS_TANF,
  IFF(LEFT(UPPER(TRIM(FH_COVERAGE_CATEGORY)), 3) = 'EXP', 1, 0) AS IS_EXPANSION,
  IFF(LEFT(UPPER(TRIM(FH_COVERAGE_CATEGORY)), 3) = 'DSN', 1, 0) AS IS_DSNP,
  IFF(
    LEFT(UPPER(TRIM(FH_COVERAGE_CATEGORY)), 3) NOT IN ('ABD', 'EXP', 'DSN')
    AND LEFT(UPPER(TRIM(FH_COVERAGE_CATEGORY)), 4) <> 'TANF',
    1, 0
  ) AS IS_OTHER
FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS;

-- Market feature engineering: binary columns for each market
-- Engagement status flags by month using INT_MEMBER_MONTHS_ORDERED
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT AS
SELECT
  m.FH_ID,
  mm.EFFECTIVE_MONTH_START,
  mm.EFFECTIVE_MONTH_END,
  -- Selected flags
  -- Engaged flags
  IFF(m.FIRST_ENGAGED_DATE IS NOT NULL AND mm.EFFECTIVE_MONTH_START <= $global_max_month, 1, 0) AS is_ever_engaged,
  IFF(m.FIRST_ENGAGED_DATE IS NOT NULL AND DATE_TRUNC('MONTH', m.FIRST_ENGAGED_DATE) = mm.EFFECTIVE_MONTH_START AND mm.EFFECTIVE_MONTH_START <= $global_max_month, 1, 0) AS is_engaged_month
FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS m
JOIN INT_MEMBER_MONTHS_ORDERED mm
  ON m.FH_ID = mm.FH_ID;

-- Step 3: Summary statistics for eligibility, insurance product flags, and market breakdown

-- Step 3 & 4: Combined validation query for summary statistics and engagement flags (display as rows)
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.O AS
SELECT 'unique_members' AS flag, COUNT(DISTINCT s.FH_ID) AS value FROM INT_ELIGIBILITY_SPANS s UNION ALL
SELECT 'unique_disenrolled_members', COUNT(DISTINCT s.FH_ID) FROM INT_ELIGIBILITY_SPANS s WHERE s.IS_DISENROLLED = 1 UNION ALL
SELECT 'abd_members', COUNT(DISTINCT memb.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS memb WHERE memb.IS_ABD = 1 UNION ALL
SELECT 'tanf_members', COUNT(DISTINCT memb.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS memb WHERE memb.IS_TANF = 1 UNION ALL
SELECT 'expansion_members', COUNT(DISTINCT memb.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS memb WHERE memb.IS_EXPANSION = 1 UNION ALL
SELECT 'dsnp_members', COUNT(DISTINCT memb.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS memb WHERE memb.IS_DSNP = 1 UNION ALL
SELECT 'other_members', COUNT(DISTINCT memb.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS memb WHERE memb.IS_OTHER = 1 UNION ALL
SELECT 'market_canton_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_canton = 1 UNION ALL
SELECT 'market_youngstown_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_youngstown = 1 UNION ALL
SELECT 'market_memphis_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_memphis = 1 UNION ALL
SELECT 'market_dayton_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_dayton = 1 UNION ALL
SELECT 'market_richmond_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_richmond = 1 UNION ALL
SELECT 'market_cleveland_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_cleveland = 1 UNION ALL
SELECT 'market_chattanooga_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_chattanooga = 1 UNION ALL
SELECT 'market_detROIT_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_detROIT = 1 UNION ALL
SELECT 'market_orlando_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_orlando = 1 UNION ALL
SELECT 'market_toledo_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_toledo = 1 UNION ALL
SELECT 'market_upper_east_tn_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_upper_east_tn = 1 UNION ALL
SELECT 'market_akron_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_akron = 1 UNION ALL
SELECT 'market_columbus_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_columbus = 1 UNION ALL
SELECT 'market_nashville_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_nashville = 1 UNION ALL
SELECT 'market_jacksonville_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_jacksonville = 1 UNION ALL
SELECT 'market_cincinnati_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_cincinnati = 1 UNION ALL
SELECT 'market_fairfax_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_fairfax = 1 UNION ALL
SELECT 'market_support_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_support = 1 UNION ALL
SELECT 'market_tacoma_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_tacoma = 1 UNION ALL
SELECT 'market_miami_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_miami = 1 UNION ALL
SELECT 'market_knoxville_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_knoxville = 1 UNION ALL
SELECT 'market_southwest_virginia_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_southwest_virginia = 1 UNION ALL
SELECT 'market_other_members', COUNT(DISTINCT m.FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES m WHERE m.is_market_other = 1 UNION ALL
SELECT 'total_members', COUNT(DISTINCT FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT UNION ALL
SELECT 'ever_selected_members', COUNT(DISTINCT FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT WHERE is_ever_selected = 1 UNION ALL
SELECT 'selected_month_members', COUNT(DISTINCT FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT WHERE is_selected_month = 1 UNION ALL
SELECT 'ever_engaged_members', COUNT(DISTINCT FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT WHERE is_ever_engaged = 1 UNION ALL
SELECT 'engaged_month_members', COUNT(DISTINCT FH_ID) FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT WHERE is_engaged_month = 1;

CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_CONTACT_FLAGS AS
  SELECT
    FH_ID,
    CONTACT_DATE,
    STATUS,
    DATE_TRUNC('MONTH', CONTACT_DATE) AS CONTACT_MONTH
  FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_CONTACT_ATTEMPTS
  WHERE CONTACT_SUBJECT = 'individual'
),
current_month AS (
  SELECT DATE_TRUNC('MONTH', CURRENT_DATE()) AS month_start
),
base AS (
  SELECT DISTINCT FH_ID FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS
),
agg AS (
  SELECT
    b.FH_ID,
    -- Ever successful contact
    MAX(IFF(ca.STATUS = 'successful', 1, 0)) AS has_ever_successful_contact,
    -- Successful contact this month
    MAX(IFF(ca.STATUS = 'successful' AND ca.CONTACT_MONTH = cm.month_start, 1, 0)) AS has_successful_contact_this_month,
    -- Success rate this month
    IFF(SUM(IFF(ca.CONTACT_MONTH = cm.month_start, 1, 0)) > 0,
      SUM(IFF(ca.STATUS = 'successful' AND ca.CONTACT_MONTH = cm.month_start, 1, 0)) / SUM(IFF(ca.CONTACT_MONTH = cm.month_start, 1, 0)),
      NULL) AS contact_success_rate_this_month,
    -- Success rate last 3 months
    IFF(SUM(IFF(ca.CONTACT_MONTH >= DATEADD('MONTH', -2, cm.month_start) AND ca.CONTACT_MONTH <= cm.month_start, 1, 0)) > 0,
      SUM(IFF(ca.STATUS = 'successful' AND ca.CONTACT_MONTH >= DATEADD('MONTH', -2, cm.month_start) AND ca.CONTACT_MONTH <= cm.month_start, 1, 0)) /
      SUM(IFF(ca.CONTACT_MONTH >= DATEADD('MONTH', -2, cm.month_start) AND ca.CONTACT_MONTH <= cm.month_start, 1, 0)),
      NULL) AS contact_success_rate_3month
  FROM base b
  LEFT JOIN contact_attempts ca ON b.FH_ID = ca.FH_ID
  CROSS JOIN current_month cm
  GROUP BY b.FH_ID, cm.month_start
),
final AS (
  SELECT
    *,
    CASE
      WHEN contact_success_rate_this_month IS NULL OR contact_success_rate_3month IS NULL THEN NULL
      WHEN contact_success_rate_this_month > contact_success_rate_3month THEN 'up'
      WHEN contact_success_rate_this_month < contact_success_rate_3month THEN 'down'
      WHEN contact_success_rate_this_month = contact_success_rate_3month THEN 'same'
      ELSE NULL
    END AS contact_success_trend
  FROM agg
)
SELECT * FROM final;



