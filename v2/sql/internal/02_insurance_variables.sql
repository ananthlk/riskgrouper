-- Script 2: Insurance variables, product flags, disenrollment, market features
USE DATABASE TRANSFORMED_DATA;
USE SCHEMA _TEMP;

-- Insurance product mapping
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

-- Market feature engineering
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
    'canton','youngstown','memphis','dayton','richmond','cleveland','chattanooga','detroit','orlando','toledo',
    'upper east tn','akron','columbus','nashville','jacksonville','cincinnati','fairfax','support','tacoma','miami','knoxville','southwest virginia'
  ), 1, 0) AS is_market_other
FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS;
