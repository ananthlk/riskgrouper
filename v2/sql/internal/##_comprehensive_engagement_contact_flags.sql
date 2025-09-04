-- Comprehensive Engagement & Contact Flags Query
-- Combines all engineered flags and metrics for attributed members by month

USE DATABASE TRANSFORMED_DATA;
USE SCHEMA _TEMP;

SELECT
  mm.FH_ID,
  mm.EFFECTIVE_MONTH_START,
  -- Engagement Flags
  eng.is_ever_selected,
  eng.is_selected_month,
  eng.is_ever_engaged,
  eng.is_engaged_month,
  -- Contact Flags
  cf.successful_contact_attempts,
  cf.unsuccessful_contact_attempts,
  cf.total_contact_attempts,
  cf.is_tried_contact_in_month,
  cf.is_succefful_contact_in_month,
  cf.success_rate_this_month,
  cf.is_intense_attempt_in_month,
  cf.is_intense_support_in_month,
  -- Phone Responsiveness
  pr.is_potentially_not_responsive,
  -- SMS Engagement
  sf.texted_in_month,
  sf.likes_to_converse_by_text_in_month
FROM IDENTIFIER($member_months_table) mm
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT eng
  ON mm.FH_ID = eng.FH_ID AND mm.EFFECTIVE_MONTH_START = eng.EFFECTIVE_MONTH_START
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_CONTACT_FLAGS cf
  ON mm.FH_ID = cf.FH_ID AND mm.EFFECTIVE_MONTH_START = cf.EFFECTIVE_MONTH_START
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_PHONE_RESPONSIVENESS pr
  ON mm.FH_ID = pr.FH_ID AND mm.EFFECTIVE_MONTH_START = pr.EFFECTIVE_MONTH_START
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_SMS_FLAGS sf
  ON mm.FH_ID = sf.FH_ID AND mm.EFFECTIVE_MONTH_START = sf.EFFECTIVE_MONTH_START;
