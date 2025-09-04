-- Script 3: Engagement metrics and contact flags
USE DATABASE TRANSFORMED_DATA;
USE SCHEMA _TEMP;

SET global_max_month = '2024-12-01';
SET member_months_table = 'TRANSFORMED_DATA._TEMP.INT_MEMBER_MONTHS_ORDERED';


CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT AS

SELECT
  m.FH_ID,
  mm.EFFECTIVE_MONTH_START,
  mm.EFFECTIVE_MONTH_END,
  IFF(m.FIRST_SELECTED_DATE IS NOT NULL AND mm.EFFECTIVE_MONTH_START <= $global_max_month, 1, 0) AS is_ever_selected,
  IFF(m.FIRST_SELECTED_DATE IS NOT NULL AND DATE_TRUNC('MONTH', m.FIRST_SELECTED_DATE) = mm.EFFECTIVE_MONTH_START AND mm.EFFECTIVE_MONTH_START <= $global_max_month, 1, 0) AS is_selected_month,
  IFF(m.FIRST_ENGAGED_DATE IS NOT NULL AND mm.EFFECTIVE_MONTH_START <= $global_max_month, 1, 0) AS is_ever_engaged,
  IFF(m.FIRST_ENGAGED_DATE IS NOT NULL AND DATE_TRUNC('MONTH', m.FIRST_ENGAGED_DATE) = mm.EFFECTIVE_MONTH_START AND mm.EFFECTIVE_MONTH_START <= $global_max_month, 1, 0) AS is_engaged_month
FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS m
JOIN INT_MEMBER_MONTHS_ORDERED mm ON m.FH_ID = mm.FH_ID;


CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_CONTACT_FLAGS AS
WITH contact_attempts AS (
  SELECT
    FH_ID,
    date(DATE_TRUNC('MONTH', CONTACT_DATE)) AS CONTACT_MONTH,
    STATUS
  FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_CONTACT_ATTEMPTS
  WHERE CONTACT_SUBJECT = 'individual'
)
SELECT
  mm.FH_ID,
  mm.EFFECTIVE_MONTH_START,
  sum( case when ca.STATUS ='successful' then 1 else 0 end) AS successful_contact_attempts,
  sum( case when ca.STATUS ='unsuccessful' then 1 else 0 end) AS unsuccessful_contact_attempts,
  COALESCE(successful_contact_attempts + unsuccessful_contact_attempts,0)  as total_contact_attempts,
  case when total_contact_attempts >1 then 1 else 0 end as is_tried_contact_in_month,
  case when successful_contact_attempts >1 then 1 else 0 end as is_succefful_contact_in_month,
  case when total_contact_attempts >0 then successful_contact_attempts/total_contact_attempts else 0 end as success_rate_this_month,
  case when total_contact_attempts >=10 then 1 else 0 end as is_intense_attempt_in_month,
  case when successful_contact_attempts >=5 then 1 else 0 end as is_intense_support_in_month
FROM IDENTIFIER($member_months_table) mm
LEFT JOIN contact_attempts ca ON mm.FH_ID = ca.FH_ID and mm.effective_month_start = CONTACT_MONTH
GROUP BY mm.FH_ID, mm.EFFECTIVE_MONTH_START;

-- Phone responsiveness metric
-- If there is ever a missed call (duration <= 1) and no successful call within 2 days after, mark is_potentially_not_responsive = 1

CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_PHONE_RESPONSIVENESS AS
WITH missed_calls AS (
  SELECT
    FH_ID,
    DATE_TRUNC('MONTH', TO_DATE(DT1)) AS missed_call_month,
    TO_DATE(DT1) AS missed_call_date
  FROM TRANSFORMED_DATA._TEMP.AL_VERIZON_PHONE_LOG
  WHERE DURATION <= 1
),
followup_calls AS (
  SELECT
    m.FH_ID,
    m.missed_call_month,
    m.missed_call_date,
    MIN(v.DT1) AS first_followup_date
  FROM missed_calls m
  LEFT JOIN TRANSFORMED_DATA._TEMP.AL_VERIZON_PHONE_LOG v
    ON m.FH_ID = v.FH_ID
    AND v.DURATION > 1
    AND v.DT1 >= m.missed_call_date
    AND v.DT1 <= DATEADD('DAY', 2, m.missed_call_date)
  GROUP BY m.FH_ID, m.missed_call_month, m.missed_call_date
),
responsive_flags AS (
  SELECT
    FH_ID,
    missed_call_month,
    MAX(IFF(first_followup_date IS NULL, 1, 0)) AS is_potentially_not_responsive
  FROM followup_calls
  GROUP BY FH_ID, missed_call_month
)
SELECT
  mm.FH_ID,
  mm.EFFECTIVE_MONTH_START,
  COALESCE(rf.is_potentially_not_responsive, 0) AS is_potentially_not_responsive
FROM IDENTIFIER($member_months_table) mm
LEFT JOIN responsive_flags rf ON mm.FH_ID = rf.FH_ID AND rf.missed_call_month = mm.EFFECTIVE_MONTH_START;


-- SMS engagement flags
-- (a) texted in a month, (b) likes to converse by text (5+ interactions on a day)
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_SMS_FLAGS AS
WITH sms_months AS (
  SELECT
    FH_ID,
    DATE_TRUNC('MONTH', TO_DATE(DT1)) AS sms_month
  FROM TRANSFORMED_DATA._TEMP.AL_VERIZON_sms_log
),
converse_days AS (
  SELECT
    FH_ID,
    TO_DATE(DT1) AS sms_date,
    COUNT(*) AS sms_count
  FROM TRANSFORMED_DATA._TEMP.AL_VERIZON_sms_log
  GROUP BY FH_ID, TO_DATE(DT1)
),
converse_flags AS (
  SELECT
    FH_ID,
    sms_date,
    IFF(sms_count >= 5, 1, 0) AS likes_to_converse_by_text
  FROM converse_days
)
SELECT
  mm.FH_ID,
  mm.EFFECTIVE_MONTH_START,
  IFF(EXISTS (
    SELECT 1 FROM sms_months s WHERE s.FH_ID = mm.FH_ID AND s.sms_month = mm.EFFECTIVE_MONTH_START
  ), 1, 0) AS texted_in_month,
  IFF(EXISTS (
    SELECT 1 FROM converse_flags cf WHERE cf.FH_ID = mm.FH_ID AND cf.likes_to_converse_by_text = 1 AND DATE_TRUNC('MONTH', cf.sms_date) = mm.EFFECTIVE_MONTH_START
  ), 1, 0) AS likes_to_converse_by_text_in_month
FROM IDENTIFIER($member_months_table) mm;

-- FINAL: Comprehensive Engagement Trends Table
-- This table combines all month-over-month engagement trend flags for attributed members.
-- Includes contact attempt metrics, responsiveness, missed calls, texting, and success rate changes.

CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT_TRENDS AS
SELECT
  c.FH_ID, -- Member ID
  c.EFFECTIVE_MONTH_START, -- Month for trend calculation

  -- Engagement Flags
  eng.is_ever_selected,
  eng.is_selected_month,
  eng.is_ever_engaged,
  eng.is_engaged_month,

  -- Contact attempt metrics
  c.total_contact_attempts, -- Total contact attempts in month
  c.successful_contact_attempts, -- Successful contact attempts in month
  c.unsuccessful_contact_attempts, -- Unsuccessful contact attempts in month
  c.success_rate_this_month, -- Success rate for contact attempts in month
  c.is_tried_contact_in_month,
  c.is_succefful_contact_in_month,
  c.is_intense_attempt_in_month,
  c.is_intense_support_in_month,

  -- Responsiveness trend flags
  CASE WHEN LAG(pr.is_potentially_not_responsive, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 0 AND pr.is_potentially_not_responsive = 1 THEN 1 ELSE 0 END AS became_non_responsive,
  CASE WHEN LAG(pr.is_potentially_not_responsive, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 1 AND pr.is_potentially_not_responsive = 0 THEN 1 ELSE 0 END AS became_responsive,

  pr.is_potentially_not_responsive - LAG(pr.is_potentially_not_responsive, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) AS increase_in_missed_calls,
  CASE WHEN pr.is_potentially_not_responsive = 1 AND LAG(pr.is_potentially_not_responsive, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 0 THEN 1 ELSE 0 END AS new_missed_calls,

  -- SMS texting trend flags
  CASE WHEN LAG(sf.texted_in_month, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 0 AND sf.texted_in_month = 1 THEN 1 ELSE 0 END AS started_texting,
  CASE WHEN LAG(sf.texted_in_month, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 1 AND sf.texted_in_month = 0 THEN 1 ELSE 0 END AS stopped_texting,
  CASE WHEN LAG(sf.likes_to_converse_by_text_in_month, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 0 AND sf.likes_to_converse_by_text_in_month = 1 THEN 1 ELSE 0 END AS started_intense_texting,
  CASE WHEN LAG(sf.likes_to_converse_by_text_in_month, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) = 1 AND sf.likes_to_converse_by_text_in_month = 0 THEN 1 ELSE 0 END AS stopped_intense_texting,

  CASE WHEN c.success_rate_this_month - LAG(c.success_rate_this_month, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) >= 0.2 THEN 1 ELSE 0 END AS success_rate_increased,
  CASE WHEN c.success_rate_this_month - LAG(c.success_rate_this_month, 1) OVER (PARTITION BY c.FH_ID ORDER BY c.EFFECTIVE_MONTH_START) <= -0.2 THEN 1 ELSE 0 END AS success_rate_decreased
FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_CONTACT_FLAGS c
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT eng
  ON c.FH_ID = eng.FH_ID AND c.EFFECTIVE_MONTH_START = eng.EFFECTIVE_MONTH_START
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_PHONE_RESPONSIVENESS pr
  ON c.FH_ID = pr.FH_ID AND c.EFFECTIVE_MONTH_START = pr.EFFECTIVE_MONTH_START
LEFT JOIN TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_SMS_FLAGS sf
  ON c.FH_ID = sf.FH_ID AND c.EFFECTIVE_MONTH_START = sf.EFFECTIVE_MONTH_START;
