-- 10_notes_derived_metrics.sql
-- Derived metrics from AL_REG_ATTRIBUTED_MEMBERS_NOTES
-- Flags for each category: present in month, and max score >= 3

CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_NOTES_DERIVED AS
WITH note_flags AS (
  SELECT
    FH_ID,
    DATE_TRUNC('MONTH', note_date) AS month_start,
    MAX(IFF(category = 'SELF', SCORE, NULL)) AS max_self_score,
    MAX(IFF(category = 'MED_ADHERENCE', SCORE, NULL)) AS max_med_adherence_score,
    MAX(IFF(category = 'HEALTH', SCORE, NULL)) AS max_health_score,
    MAX(IFF(category = 'CARE_ENGAGEMENT', SCORE, NULL)) AS max_care_engagement_score,
    MAX(IFF(category = 'PROGRAM_TRUST', SCORE, NULL)) AS max_program_trust_score,
    MAX(IFF(category = 'RISK_HARM', SCORE, NULL)) AS max_risk_harm_score,
    MAX(IFF(category = 'SOCIAL_STABILITY', SCORE, NULL)) AS max_social_stability_score,
    MAX(IFF(category = 'SELF', 1, 0)) AS self_present,
    MAX(IFF(category = 'MED_ADHERENCE', 1, 0)) AS med_adherence_present,
    MAX(IFF(category = 'HEALTH', 1, 0)) AS health_present,
    MAX(IFF(category = 'CARE_ENGAGEMENT', 1, 0)) AS care_engagement_present,
    MAX(IFF(category = 'PROGRAM_TRUST', 1, 0)) AS program_trust_present,
    MAX(IFF(category = 'RISK_HARM', 1, 0)) AS risk_harm_present,
    MAX(IFF(category = 'SOCIAL_STABILITY', 1, 0)) AS social_stability_present
  FROM TRANSFORMED_DATA._TEMP.AL_REG_ATTRIBUTED_MEMBERS_NOTES
  GROUP BY FH_ID, DATE_TRUNC('MONTH', note_date)
)
SELECT
  m.FH_ID,
  m.EFFECTIVE_MONTH_START,
  IFF(nf.self_present = 1, 1, 0) AS self_present_this_month,
  IFF(nf.max_self_score >= 3, 1, IFF(nf.self_present = 1, 0, NULL)) AS self_score_high_this_month,
  IFF(nf.med_adherence_present = 1, 1, 0) AS med_adherence_present_this_month,
  IFF(nf.max_med_adherence_score >= 3, 1, IFF(nf.med_adherence_present = 1, 0, NULL)) AS med_adherence_score_high_this_month,
  IFF(nf.health_present = 1, 1, 0) AS health_present_this_month,
  IFF(nf.max_health_score >= 3, 1, IFF(nf.health_present = 1, 0, NULL)) AS health_score_high_this_month,
  IFF(nf.care_engagement_present = 1, 1, 0) AS care_engagement_present_this_month,
  IFF(nf.max_care_engagement_score >= 3, 1, IFF(nf.care_engagement_present = 1, 0, NULL)) AS care_engagement_score_high_this_month,
  IFF(nf.program_trust_present = 1, 1, 0) AS program_trust_present_this_month,
  IFF(nf.max_program_trust_score >= 3, 1, IFF(nf.program_trust_present = 1, 0, NULL)) AS program_trust_score_high_this_month,
  IFF(nf.risk_harm_present = 1, 1, 0) AS risk_harm_present_this_month,
  IFF(nf.max_risk_harm_score >= 3, 1, IFF(nf.risk_harm_present = 1, 0, NULL)) AS risk_harm_score_high_this_month,
  IFF(nf.social_stability_present = 1, 1, 0) AS social_stability_present_this_month,
  IFF(nf.max_social_stability_score >= 3, 1, IFF(nf.social_stability_present = 1, 0, NULL)) AS social_stability_score_high_this_month
FROM TRANSFORMED_DATA._TEMP.INT_MEMBER_MONTHS_ORDERED m
LEFT JOIN note_flags nf ON m.FH_ID = nf.FH_ID AND m.EFFECTIVE_MONTH_START = nf.month_start;
