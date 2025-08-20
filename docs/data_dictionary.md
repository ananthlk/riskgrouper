# Data Dictionary

This document contains the schema for all tables used in the SQL scripts.

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.ASSESSMENTS `

| Column Name       | Data Type       | Description   |
|:------------------|:----------------|:--------------|
| SCORE             | VARCHAR(256)    |               |
| COMPLETED_AT      | TIMESTAMP_TZ(9) |               |
| UPDATED_AT        | TIMESTAMP_TZ(9) |               |
| SUBJECT_MRN       | VARCHAR(256)    |               |
| STAFF_EMR_KEY     | VARCHAR(256)    |               |
| CREATED_AT        | TIMESTAMP_TZ(9) |               |
| ID                | VARCHAR(256)    |               |
| TYPE              | VARCHAR(256)    |               |
| _FIVETRAN_DELETED | BOOLEAN         |               |
| _FIVETRAN_SYNCED  | TIMESTAMP_TZ(9) |               |
| FH_ID             | NUMBER(38,0)    |               |
| AUTHOR_USER_ID    | VARCHAR(256)    |               |

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.ASSESSMENT_ANSWERS `

| Column Name       | Data Type       | Description   |
|:------------------|:----------------|:--------------|
| ANSWER            | VARCHAR(256)    |               |
| UPDATED_AT        | TIMESTAMP_TZ(9) |               |
| CREATED_AT        | TIMESTAMP_TZ(9) |               |
| ASSESSMENT_ID     | VARCHAR(256)    |               |
| ID                | VARCHAR(256)    |               |
| QUESTION_ID       | VARCHAR(256)    |               |
| _FIVETRAN_DELETED | BOOLEAN         |               |
| _FIVETRAN_SYNCED  | TIMESTAMP_TZ(9) |               |

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.COMMUNITY_TEAM_HEALTH_CHECKS `

| Column Name                        | Data Type       | Description   |
|:-----------------------------------|:----------------|:--------------|
| ID                                 | VARCHAR(256)    |               |
| AUTHOR_USER_ID                     | VARCHAR(256)    |               |
| FH_ID                              | NUMBER(38,0)    |               |
| CREATED_AT                         | TIMESTAMP_TZ(9) |               |
| HAS_RECENT_HOSPITALIZATION         | BOOLEAN         |               |
| FACILITY                           | VARCHAR(256)    |               |
| PROVIDER                           | VARCHAR(256)    |               |
| HOSPITALIZATION_TYPE               | VARCHAR(256)    |               |
| ADMISSION_DATE                     | TIMESTAMP_TZ(9) |               |
| DISCHARGE_DATE                     | TIMESTAMP_TZ(9) |               |
| VISIT_TYPE                         | VARCHAR(256)    |               |
| VITALS_COLLECTED                   | BOOLEAN         |               |
| BLOOD_PRESSURE_SYSTOLIC            | NUMBER(38,0)    |               |
| BLOOD_PRESSURE_DIASTOLIC           | NUMBER(38,0)    |               |
| HEART_RATE                         | NUMBER(38,0)    |               |
| OXYGEN_SATURATION                  | NUMBER(38,0)    |               |
| NOTES                              | VARCHAR(4096)   |               |
| _FIVETRAN_DELETED                  | BOOLEAN         |               |
| _FIVETRAN_SYNCED                   | TIMESTAMP_TZ(9) |               |
| TEMPERATURE                        | FLOAT           |               |
| IS_HEALTH_DECLINING                | VARCHAR(256)    |               |
| INDIVIDUAL_HAS_HEALTH_CONCERNS     | VARCHAR(256)    |               |
| INDIVIDUAL_HAS_MEDICATION_CONCERNS | VARCHAR(256)    |               |
| SEARCH_DOCUMENT                    | VARCHAR(4096)   |               |
| NO_OUTREACH_REQUIRED_REASON        | VARCHAR(256)    |               |
| IS_CURRENTLY_HOSPITALIZED          | BOOLEAN         |               |
| HOSPITAL_ADMISSION_ID              | VARCHAR(256)    |               |

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.HOSPITAL_ADMISSIONS `

| Column Name         | Data Type       | Description   |
|:--------------------|:----------------|:--------------|
| ID                  | VARCHAR(256)    |               |
| FH_ID               | NUMBER(38,0)    |               |
| CREATED_AT          | TIMESTAMP_TZ(9) |               |
| UPDATED_AT          | TIMESTAMP_TZ(9) |               |
| ADMIT_KEY           | VARCHAR(256)    |               |
| ADMIT_DATE          | DATE            |               |
| DISCHARGE_DATE      | DATE            |               |
| FACILITY_NAME       | VARCHAR(256)    |               |
| FACILITY_STATE      | VARCHAR(256)    |               |
| PRIMARY_DIAGNOSIS   | VARCHAR(256)    |               |
| DISCHARGE_TYPE      | VARCHAR(256)    |               |
| TREATMENT_SETTING   | VARCHAR(256)    |               |
| LATEST_HG_VISIT     | DATE            |               |
| NEXT_HG_APPOINTMENT | DATE            |               |
| _FIVETRAN_DELETED   | BOOLEAN         |               |
| _FIVETRAN_SYNCED    | TIMESTAMP_TZ(9) |               |
| SOURCE              | VARCHAR(4092)   |               |

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.NON_VISIT_NOTES `

| Column Name       | Data Type       | Description   |
|:------------------|:----------------|:--------------|
| ID                | VARCHAR(256)    |               |
| FH_ID             | NUMBER(38,0)    |               |
| CONTENT           | VARCHAR(8192)   |               |
| CONTENT_CARE_TAGS | VARIANT         |               |
| CREATED_BY        | VARCHAR(256)    |               |
| CREATED_AT        | TIMESTAMP_TZ(9) |               |
| UPDATED_AT        | TIMESTAMP_TZ(9) |               |
| _FIVETRAN_DELETED | BOOLEAN         |               |
| _FIVETRAN_SYNCED  | TIMESTAMP_TZ(9) |               |
| NOTE_TYPE         | VARCHAR(256)    |               |

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.NOTES `

| Column Name           | Data Type       | Description   |
|:----------------------|:----------------|:--------------|
| ID                    | VARCHAR(256)    |               |
| UPDATED_AT            | TIMESTAMP_TZ(9) |               |
| CONTENTS              | VARCHAR(8192)   |               |
| SUBJECT_MRN           | VARCHAR(256)    |               |
| CREATED_AT            | TIMESTAMP_TZ(9) |               |
| AUTHOR_EMR_KEY        | VARCHAR(256)    |               |
| ORGANIZATIONAL_AUTHOR | VARCHAR(256)    |               |
| IS_PINNED             | BOOLEAN         |               |
| _FIVETRAN_DELETED     | BOOLEAN         |               |
| _FIVETRAN_SYNCED      | TIMESTAMP_TZ(9) |               |
| PROVENANCE            | VARCHAR(256)    |               |
| FH_ID                 | NUMBER(38,0)    |               |
| AUTHOR_USER_ID        | VARCHAR(256)    |               |

## ` PC_FIVETRAN_DB.HELPINGHAND_PROD_DB_PUBLIC.POM_QUESTIONS `

| Column Name       | Data Type       | Description   |
|:------------------|:----------------|:--------------|
| ID                | VARCHAR(256)    |               |
| UPDATED_AT        | TIMESTAMP_TZ(9) |               |
| CREATED_AT        | TIMESTAMP_TZ(9) |               |
| LABEL             | VARCHAR(256)    |               |
| ORDER             | NUMBER(38,0)    |               |
| _FIVETRAN_DELETED | BOOLEAN         |               |
| _FIVETRAN_SYNCED  | TIMESTAMP_TZ(9) |               |

## ` TRANSFORMED_DATA.DBT_SLOPEZ_BASE.HCPCS_CATEGORIES `

| Column Name          | Data Type         | Description   |
|:---------------------|:------------------|:--------------|
| HCPCS_CODE           | VARCHAR(16777216) |               |
| HCPCS_CATEGORY       | VARCHAR(16777216) |               |
| HCPCS_CATEGORY_SHORT | VARCHAR(16777216) |               |

## ` TRANSFORMED_DATA.PROD.FH_MEMBERS `

| Column Name                                    | Data Type         | Description   |
|:-----------------------------------------------|:------------------|:--------------|
| FH_ID                                          | VARCHAR(16777216) |               |
| MEMB_DIM_ID                                    | VARCHAR(16777216) |               |
| PAT_DATE_OF_BIRTH                              | DATE              |               |
| PAT_DATE_OF_DEATH                              | DATE              |               |
| PAT_FIRST_NAME                                 | VARCHAR(16777216) |               |
| PAT_LAST_NAME                                  | VARCHAR(16777216) |               |
| PAT_FULL_NAME                                  | VARCHAR(16777216) |               |
| PAT_AGE                                        | NUMBER(38,0)      |               |
| PAT_GENDER                                     | VARCHAR(16777216) |               |
| IS_PAT_SEX_FEMALE                              | BOOLEAN           |               |
| IS_PAT_SEX_MALE                                | BOOLEAN           |               |
| IS_PAT_DECEASED                                | NUMBER(38,0)      |               |
| ELATION_MRN                                    | NUMBER(38,0)      |               |
| MOST_RECENT_XWALK_STATE                        | VARCHAR(16777216) |               |
| PAT_PAYER                                      | VARCHAR(16777216) |               |
| HAS_EVER_BEEN_ON_XWALK                         | NUMBER(1,0)       |               |
| IS_BATCHED                                     | BOOLEAN           |               |
| IS_ON_MOST_RECENT_XWALK                        | NUMBER(38,0)      |               |
| MOST_RECENT_XWALK_DATE                         | DATE              |               |
| PAT_MIDDLE_INITIAL                             | VARCHAR(16777216) |               |
| PAT_LANGUAGE                                   | VARCHAR(16777216) |               |
| SOURCE_PATIENT_ID                              | VARCHAR(16777216) |               |
| MOST_RECENT_ELIGIBILITY_DATE                   | DATE              |               |
| PAT_PHONE                                      | VARCHAR(16777216) |               |
| PAT_ADDRESS_LINE_1                             | VARCHAR(16777216) |               |
| PAT_ADDRESS_LINE_2                             | VARCHAR(16777216) |               |
| PAT_CITY                                       | VARCHAR(16777216) |               |
| PAT_STATE                                      | VARCHAR(16777216) |               |
| PAT_ZIP_5_DIGIT                                | VARCHAR(16777216) |               |
| CBSA                                           | VARCHAR(16777216) |               |
| CBSA_TITLE                                     | VARCHAR(16777216) |               |
| DATE_FIRST_ASSIGNED_TO_CARETEAM                | TIMESTAMP_TZ(9)   |               |
| IS_ASSIGNED                                    | BOOLEAN           |               |
| IS_OUTREACHED                                  | BOOLEAN           |               |
| IS_REACHED                                     | BOOLEAN           |               |
| IS_MET                                         | BOOLEAN           |               |
| FIRST_ASSIGNED_FHG_FULL_NAME                   | VARCHAR(4610)     |               |
| BATCH_LOCATION                                 | VARCHAR(16777216) |               |
| MARKET                                         | VARCHAR(16777216) |               |
| FIRST_REACH_DATE                               | DATE              |               |
| FIRST_MET_DATE                                 | DATE              |               |
| FIRST_ENGAGED_DATE                             | DATE              |               |
| FIRST_ENGAGEMENT_FHG_FULL_NAME                 | VARCHAR(1542)     |               |
| BATCH_DATE                                     | DATE              |               |
| IS_SELECTED                                    | BOOLEAN           |               |
| FH_COVERAGE_CATEGORY                           | VARCHAR(16777216) |               |
| FH_COVERAGE_CATEGORY_AT_ENGAGEMENT             | VARCHAR(16777216) |               |
| IS_DSNP                                        | BOOLEAN           |               |
| IS_TANF                                        | BOOLEAN           |               |
| IS_ABD                                         | BOOLEAN           |               |
| IS_BEN_SUPP_ELIGIBLE                           | BOOLEAN           |               |
| IS_DISENROLLED                                 | BOOLEAN           |               |
| IS_UNABLE_TO_REACH                             | BOOLEAN           |               |
| IS_ENGAGED                                     | BOOLEAN           |               |
| IS_OPT_OUT                                     | BOOLEAN           |               |
| IS_PENDING_OUTREACH                            | BOOLEAN           |               |
| IS_CONTACTED                                   | BOOLEAN           |               |
| IS_SUSPENDED                                   | BOOLEAN           |               |
| IS_CONVERTED                                   | BOOLEAN           |               |
| HAS_EVER_BEEN_ENGAGED                          | BOOLEAN           |               |
| HAS_DECLINED_PAYOR_OUTREACH                    | BOOLEAN           |               |
| NEVER_ENGAGED                                  | BOOLEAN           |               |
| IS_INACTIVE                                    | BOOLEAN           |               |
| IS_BNYE                                        | BOOLEAN           |               |
| CURRENT_ENGAGEMENT_STATUS                      | VARCHAR(256)      |               |
| CURRENT_ENGAGEMENT_STATUS_REASON               | VARCHAR(256)      |               |
| IS_TRANSITIONED_TO_THL                         | BOOLEAN           |               |
| IS_RATE_CELL_CONVERTED                         | BOOLEAN           |               |
| CURRENT_ENGAGEMENT_STATUS_DATE                 | DATE              |               |
| DATE_ASSIGNED_TO_MOST_RECENT_CARETEAM          | TIMESTAMP_TZ(9)   |               |
| CURRENTLY_ASSIGNED_CRG_FULL_NAME               | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_CRG_STAFF_ID                | VARCHAR(16777216) |               |
| IS_CRG_ACTIVE                                  | BOOLEAN           |               |
| CURRENTLY_ASSIGNED_FHG_FULL_NAME               | VARCHAR(16777216) |               |
| IS_FHG_ACTIVE                                  | BOOLEAN           |               |
| CURRENTLY_ASSIGNED_FHG_STAFF_ID                | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_HG_FULL_NAME                | VARCHAR(16777216) |               |
| FHG_POD_NAME                                   | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_STRIVE_SPECIALIST_FULL_NAME | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_STRIVE_SPECIALIST_STAFF_ID  | VARCHAR(16777216) |               |
| POD_NAME                                       | VARCHAR(256)      |               |
| IS_STRIVE_ACTIVE                               | BOOLEAN           |               |
| CONSENT_EFFECTIVE_START_DATE                   | DATE              |               |
| DAYS_ENGAGED                                   | NUMBER(9,0)       |               |
| DAYS_ASSIGNED                                  | NUMBER(9,0)       |               |
| MOST_RECENT_SUCCESSFUL_INTERACTION_DATE        | DATE              |               |
| DAYS_SINCE_LAST_SUCCESSFUL_INTERACTION         | NUMBER(9,0)       |               |
| LATEST_CE_QUALIFYING_INTERACTION_DATE          | DATE              |               |
| IS_CONTINUOUSLY_ENGAGED                        | BOOLEAN           |               |
| HAS_SUCCESSFUL_INTERACTION                     | BOOLEAN           |               |
| HAS_POM                                        | BOOLEAN           |               |
| HAS_CRG_INITIAL_ASSESSMENT                     | BOOLEAN           |               |
| IS_1696_FILED                                  | BOOLEAN           |               |
| IS_1696_UPLOADED                               | BOOLEAN           |               |
| FIRST_BILLABLE_DATE                            | DATE              |               |
| IS_BILLABLE                                    | BOOLEAN           |               |
| IS_HIGH_RISK_LEVEL                             | BOOLEAN           |               |
| IS_MEDIUM_RISK_LEVEL                           | BOOLEAN           |               |
| IS_SELF_MANAGEMENT_LEVEL                       | BOOLEAN           |               |
| RISK_LEVEL                                     | VARCHAR(256)      |               |
| RISK_LEVEL_OUTREACH_FREQUENCY                  | VARCHAR(16777216) |               |
| SUICIDIAL_IDEATION_RISK_LEVEL                  | VARCHAR(256)      |               |
| IS_CLINICALLY_STABLE                           | BOOLEAN           |               |
| IS_COMMUNITY_CONNECTED                         | BOOLEAN           |               |
| IS_MED_ADHERENT                                | BOOLEAN           |               |
| IS_SOCIALLY_STABLE                             | BOOLEAN           |               |
| HAS_SCHIZOPHRENIA                              | BOOLEAN           |               |
| HAS_BIPOLAR                                    | BOOLEAN           |               |
| HAS_MDD                                        | BOOLEAN           |               |
| CLINICAL_CATEGORY                              | VARCHAR(8)        |               |

## ` TRANSFORMED_DATA.PROD.FH_MEMBER_SELECTION_QUALIFICATION `

| Column Name                             | Data Type         | Description   |
|:----------------------------------------|:------------------|:--------------|
| FH_ID                                   | VARCHAR(16777216) |               |
| ASSOURCE_PATIENT_ID                     | VARCHAR(16777216) |               |
| MOST_RECENT_XWALK_STATE                 | VARCHAR(16777216) |               |
| MOST_RECENT_XWALK_DATE                  | DATE              |               |
| MOST_RECENT_ELIGIBILITY_DATE            | DATE              |               |
| HAS_EVER_BEEN_ON_XWALK                  | NUMBER(1,0)       |               |
| IS_ON_MOST_RECENT_XWALK                 | NUMBER(38,0)      |               |
| PAT_PAYER                               | VARCHAR(16777216) |               |
| IS_BATCHED                              | BOOLEAN           |               |
| BATCH_LOCATION                          | VARCHAR(16777216) |               |
| PAT_ZIP_5_DIGIT                         | VARCHAR(16777216) |               |
| CBSA                                    | VARCHAR(16777216) |               |
| CBSA_TITLE                              | VARCHAR(16777216) |               |
| EXCLUDE_FOR_MISSING_ON_LATEST_XWALK     | NUMBER(1,0)       |               |
| EXCLUDE_FOR_DECEASED                    | NUMBER(1,0)       |               |
| EXCLUDE_FOR_UNSERVED_LANGUAGE           | NUMBER(1,0)       |               |
| EXCLUDE_FOR_AGE                         | NUMBER(1,0)       |               |
| EXCLUDE_FOR_MISSING_DOB                 | NUMBER(1,0)       |               |
| EXCLUDE_FOR_MISSING_CONTACT_INFORMATION | NUMBER(1,0)       |               |
| EXCLUDE_FOR_BAD_CONTACT_INFORMATION     | NUMBER(1,0)       |               |
| EXCLUDE_FOR_UNSERVED_BATCHING_LOCATION  | NUMBER(1,0)       |               |
| EXCLUDE_FOR_CURRENT_ACO                 | NUMBER(1,0)       |               |
| EXCLUDE_FOR_CURRENT_THL_MEMBER          | NUMBER(1,0)       |               |
| EXCLUDE_FOR_UNSERVED_PRODUCT            | NUMBER(1,0)       |               |
| EXCLUDE_FOR_EXCLUDED_PRODUCT            | NUMBER(1,0)       |               |
| EXCLUDE_FOR_NO_ACTIVE_COVERAGE          | NUMBER(1,0)       |               |
| EXCLUDE_FOR_COVERAGE_TERM_IN_LT_3MONTHS | NUMBER(1,0)       |               |
| EXCLUDE_FOR_HEMOPHILIA                  | NUMBER(1,0)       |               |
| EXCLUDE_FOR_PREGNANCY                   | NUMBER(1,0)       |               |
| EXCLUDE_FOR_CANCER                      | NUMBER(1,0)       |               |
| EXCLUDE_FOR_ESRD                        | NUMBER(1,0)       |               |
| EXCLUDE_FOR_ACT_PACT                    | NUMBER(1,0)       |               |
| EXCLUDE_FOR_SNF                         | NUMBER(1,0)       |               |
| EXCLUDE_FOR_TRANSPLANT                  | NUMBER(1,0)       |               |
| EXCLUDE_FOR_PRESSURE_ULCER              | NUMBER(1,0)       |               |
| EXCLUDE_FOR_INTUBATION                  | NUMBER(1,0)       |               |
| EXCLUDE_FOR_MSK                         | NUMBER(1,0)       |               |
| EXCLUDE_FOR_NEUROLOGICAL_DISORDER       | NUMBER(1,0)       |               |
| EXCLUDE_FOR_LVAD                        | NUMBER(1,0)       |               |
| EXCLUDE_FOR_HOSPICE                     | NUMBER(1,0)       |               |
| EXCLUDE_FOR_SICKLE_CELL                 | NUMBER(1,0)       |               |
| EXCLUDE_FOR_INTELLECTUAL_DISABILITIES   | NUMBER(1,0)       |               |
| EXCLUDE_FOR_UNSERVED_PRODUCT_IN_MARKET  | NUMBER(1,0)       |               |
| BEN_SUPP_OPTIMIZED_FLAG                 | NUMBER(1,0)       |               |
| BENSUPP_SCORE_PROBABILITY               | FLOAT             |               |
| EXCLUDE_FOR_NOT_BENSUPP_OPTIMIZED       | NUMBER(1,0)       |               |
| CURRENTLY_FH_QUALIFIED                  | NUMBER(1,0)       |               |
| TOTAL_ACTIVE_EXCLUSIONS                 | NUMBER(25,0)      |               |
| TOTAL_ACTIVE_CLINICAL_EXCLUSIONS        | NUMBER(15,0)      |               |
| TOTAL_OPERATIONAL_EXCLUSIONS            | NUMBER(9,0)       |               |
| HAS_OPERATIONAL_EXCLUSIONS              | BOOLEAN           |               |
| MEETS_EXCLUSION_CRITERIA                | BOOLEAN           |               |
| HAS_ACTIVE_CLINICAL_EXCLUSIONS          | NUMBER(1,0)       |               |
| IS_SELECTED                             | BOOLEAN           |               |
| FH_COVERAGE_CATEGORY                    | VARCHAR(16777216) |               |
| FH_COVERAGE_CATEGORY_TYPE               | VARCHAR(8)        |               |
| FH_COVERAGE_CATEGORY_PLAN_DESC          | VARCHAR(16777216) |               |
| FH_COVERAGE_CATEGORY_PRODUCT_DESC       | VARCHAR(16777216) |               |
| CURRENT_THL_MEMBER_STATUS               | VARCHAR(16777216) |               |
| CURRENT_THL_PRACTICE_NAME               | VARCHAR(16777216) |               |
| MEETS_INCLUSION_CRITERIA                | BOOLEAN           |               |
| IS_OPERATIONALLY_QUALIFIED              | BOOLEAN           |               |
| IS_FH_CLINICALLY_QUALIFIED              | NUMBER(1,0)       |               |
| TOTAL_ACTIVE_MARKET_EXCLUSIONS          | NUMBER(18,0)      |               |
| IS_MARKET_QUALIFIED                     | BOOLEAN           |               |
| IS_QUALIFIED                            | NUMBER(1,0)       |               |
| IS_BATCHABLE                            | NUMBER(1,0)       |               |
| IS_GEOGRAPHIC_UNLOCK                    | BOOLEAN           |               |

## ` TRANSFORMED_DATA.PROD.STG_ELATION_CLINICAL_FORM_ANSWERS `

| Column Name                | Data Type         | Description   |
|:---------------------------|:------------------|:--------------|
| ELATION_PATIENT_ID         | NUMBER(38,0)      |               |
| PK                         | VARCHAR(32)       |               |
| ITEM_ID                    | NUMBER(38,0)      |               |
| QUESTIONNAIRE_COMPLETED_AT | TIMESTAMP_NTZ(9)  |               |
| QUESTIONNAIRE_NAME         | VARCHAR(16777216) |               |
| SCORE                      | NUMBER(38,0)      |               |
| QUESTION_NAME              | VARCHAR(16777216) |               |
| QUESTION_ANSWER            | VARCHAR(16777216) |               |
| IS_TOTAL_SCORE             | BOOLEAN           |               |
| SEQUENCE_NUMBER            | NUMBER(38,0)      |               |
| LOINC_ANSWER_CODE          | VARCHAR(16777216) |               |
| CREATED_BY_USER_ID         | NUMBER(38,0)      |               |
| IS_DELETED                 | BOOLEAN           |               |
| FH_ID                      | VARCHAR(16777216) |               |

## ` TRANSFORMED_DATA.PROD_BASE.UHC_INPATIENT_AUTHORIZATIONS `

| Column Name                | Data Type         | Description   |
|:---------------------------|:------------------|:--------------|
| PRIMARY_KEY                | VARCHAR(16777216) |               |
| FH_ID                      | VARCHAR(16777216) |               |
| UHC_DIAMOND_ID             | VARCHAR(16777216) |               |
| ADMIT_DATE                 | VARCHAR(256)      |               |
| DISCHARGE_DATE             | VARCHAR(256)      |               |
| LOS                        | NUMBER(38,0)      |               |
| BUSINESS_SEGMENT           | VARCHAR(256)      |               |
| DSNP_IND                   | VARCHAR(256)      |               |
| IP                         | VARCHAR(256)      |               |
| PRIM_DIAG_CD               | VARCHAR(256)      |               |
| PRIM_DIAG_DESCRIPTION      | VARCHAR(16777216) |               |
| FACILITY_NAME              | VARCHAR(256)      |               |
| FACILITY_STATE             | VARCHAR(256)      |               |
| ATTENDING_NPI              | VARCHAR(256)      |               |
| RPM_SCORE                  | FLOAT             |               |
| DISCHARGE_TYPE             | VARCHAR(256)      |               |
| RST_SCORE                  | NUMBER(38,0)      |               |
| CASETYPE                   | VARCHAR(256)      |               |
| TREATMENT_SETTING          | VARCHAR(256)      |               |
| FACILITY_MPIN              | VARCHAR(256)      |               |
| INDIVIDUAL_IN_HOSPITAL_NOW | BOOLEAN           |               |
| NOTIFIED_DATE              | DATE              |               |

## ` TRANSFORMED_DATA.PROD_CORE.CORE_MEDICAL_CLAIM_LINES `

| Column Name                     | Data Type         | Description   |
|:--------------------------------|:------------------|:--------------|
| CLAIM_END_DATE                  | DATE              |               |
| CLAIM_LINE_NUMBER               | NUMBER(38,0)      |               |
| CLAIM_START_DATE                | DATE              |               |
| CMS_PLACE_OF_SERVICE_CODE       | VARCHAR(16777216) |               |
| CMS_REVENUE_CENTER_CODE         | VARCHAR(16777216) |               |
| CMS_TYPE_OF_BILL_CODE           | VARCHAR(16777216) |               |
| DOCUMENT_EFFECTIVE_DATE         | DATE              |               |
| URSA_CLAIM_SERVICE_LINE_ITEM_ID | VARCHAR(16777216) |               |
| CLAIM_LINE_ID                   | VARCHAR(16777216) |               |
| FH_ID                           | VARCHAR(16777216) |               |
| HCPCS_CODE                      | VARCHAR(16777216) |               |
| URSA_CLAIM_ID                   | VARCHAR(16777216) |               |
| CLAIM_ID                        | VARCHAR(16777216) |               |
| URSA_DOCUMENT_ID                | VARCHAR(16777216) |               |
| URSA_SERVICE_PROVIDER_ID        | VARCHAR(16777216) |               |
| URSA_BILLING_PROVIDER_ID        | VARCHAR(16777216) |               |
| HCPCS_DESC                      | VARCHAR(16777216) |               |
| CLAIM_PAID_DATE                 | DATE              |               |
| BILLING_PROVIDER_NPI            | VARCHAR(16777216) |               |
| BILLING_PROVIDER_TIN            | VARCHAR(16777216) |               |
| BILLING_PROVIDER_NAME           | VARCHAR(16777216) |               |
| SERVING_PROVIDER_NPI            | VARCHAR(16777216) |               |
| SERVING_PROVIDER_NAME           | VARCHAR(16777216) |               |
| RECEIVED_DATE                   | DATE              |               |
| IS_HOUSE_CALL_CLAIM             | BOOLEAN           |               |
| IS_FIRSTHAND_PROVIDER           | BOOLEAN           |               |
| URSA_ENCOUNTER_ID               | VARCHAR(16777216) |               |
| ENCOUNTER_ID                    | VARCHAR(16777216) |               |
| CLAIM_PLAN_PAID_AMOUNT          | NUMBER(31,9)      |               |
| IS_RADIOLOGY_HCPCS_CODE         | NUMBER(1,0)       |               |
| IS_PATHOLOGY_LAB_HCPCS_CODE     | NUMBER(1,0)       |               |
| IS_CHEMO_HCPCS_CODE             | NUMBER(1,0)       |               |
| IS_HEMOPHILIA_HCPCS_CODE        | NUMBER(1,0)       |               |
| IS_ESRD_HCPCS_CODE              | NUMBER(1,0)       |               |
| IS_ACT_PACT_HCPCS_CODE          | NUMBER(1,0)       |               |
| SPEND_CAT_LEVEL_1               | VARCHAR(16777216) |               |
| SPEND_CAT_LEVEL_2               | VARCHAR(10)       |               |
| IS_BH_CLAIM                     | BOOLEAN           |               |
| IS_SUD_CLAIM                    | BOOLEAN           |               |
| ENCOUNTER_TYPE                  | VARCHAR(16777216) |               |
| ENCOUNTER_GROUP                 | VARCHAR(16777216) |               |

## ` TRANSFORMED_DATA.PROD_CORE.CORE_PHARMACY_CLAIMS `

| Column Name                                                     | Data Type         | Description   |
|:----------------------------------------------------------------|:------------------|:--------------|
| UHC_DIAMOND_ID                                                  | VARCHAR(16777216) |               |
| URSA_DOCUMENT_ID                                                | VARCHAR(16777216) |               |
| FH_ID                                                           | VARCHAR(16777216) |               |
| URSA_FILLING_PROVIDER_ID                                        | VARCHAR(16777216) |               |
| URSA_PRESCRIBING_PROVIDER_ID                                    | VARCHAR(16777216) |               |
| FILLED_DATE                                                     | DATE              |               |
| RECEIVED_DATE                                                   | DATE              |               |
| FILLING_PROVIDER_NPI                                            | VARCHAR(16777216) |               |
| FILLING_PROVIDER_TIN                                            | VARCHAR(16777216) |               |
| FILLING_PROVIDER_NAME                                           | VARCHAR(16777216) |               |
| FILLING_PROVIDER_MAJOR_CHAIN_DESCRIPTION                        | VARCHAR(16777216) |               |
| FILLING_PROVIDER_PRACTICE_ADDRESS_LINE_1                        | VARCHAR(16777216) |               |
| FILLING_PROVIDER_PRACTICE_ADDRESS_CITY                          | VARCHAR(16777216) |               |
| FILLING_PROVIDER_PRACTICE_ADDRESS_STATE                         | VARCHAR(16777216) |               |
| FILLING_PROVIDER_PRACTICE_ADDRESS_ZIP                           | VARCHAR(16777216) |               |
| FILLING_PROVIDER_PHONE_NUMBER_1                                 | VARCHAR(16777216) |               |
| FILLING_PROVIDER_PHONE_NUMBER_2                                 | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_NAME                                       | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_TIN                                        | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_NPI                                        | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_PRACTICE_ADDRESS_LINE_1                    | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_PRACTICE_ADDRESS_CITY                      | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_LEGAL_BUSINESS_NAME                        | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_PHONE_NUMBER_1                             | VARCHAR(16777216) |               |
| PRESCRIBING_PROVIDER_PHONE_NUMBER_2                             | VARCHAR(16777216) |               |
| IS_FILLING_PROV_SPECIALTY_PHARMACY                              | NUMBER(38,5)      |               |
| IS_FILLING_PROV_MAIL_ORDER_PHARMACY                             | NUMBER(38,5)      |               |
| FILLING_PROV_PRIMARY_NUCC_PROV_TAXONOMY_CODE                    | VARCHAR(16777216) |               |
| FILLING_PROVIDER_TYPE                                           | VARCHAR(16777216) |               |
| ACTIVE_INGREDIENTS_NAME                                         | VARCHAR(16777216) |               |
| PLAN_PAID_AMOUNT                                                | NUMBER(31,9)      |               |
| NDC_CODE                                                        | VARCHAR(16777216) |               |
| IS_TARGETED_MEDICATION_TYPE                                     | NUMBER(18,5)      |               |
| IS_CHRONIC_MEDICATION                                           | NUMBER(38,5)      |               |
| IS_PRIORITY_ADHERENCE_MEDICATION                                | NUMBER(18,5)      |               |
| PRIORITY_ADHERENCE_MEDICATION_CAT                               | VARCHAR(16777216) |               |
| IS_ANTIDEPRESSANT                                               | NUMBER(38,5)      |               |
| IS_ANTINEOPLASTIC                                               | NUMBER(38,5)      |               |
| IS_ANTIDIABETIC                                                 | NUMBER(38,5)      |               |
| IS_ORAL_ANTIDIABETIC                                            | NUMBER(38,5)      |               |
| IS_ORAL_ACEI_ARB                                                | NUMBER(38,5)      |               |
| IS_STATIN                                                       | NUMBER(38,5)      |               |
| IS_BETA_BLOCKER                                                 | NUMBER(38,5)      |               |
| IS_HIGH_RISK_MEDICATION                                         | NUMBER(18,5)      |               |
| HIGH_RISK_MEDICATION_CAT                                        | VARCHAR(16777216) |               |
| IS_ANTICOAGULANT                                                | NUMBER(38,5)      |               |
| IS_ORAL_ANTIPLATELET                                            | NUMBER(38,5)      |               |
| IS_CARDIAC_GLYCOSIDES                                           | NUMBER(38,5)      |               |
| IS_ORAL_HYPOGLYCEMIC                                            | NUMBER(38,5)      |               |
| IS_INSULIN                                                      | NUMBER(38,5)      |               |
| IS_OPIATE_AGONISTS                                              | NUMBER(38,5)      |               |
| IS_MEDICATION_INCLUDED_IN_PDC_MEASURES                          | NUMBER(18,5)      |               |
| PDC_MEDICATION_CAT                                              | VARCHAR(16777216) |               |
| IS_SACUBITRIL_VALSARTAN                                         | NUMBER(38,5)      |               |
| NDC_CODE_11_DIGIT                                               | VARCHAR(16777216) |               |
| LABEL_DESC                                                      | VARCHAR(16777216) |               |
| IS_GENERIC                                                      | NUMBER(38,5)      |               |
| IS_SINGLE_SOURCE                                                | NUMBER(38,5)      |               |
| IS_OTC                                                          | NUMBER(38,5)      |               |
| REDBOOK_GENERIC_CROSS_REFERENCE_CODE                            | VARCHAR(16777216) |               |
| REDBOOK_GENERIC_FORMULATION_CODE                                | VARCHAR(16777216) |               |
| PRIMARY_AGENT_DESC                                              | VARCHAR(16777216) |               |
| FORM_DESC                                                       | VARCHAR(16777216) |               |
| ROUTE_OF_ADMINISTRATION_DESC                                    | VARCHAR(16777216) |               |
| QUANTITY_DISPENSED                                              | NUMBER(38,9)      |               |
| DAYS_SUPPLY                                                     | NUMBER(38,0)      |               |
| DAYS_SUPPLY_CAT_TIER_1_DESC                                     | VARCHAR(16777216) |               |
| IS_SINGLE_DAY_SUPPLY                                            | NUMBER(18,5)      |               |
| IS_30D_SUPPLY                                                   | BOOLEAN           |               |
| IS_90D_SUPPLY                                                   | BOOLEAN           |               |
| AHFS_THERAPEUTIC_CLASS_CODE_6_DIGIT                             | VARCHAR(16777216) |               |
| AHFS_THERAPEUTIC_CLASS_TIER_1_DESC                              | VARCHAR(16777216) |               |
| AHFS_THERAPEUTIC_CLASS_TIER_2_DESC                              | VARCHAR(16777216) |               |
| AHFS_THERAPEUTIC_CLASS_TIER_3_DESC                              | VARCHAR(16777216) |               |
| NCPDP_DAW_CODE                                                  | VARCHAR(16777216) |               |
| NDC_COUNT_OF_FILLS_RANK                                         | NUMBER(23,5)      |               |
| STRENGTH_DESC                                                   | VARCHAR(16777216) |               |
| IS_CMS_STAR_MEASURES_ORAL_ANTIDIABETIC                          | NUMBER(38,5)      |               |
| IS_CMS_STAR_MEASURES_ORAL_ACEI_ARB                              | NUMBER(38,5)      |               |
| IS_CMS_STAR_MEASURES_STATIN                                     | NUMBER(38,5)      |               |
| IS_CMS_STAR_MEASURES_SACUBITRIL_VALSARTAN                       | NUMBER(38,5)      |               |
| IS_CMS_STAR_MEASURES_INSULIN                                    | NUMBER(38,5)      |               |
| NDC_REFERENCE_LABEL_DESC                                        | VARCHAR(16777216) |               |
| NDC_REFERENCE_ACTIVE_INGREDIENTS_DESC                           | VARCHAR(16777216) |               |
| NDC_REFERENCE_PRIMARY_AGENT_DESC                                | VARCHAR(16777216) |               |
| NDC_REFERENCE_FORM_DESC                                         | VARCHAR(16777216) |               |
| NDC_REFERENCE_ROUTE_OF_ADMINISTRATION_DESC                      | VARCHAR(16777216) |               |
| NDC_REF_IS_FILL_WITH_ANY_PAID_AMOUNT                            | NUMBER(18,5)      |               |
| NDC_REF_NDC_COUNT_OF_FILLS_WITH_PAYMENT                         | NUMBER(18,5)      |               |
| NDC_COUNT_OF_FILLS_WITH_PAYMENT_PERCENTILE                      | FLOAT             |               |
| NDC_COUNT_OF_FILLS_QUINTILE_CAT                                 | VARCHAR(16777216) |               |
| PRESCRIPTION_NUMBER                                             | VARCHAR(16777216) |               |
| PROV_FILL_VOLUME_PERCENTILE_RANK                                | NUMBER(28,9)      |               |
| PROV_FILL_VOLUME_QUINTILE_CAT                                   | VARCHAR(16777216) |               |
| INGREDIENTS_HASH                                                | VARCHAR(16777216) |               |
| FILL_COUNT_QUALIFYING_TARGETED_MED_CATEGORIES                   | NUMBER(38,5)      |               |
| IS_FH_ANTIPSYCHOTIC                                             | NUMBER(38,5)      |               |
| IS_FH_ANTIPSYCHOTIC_ROUTE_OF_ADMINISTRATION_LONG_ACTING_INJECTA | NUMBER(38,5)      |               |
| IS_FH_ANTIPSYCHOTIC_ROUTE_OF_ADMINISTRATION_ORAL                | NUMBER(38,5)      |               |
| IS_FH_ANTIPSYCHOTIC_ROUTE_OF_ADMINISTRATION_OTHER               | NUMBER(38,5)      |               |
| IS_FH_LONG_ACTING_INJECTABLE_AND_LABELED_ABILIFY                | NUMBER(38,5)      |               |
| FH_ANTIPSYCHOTIC_CAT                                            | VARCHAR(16777216) |               |
| ANTIPSYCHOTIC_ROUTE_OF_ADMINISTRATION_CAT                       | VARCHAR(16777216) |               |
| IS_FILLED_LAST_60D                                              | NUMBER(1,0)       |               |
| IS_FILLED_LAST_120D                                             | NUMBER(1,0)       |               |
| PHARMACY_NAME                                                   | VARCHAR(16777216) |               |
| CLASS_1_DESC                                                    | VARCHAR(16777216) |               |
| CLASS_2_DESC                                                    | VARCHAR(16777216) |               |
| PRODUCT_NAME                                                    | VARCHAR(16777216) |               |
| IS_MAINTENANCE_MEDICATION                                       | BOOLEAN           |               |
| IS_ANTIPSYCH_MED                                                | BOOLEAN           |               |
| IS_DIABETES_MED                                                 | BOOLEAN           |               |
| IS_COPD_MAINTENANCE_INHALER                                     | BOOLEAN           |               |
| IS_MDD_SUSPECT_MED                                              | BOOLEAN           |               |

## ` TRANSFORMED_DATA.PROD_MARTS.MARTS_ID_STRAT_ANALYTICS `

| Column Name                            | Data Type         | Description   |
|:---------------------------------------|:------------------|:--------------|
| FH_ID                                  | VARCHAR(16777216) |               |
| SOURCE_PATIENT_ID                      | VARCHAR(16777216) |               |
| MARKET                                 | VARCHAR(16777216) |               |
| HAS_EVER_BEEN_ON_XWALK                 | NUMBER(1,0)       |               |
| PAT_PAYER                              | VARCHAR(16777216) |               |
| PAT_CITY                               | VARCHAR(16777216) |               |
| PAT_STATE                              | VARCHAR(16777216) |               |
| PAT_ZIP_5_DIGIT                        | VARCHAR(16777216) |               |
| CBSA                                   | VARCHAR(16777216) |               |
| CBSA_TITLE                             | VARCHAR(16777216) |               |
| BATCH_DATE                             | DATE              |               |
| BATCH_LOCATION                         | VARCHAR(16777216) |               |
| POD_NAME                               | VARCHAR(256)      |               |
| PAT_FIRST_NAME                         | VARCHAR(16777216) |               |
| PAT_LAST_NAME                          | VARCHAR(16777216) |               |
| PAT_DATE_OF_BIRTH                      | DATE              |               |
| PAT_DATE_OF_DEATH                      | DATE              |               |
| PAT_AGE                                | NUMBER(38,0)      |               |
| PAT_LANGUAGE                           | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_FHG_FULL_NAME       | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_CRG_FULL_NAME       | VARCHAR(16777216) |               |
| DATE_FIRST_ASSIGNED_TO_CARETEAM        | TIMESTAMP_TZ(9)   |               |
| DATE_ASSIGNED_TO_MOST_RECENT_CARETEAM  | TIMESTAMP_TZ(9)   |               |
| MOST_RECENT_XWALK_STATE                | VARCHAR(16777216) |               |
| FH_COVERAGE_CATEGORY                   | VARCHAR(16777216) |               |
| IS_DSNP                                | BOOLEAN           |               |
| IS_TANF                                | BOOLEAN           |               |
| IS_ABD                                 | BOOLEAN           |               |
| IS_BEN_SUPP_ELIGIBLE                   | BOOLEAN           |               |
| FH_COVERAGE_CATEGORY_AT_ENGAGEMENT     | VARCHAR(16777216) |               |
| FH_COVERAGE_CATEGORY_TYPE              | VARCHAR(8)        |               |
| CURRENT_ENGAGEMENT_STATUS              | VARCHAR(256)      |               |
| CURRENT_ENGAGEMENT_STATUS_DATE         | DATE              |               |
| IS_ASSIGNED                            | BOOLEAN           |               |
| IS_BATCHED                             | BOOLEAN           |               |
| IS_ENGAGED                             | BOOLEAN           |               |
| HAS_EVER_BEEN_ENGAGED                  | BOOLEAN           |               |
| IS_DISENROLLED                         | BOOLEAN           |               |
| IS_SUSPENDED                           | BOOLEAN           |               |
| IS_OPT_OUT                             | BOOLEAN           |               |
| IS_SELECTED                            | BOOLEAN           |               |
| IS_CONVERTED                           | BOOLEAN           |               |
| IS_BNYE                                | BOOLEAN           |               |
| DAYS_ENGAGED                           | NUMBER(9,0)       |               |
| DAYS_ASSIGNED                          | NUMBER(9,0)       |               |
| MOST_RECENT_XWALK_DATE                 | DATE              |               |
| MOST_RECENT_ELIGIBILITY_DATE           | DATE              |               |
| IS_ON_MOST_RECENT_XWALK                | NUMBER(38,0)      |               |
| FIRST_REACH_DATE                       | DATE              |               |
| FIRST_ENGAGED_DATE                     | DATE              |               |
| BEN_SUPP_OPTIMIZED_FLAG                | NUMBER(1,0)       |               |
| BENSUPP_SCORE_PROBABILITY              | FLOAT             |               |
| IS_QUALIFIED                           | NUMBER(1,0)       |               |
| POTENTIAL_VALUE                        | FLOAT             |               |
| POTENTIAL_VALUE_CATEGORY               | VARCHAR(16777216) |               |
| EXPECTED_VALUE                         | FLOAT             |               |
| MEDEX_VALUE                            | NUMBER(38,15)     |               |
| ACD_VALUE                              | FLOAT             |               |
| BENSUPP_VALUE                          | FLOAT             |               |
| REALIZED_VALUE                         | FLOAT             |               |
| BENSUPP_REALIZED_VALUE                 | NUMBER(38,6)      |               |
| ACD_REALIZED_VALUE                     | FLOAT             |               |
| MEDEX_REALIZED_VALUE                   | NUMBER(38,9)      |               |
| PROBABILITY_OF_CONTACT                 | FLOAT             |               |
| POTENTIAL_VALUE_AT_BATCHING            | FLOAT             |               |
| ACD_VALUE_AT_BATCHING                  | FLOAT             |               |
| BENSUPP_VALUE_AT_BATCHING              | FLOAT             |               |
| MEDEX_VALUE_AT_BATCHING                | NUMBER(38,9)      |               |
| POTENTIAL_VALUE_AT_EARLIEST            | FLOAT             |               |
| ACD_VALUE_AT_EARLIEST                  | FLOAT             |               |
| BENSUPP_VALUE_AT_EARLIEST              | FLOAT             |               |
| MEDEX_VALUE_AT_EARLIEST                | NUMBER(38,9)      |               |
| POTENTIAL_VALUE_AT_ENGAGEMENT          | FLOAT             |               |
| ACD_VALUE_AT_ENGAGEMENT                | FLOAT             |               |
| BENSUPP_VALUE_AT_ENGAGEMENT            | FLOAT             |               |
| MEDEX_VALUE_AT_ENGAGEMENT              | NUMBER(38,9)      |               |
| TOTAL_ACTIVE_EXCLUSIONS                | NUMBER(25,0)      |               |
| TOTAL_ACTIVE_CLINICAL_EXCLUSIONS       | NUMBER(15,0)      |               |
| MEETS_EXCLUSION_CRITERIA               | BOOLEAN           |               |
| MEETS_INCLUSION_CRITERIA               | BOOLEAN           |               |
| IS_GEOGRAPHIC_UNLOCK                   | BOOLEAN           |               |
| IS_OPERATIONALLY_QUALIFIED             | BOOLEAN           |               |
| IS_FH_CLINICALLY_QUALIFIED             | NUMBER(1,0)       |               |
| IS_MARKET_QUALIFIED                    | BOOLEAN           |               |
| IS_BATCHABLE                           | NUMBER(1,0)       |               |
| IS_GEOGRAPHIC_AND_PV_UNLOCK            | BOOLEAN           |               |
| EXCLUDE_FOR_UNSERVED_BATCHING_LOCATION | NUMBER(1,0)       |               |
| IS_INDIVIDUAL_IN_SERVED_CBSA           | BOOLEAN           |               |

## ` TRANSFORMED_DATA.PROD_MARTS.ZUS_AUTH_ADMITS `

| Column Name                      | Data Type         | Description   |
|:---------------------------------|:------------------|:--------------|
| PRIMARY_KEY                      | VARCHAR(16777216) |               |
| FH_ID                            | VARCHAR(16777216) |               |
| ADMIT_DATE                       | DATE              |               |
| ADMIT_KEY                        | VARCHAR(16777216) |               |
| HAS_ZUS_NOTIFICATION             | BOOLEAN           |               |
| HAS_AUTH_NOTIFICATION            | BOOLEAN           |               |
| AUTH_SOURCE_ID                   | VARCHAR(16777216) |               |
| ZUS_SOURCE_ID                    | VARCHAR(16777216) |               |
| NOTIFIED_DATE                    | DATE              |               |
| DISCHARGE_DATE                   | DATE              |               |
| FACILITY_NAME                    | VARCHAR(16777216) |               |
| FACILITY_STATE                   | VARCHAR(16777216) |               |
| PRIMARY_DIAGNOSIS                | VARCHAR(16777216) |               |
| DISCHARGE_TYPE                   | VARCHAR(16777216) |               |
| TREATMENT_SETTING                | VARCHAR(16777216) |               |
| UHC_DIAMOND_ID                   | VARCHAR(16777216) |               |
| ELATION_MRN                      | NUMBER(38,0)      |               |
| NAME                             | VARCHAR(16777216) |               |
| DATE_OF_BIRTH                    | DATE              |               |
| MOST_RECENT_ENGAGE_STATUS        | VARCHAR(256)      |               |
| MARKET                           | VARCHAR(16777216) |               |
| POD_NAME                         | VARCHAR(256)      |               |
| FH_COVERAGE_CATEGORY             | VARCHAR(16777216) |               |
| CURRENTLY_ASSIGNED_FHG_FULL_NAME | VARCHAR(16777216) |               |
| LATEST_HG_VISIT                  | DATE              |               |
| NEXT_HG_APPOINTMENT              | DATE              |               |

## ` TRANSFORMED_DATA.PROD_STAGING.DIM_ICD_CMS_HCC_MAP `

| Column Name        | Data Type         | Description   |
|:-------------------|:------------------|:--------------|
| MODEL_VERSION      | VARCHAR(11)       |               |
| DIAGNOSIS_CODE     | VARCHAR(16777216) |               |
| HCC_CODE           | NUMBER(38,0)      |               |
| RAF_WEIGHT         | VARCHAR(16777216) |               |
| PAYMENT_YEAR       | NUMBER(4,0)       |               |
| IS_ACUTE_DIAGNOSIS | BOOLEAN           |               |

## ` TRANSFORMED_DATA.PROD_TRANSFORM.CORE_MEDICAL_CLAIM_DIAGNOSIS_LINE_ITEMS `

| Column Name                | Data Type         | Description   |
|:---------------------------|:------------------|:--------------|
| FH_ID                      | VARCHAR(16777216) |               |
| URSA_CLAIM_ID              | VARCHAR(16777216) |               |
| URSA_SERVICE_PROVIDER_ID   | VARCHAR(16777216) |               |
| SERVING_PROVIDER_NPI       | VARCHAR(16777216) |               |
| SERVING_PROVIDER_NAME      | VARCHAR(16777216) |               |
| BILLING_PROVIDER_NPI       | VARCHAR(16777216) |               |
| BILLING_PROVIDER_NAME      | VARCHAR(16777216) |               |
| BILLING_PROVIDER_TIN       | VARCHAR(16777216) |               |
| CLAIM_START_DATE           | DATE              |               |
| CLAIM_END_DATE             | DATE              |               |
| CLAIM_PAID_DATE            | DATE              |               |
| RECEIVED_DATE              | DATE              |               |
| ICD10CM_CODE               | VARCHAR(16777216) |               |
| ICD10CM_DESC               | VARCHAR(16777216) |               |
| DX_LINE_NUMBER             | NUMBER(38,0)      |               |
| IS_CMS_HCC_RISK_ADJUSTABLE | BOOLEAN           |               |
| IS_CDPS_RISK_ADJUSTABLE    | BOOLEAN           |               |
| IS_FIRST_HAND_CLAIM        | BOOLEAN           |               |
| IS_HOUSE_CALL_CLAIM        | BOOLEAN           |               |
| IS_FIRSTHAND_PROVIDER      | BOOLEAN           |               |

## ` TRANSFORMED_DATA._TEMP.AL_AI_BASELINE_SCORING `

| Column Name             | Data Type         | Description   |
|:------------------------|:------------------|:--------------|
| MARKET                  | VARCHAR(16777216) |               |
| FH_ID                   | NUMBER(38,5)      |               |
| SOURCE_INTERACTION_DATE | DATE              |               |
| CATEGORY                | VARCHAR(16777216) |               |
| SCORE                   | NUMBER(38,0)      |               |
| CONFIDENCE              | NUMBER(38,0)      |               |
| EVIDENCE                | VARCHAR(16777216) |               |
| COMBINED_NOTES          | VARCHAR(16777216) |               |
| RAW_RESPONSE            | VARCHAR(16777216) |               |
| POPULATION_BASELINE     | NUMBER(38,3)      |               |
| MARKET_BASELINE         | NUMBER(38,3)      |               |
| INDIVIDUAL_BASELINE     | NUMBER(38,3)      |               |

## ` TRANSFORMED_DATA._TEMP.AL_AI_PROMPTS `

| Column Name   | Data Type     | Description   |
|:--------------|:--------------|:--------------|
| PROMPT_NAME   | VARCHAR(24)   |               |
| PROMPT_TEXT   | VARCHAR(3750) |               |

## ` TRANSFORMED_DATA._TEMP.EVENTS_DAILY_AGG `

| Column Name                      | Data Type         | Description   |
|:---------------------------------|:------------------|:--------------|
| MEMBER_ID                        | NUMBER(38,5)      |               |
| EVENT_DATE                       | DATE              |               |
| DOB                              | DATE              |               |
| GENDER                           | VARCHAR(16777216) |               |
| ENGAGEMENT_GROUP                 | VARCHAR(27)       |               |
| NORMALIZED_COVERAGE_CATEGORY     | VARCHAR(16777216) |               |
| MONTHS_SINCE_BATCHED             | NUMBER(9,0)       |               |
| CNT_CLAIM_DX                     | NUMBER(13,0)      |               |
| CNT_CLAIM_PROC                   | NUMBER(13,0)      |               |
| CNT_CLAIM_REV                    | NUMBER(13,0)      |               |
| CNT_CLAIM_EVENTS                 | NUMBER(13,0)      |               |
| ANY_ED_ON_DATE                   | NUMBER(1,0)       |               |
| ANY_IP_ON_DATE                   | NUMBER(1,0)       |               |
| PAID_SUM                         | FLOAT             |               |
| CNT_HCC_DIABETES                 | NUMBER(38,0)      |               |
| CNT_HCC_MENTAL_HEALTH            | NUMBER(38,0)      |               |
| CNT_HCC_CARDIOVASCULAR           | NUMBER(38,0)      |               |
| CNT_HCC_PULMONARY                | NUMBER(38,0)      |               |
| CNT_HCC_KIDNEY                   | NUMBER(38,0)      |               |
| CNT_HCC_SUD                      | NUMBER(38,0)      |               |
| CNT_ANY_HCC                      | NUMBER(13,0)      |               |
| CNT_PROC_PSYCHOTHERAPY           | NUMBER(13,0)      |               |
| CNT_PROC_PSYCHIATRIC_EVALS       | NUMBER(13,0)      |               |
| NOTE_HEALTH_SCORE                | NUMBER(38,0)      |               |
| NOTE_RISK_HARM_SCORE             | NUMBER(38,0)      |               |
| NOTE_SOCIAL_STAB_SCORE           | NUMBER(38,0)      |               |
| NOTE_MED_ADHERENCE_SCORE         | NUMBER(38,0)      |               |
| NOTE_CARE_ENGAGEMENT_SCORE       | NUMBER(38,0)      |               |
| NOTE_PROGRAM_TRUST_SCORE         | NUMBER(38,0)      |               |
| NOTE_SELF_SCORE                  | NUMBER(38,0)      |               |
| HEALTH_POP_BASELINE              | NUMBER(38,3)      |               |
| HEALTH_INDIV_BASELINE            | NUMBER(38,3)      |               |
| HEALTH_MARKET_BASELINE           | NUMBER(38,3)      |               |
| RISKHARM_MARKET_BASELINE         | NUMBER(38,3)      |               |
| SOCIAL_MARKET_BASELINE           | NUMBER(38,3)      |               |
| MEDADH_MARKET_BASELINE           | NUMBER(38,3)      |               |
| CAREENG_MARKET_BASELINE          | NUMBER(38,3)      |               |
| PROGTRUST_MARKET_BASELINE        | NUMBER(38,3)      |               |
| SELF_MARKET_BASELINE             | NUMBER(38,3)      |               |
| RX_DAYS_ANY                      | NUMBER(10,0)      |               |
| RX_DAYS_ANTIPSYCH                | NUMBER(10,0)      |               |
| RX_DAYS_INSULIN                  | NUMBER(10,0)      |               |
| RX_DAYS_ORAL_ANTIDIAB            | NUMBER(10,0)      |               |
| RX_DAYS_STATIN                   | NUMBER(10,0)      |               |
| RX_DAYS_BETA_BLOCKER             | NUMBER(10,0)      |               |
| RX_DAYS_OPIOID                   | NUMBER(10,0)      |               |
| CNT_OUTPATIENT_60D               | NUMBER(18,0)      |               |
| Y_ED_30D                         | NUMBER(1,0)       |               |
| Y_ED_60D                         | NUMBER(1,0)       |               |
| Y_ED_90D                         | NUMBER(1,0)       |               |
| Y_IP_30D                         | NUMBER(1,0)       |               |
| Y_IP_60D                         | NUMBER(1,0)       |               |
| Y_IP_90D                         | NUMBER(1,0)       |               |
| Y_ANY_30D                        | NUMBER(1,0)       |               |
| Y_ANY_60D                        | NUMBER(1,0)       |               |
| Y_ANY_90D                        | NUMBER(1,0)       |               |
| INCONSISTENCY_MED_NONCOMPLIANCE  | NUMBER(1,0)       |               |
| INCONSISTENCY_APPT_NONCOMPLIANCE | NUMBER(1,0)       |               |

## ` TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX `

| Column Name                  | Data Type         | Description   |
|:-----------------------------|:------------------|:--------------|
| MEMBER_ID                    | NUMBER(38,5)      |               |
| MARKET                       | VARCHAR(16777216) |               |
| DOB                          | DATE              |               |
| GENDER                       | VARCHAR(16777216) |               |
| ENGAGEMENT_GROUP             | VARCHAR(16777216) |               |
| NORMALIZED_COVERAGE_CATEGORY | VARCHAR(16777216) |               |
| MONTHS_SINCE_BATCHED         | NUMBER(9,0)       |               |
| EVENT_DATE                   | DATE              |               |
| EVENT_TYPE                   | VARCHAR(15)       |               |
| CLAIM_ID                     | VARCHAR(16777216) |               |
| CLAIM_LINE_NUMBER            | NUMBER(38,0)      |               |
| PLACE_OF_SERVICE             | VARCHAR(16777216) |               |
| REVENUE_CODE                 | VARCHAR(16777216) |               |
| CODE_TYPE                    | VARCHAR(13)       |               |
| CODE                         | VARCHAR(16777216) |               |
| CODE_FAMILY                  | VARCHAR(16777216) |               |
| HCC_CATEGORY                 | NUMBER(38,0)      |               |
| CNT_HCC_DIABETES             | NUMBER(38,0)      |               |
| CNT_HCC_MENTAL_HEALTH        | NUMBER(38,0)      |               |
| CNT_HCC_CARDIOVASCULAR       | NUMBER(38,0)      |               |
| CNT_HCC_PULMONARY            | NUMBER(38,0)      |               |
| CNT_HCC_KIDNEY               | NUMBER(38,0)      |               |
| CNT_HCC_SUD                  | NUMBER(38,0)      |               |
| CNT_HCC_OTHER_COMPLEX        | NUMBER(38,0)      |               |
| HCPCS_CATEGORY               | VARCHAR(16777216) |               |
| HCPCS_CATEGORY_SHORT         | VARCHAR(16777216) |               |
| HCPCS_CODE                   | VARCHAR(16777216) |               |
| PAID_AMOUNT                  | FLOAT             |               |
| IS_ED_EVENT                  | BOOLEAN           |               |
| IS_IP_EVENT                  | BOOLEAN           |               |
| SCORE                        | NUMBER(38,0)      |               |
| CONFIDENCE                   | NUMBER(38,0)      |               |
| POPULATION_BASELINE          | NUMBER(38,3)      |               |
| MARKET_BASELINE              | NUMBER(38,3)      |               |
| INDIVIDUAL_BASELINE          | NUMBER(38,3)      |               |
| EVIDENCE                     | VARCHAR(16777216) |               |
| COMBINED_NOTES               | VARCHAR(16777216) |               |
| RAW_RESPONSE                 | VARCHAR(16777216) |               |
| Y_ED_30D                     | NUMBER(1,0)       |               |
| Y_ED_60D                     | NUMBER(1,0)       |               |
| Y_ED_90D                     | NUMBER(1,0)       |               |
| Y_IP_30D                     | NUMBER(1,0)       |               |
| Y_IP_60D                     | NUMBER(1,0)       |               |
| Y_IP_90D                     | NUMBER(1,0)       |               |
| Y_ANY_30D                    | NUMBER(1,0)       |               |
| Y_ANY_60D                    | NUMBER(1,0)       |               |
| Y_ANY_90D                    | NUMBER(1,0)       |               |
| RX_DAYS_ANY                  | NUMBER(10,0)      |               |
| RX_DAYS_ANTIPSYCH            | NUMBER(10,0)      |               |
| RX_DAYS_INSULIN              | NUMBER(10,0)      |               |
| RX_DAYS_ORAL_ANTIDIAB        | NUMBER(10,0)      |               |
| RX_DAYS_STATIN               | NUMBER(10,0)      |               |
| RX_DAYS_BETA_BLOCKER         | NUMBER(10,0)      |               |
| RX_DAYS_OPIOID               | NUMBER(10,0)      |               |

## ` TRANSFORMED_DATA._TEMP.MODELING_DATASET `

| Column Name                       | Data Type    | Description   |
|:----------------------------------|:-------------|:--------------|
| MEMBER_ID                         | NUMBER(38,5) |               |
| EVENT_DATE                        | DATE         |               |
| DOB                               | DATE         |               |
| DAY_OF_WEEK                       | NUMBER(2,0)  |               |
| MONTH                             | NUMBER(2,0)  |               |
| YEAR                              | NUMBER(4,0)  |               |
| AGE                               | NUMBER(17,6) |               |
| IS_MALE                           | NUMBER(1,0)  |               |
| IS_FEMALE                         | NUMBER(1,0)  |               |
| IS_ENGAGED                        | NUMBER(1,0)  |               |
| IS_SELECTED_NOT_ENGAGED           | NUMBER(1,0)  |               |
| IS_NOT_SELECTED_FOR_ENGAGEMENT    | NUMBER(1,0)  |               |
| IS_CATEGORY_TANF                  | NUMBER(1,0)  |               |
| IS_CATEGORY_EXPANSION             | NUMBER(1,0)  |               |
| IS_CATEGORY_DSNP                  | NUMBER(1,0)  |               |
| IS_CATEGORY_ABD                   | NUMBER(1,0)  |               |
| MONTHS_SINCE_BATCHED              | NUMBER(9,0)  |               |
| CNT_CLAIM_DX                      | NUMBER(25,0) |               |
| CNT_CLAIM_PROC                    | NUMBER(25,0) |               |
| CNT_CLAIM_REV                     | NUMBER(25,0) |               |
| CNT_CLAIM_EVENTS                  | NUMBER(25,0) |               |
| ANY_ED_ON_DATE                    | NUMBER(1,0)  |               |
| ANY_IP_ON_DATE                    | NUMBER(1,0)  |               |
| PAID_SUM                          | FLOAT        |               |
| CNT_HCC_DIABETES                  | NUMBER(38,0) |               |
| CNT_HCC_MENTAL_HEALTH             | NUMBER(38,0) |               |
| CNT_HCC_CARDIOVASCULAR            | NUMBER(38,0) |               |
| CNT_HCC_PULMONARY                 | NUMBER(38,0) |               |
| CNT_HCC_KIDNEY                    | NUMBER(38,0) |               |
| CNT_HCC_SUD                       | NUMBER(38,0) |               |
| CNT_ANY_HCC                       | NUMBER(25,0) |               |
| CNT_PROC_PSYCHOTHERAPY            | NUMBER(25,0) |               |
| CNT_PROC_PSYCHIATRIC_EVALS        | NUMBER(25,0) |               |
| NOTE_HEALTH_SCORE                 | NUMBER(38,6) |               |
| NOTE_RISK_HARM_SCORE              | NUMBER(38,6) |               |
| NOTE_SOCIAL_STAB_SCORE            | NUMBER(38,6) |               |
| NOTE_MED_ADHERENCE_SCORE          | NUMBER(38,6) |               |
| NOTE_CARE_ENGAGEMENT_SCORE        | NUMBER(38,6) |               |
| NOTE_PROGRAM_TRUST_SCORE          | NUMBER(38,6) |               |
| NOTE_SELF_SCORE                   | NUMBER(38,6) |               |
| HEALTH_POP_BASELINE               | NUMBER(38,3) |               |
| HEALTH_INDIV_BASELINE             | NUMBER(38,3) |               |
| RX_DAYS_ANY                       | NUMBER(22,0) |               |
| RX_DAYS_ANTIPSYCH                 | NUMBER(22,0) |               |
| RX_DAYS_INSULIN                   | NUMBER(22,0) |               |
| RX_DAYS_ORAL_ANTIDIAB             | NUMBER(22,0) |               |
| RX_DAYS_STATIN                    | NUMBER(22,0) |               |
| RX_DAYS_BETA_BLOCKER              | NUMBER(22,0) |               |
| RX_DAYS_OPIOID                    | NUMBER(22,0) |               |
| INCONSISTENCY_MED_NONCOMPLIANCE   | NUMBER(1,0)  |               |
| INCONSISTENCY_APPT_NONCOMPLIANCE  | NUMBER(1,0)  |               |
| NOTE_HEALTH_DELTA_30D             | NUMBER(38,9) |               |
| NOTE_RISK_HARM_DELTA_30D          | NUMBER(38,9) |               |
| NOTE_SOCIAL_STAB_DELTA_30D        | NUMBER(38,9) |               |
| NOTE_MED_ADHERENCE_DELTA_30D      | NUMBER(38,9) |               |
| NOTE_CARE_ENGAGEMENT_DELTA_30D    | NUMBER(38,9) |               |
| DAYS_SINCE_LAST_HEALTH_NOTE       | NUMBER(9,0)  |               |
| DAYS_SINCE_LAST_RISK_NOTE         | NUMBER(9,0)  |               |
| CLAIMS_IN_LAST_30D_COUNT          | NUMBER(37,0) |               |
| ED_IN_LAST_30D_COUNT              | NUMBER(13,0) |               |
| IP_IN_LAST_30D_COUNT              | NUMBER(13,0) |               |
| IS_FIRST_DIABETES                 | BOOLEAN      |               |
| IS_FIRST_MENTAL_HEALTH            | BOOLEAN      |               |
| IS_FIRST_CARDIOVASCULAR           | BOOLEAN      |               |
| IS_FIRST_PULMONARY                | BOOLEAN      |               |
| IS_FIRST_KIDNEY                   | BOOLEAN      |               |
| IS_FIRST_SUD                      | BOOLEAN      |               |
| NEW_RX_ANTIPSYCH_30D              | NUMBER(1,0)  |               |
| NEW_RX_INSULIN_30D                | NUMBER(1,0)  |               |
| MONTHS_SINCE_FIRST_DIABETES       | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_MENTAL_HEALTH  | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_CARDIOVASCULAR | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_PULMONARY      | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_KIDNEY         | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_SUD            | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_ANTIPSYCH      | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_INSULIN        | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_ORAL_ANTIDIAB  | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_STATIN         | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_BETA_BLOCKER   | NUMBER(9,0)  |               |
| MONTHS_SINCE_FIRST_OPIOID         | NUMBER(9,0)  |               |
| Y_ANY_30D                         | NUMBER(1,0)  |               |
| Y_ANY_60D                         | NUMBER(1,0)  |               |
| Y_ANY_90D                         | NUMBER(1,0)  |               |
| Y_ED_30D                          | NUMBER(1,0)  |               |
| Y_ED_60D                          | NUMBER(1,0)  |               |
| Y_ED_90D                          | NUMBER(1,0)  |               |
| Y_IP_30D                          | NUMBER(1,0)  |               |
| Y_IP_60D                          | NUMBER(1,0)  |               |
| Y_IP_90D                          | NUMBER(1,0)  |               |
| HAS_CARE_NOTES_POST_PERIOD        | NUMBER(1,0)  |               |
| DATASET_SPLIT                     | VARCHAR(5)   |               |

