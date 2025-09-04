---
# Risk Grouper v2: Project Documentation & Data Dictionary

## Overview

This project contains modular SQL scripts for feature engineering, member stratification, and clinical/engagement metrics for attributed members. All outputs are regression-ready, integer flags, and Snowflake compatible. The tables and features described below are used for downstream XGBoost and ML modeling.

---

## Table of Contents

1. Project Structure & Worksheets
2. Data Dictionary: Tables Created
3. Feature Dictionary: Variables & Descriptions
4. Worksheet Labels & Descriptions

---

## 1. Project Structure & Worksheets

| Worksheet | Label | Description |
|-----------|-------|-------------|
| 00_firsthand_ETL.sql | Raw Data Extraction | Extracts all raw, client-provided data sources for attributed members. Minimal transformation for downstream ETL. |
| 01_member_months.sql | Member Month Stratification | Builds ordered member months, eligibility spans, and unified member-months table with demographics and month flags. |
| 02_insurance_variables.sql | Insurance & Market Features | Maps insurance products and market features for attributed members. |
| 03_engagement_metrics.sql | Engagement & Contact Flags | Flags for engagement, contact attempts, phone/SMS responsiveness, and comprehensive engagement trends. |
| 04_diagnosis_metrics.sql | ABD Condition Flags | Flags for major ABD conditions (diabetes, mental health, etc.) and first diagnosis per month. |
| 05_treatment_metrics.sql | ABD Treatment Metrics | Flags for treatment requirement and metrics for ABD conditions. |
| 06_cordinated_care_metrics.sql | Coordinated Care Metrics | Flags for coordinated care and provider changes for ABD conditions. |
| 07_pharmacy_metrics.sql | Pharmacy Metrics | Medication adherence, missed refills, new drugs, and multiple drugs per class. |
| 08_LAB_SNF.sql | Lab & SNF Metrics | Flags for lab work and SNF/residential stays in rolling windows. |
| 09_real_time_metrics.sql | Real-Time Alert Metrics | Flags for ED/IP events, health check escalations, and medical needs. |
| 10_notes_derived_metrics.sql | Notes-Derived Metrics | Flags for notes categories (present, score_high) per month. |
| 99_validation_queries.sql | Validation & Summary | Summary statistics and validation queries for all tables. |

---

## 2. Data Dictionary: Tables Created

| Table Name | Description | Key Columns |
|------------|-------------|------------|
| INT_MEMBER_MONTHS_ORDERED | Ordered member months for each individual | FH_ID, EFFECTIVE_MONTH_START, IS_ENROLLED |
| INT_ELIGIBILITY_SPANS | Eligibility spans and disenrollment periods | FH_ID, EFFECTIVE_MONTH_START, IS_DISENROLLED |
| INT_MEMBER_MONTHS_UNIFIED | Unified member months table with demographics and month flags | FH_ID, EFFECTIVE_MONTH_START, age_bucket, is_jan, ... |
| AL_REG_ATTRIBUTED_MEMBERS_WITH_PRODUCTS | Insurance product flags | FH_ID, IS_ABD, IS_TANF, ... |
| AL_REG_ATTRIBUTED_MEMBERS_MARKET_FEATURES | Market feature flags | FH_ID, is_market_canton, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT | Engagement flags | FH_ID, is_ever_selected, is_engaged_month, ... |
| AL_REG_ATTRIBUTED_MEMBERS_CONTACT_FLAGS | Contact attempt metrics | FH_ID, successful_contact_attempts, ... |
| AL_REG_ATTRIBUTED_MEMBERS_PHONE_RESPONSIVENESS | Phone responsiveness flags | FH_ID, is_potentially_not_responsive |
| AL_REG_ATTRIBUTED_MEMBERS_SMS_FLAGS | SMS engagement flags | FH_ID, texted_in_month, likes_to_converse_by_text_in_month |
| AL_REG_ATTRIBUTED_MEMBERS_ENGAGEMENT_TRENDS | Comprehensive engagement trends | FH_ID, started_texting, success_rate_increased, ... |
| AL_REG_ATTRIBUTED_MEMBERS_CONDITION_FLAGS | ABD condition flags | FH_ID, HAS_DIABETES, HAS_MENTAL_HEALTH, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ABD_CONDITION_FIRST_DIAGNOSIS | First diagnosis flags | FH_ID, FIRST_DIAGNOSIS_DIABETES, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ABD_DIAGNOSIS_STATUS | ABD diagnosis status | FH_ID, DIAGNOSED_DIABETES, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ABD_TREATMENT_REQUIREMENT | ABD treatment requirement | FH_ID, DIABETES_TREATMENT_REQUIRED, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ABD_TREATMENT_METRICS | ABD treatment metrics | FH_ID, DIABETES_EVER_TREATED, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ABD_TREATMENT_SUMMARY | Combined treatment summary | FH_ID, DIABETES_TREATMENT_REQUIRED, ... |
| AL_REG_ATTRIBUTED_MEMBERS_ABD_CARE_COORDINATION_SUMMARY | Coordinated care summary | FH_ID, DIABETES_COORDINATED_CARE_LAST_3_MONTHS, ... |
| AL_REG_PHARMACY_METRICS_SUMMARY | Pharmacy metrics summary | FH_ID, INSULIN_EVER_PRESCRIBED, ... |
| LAB_SNF_METRICS | Lab/SNF metrics | FH_ID, LAB_LAST_3_MONTHS, SNF_THIS_MONTH, ... |
| REAL_TIME_METRICS | Real-time alert metrics | FH_ID, HAD_ED_EVENT_THIS_MONTH, ... |
| AL_REG_ATTRIBUTED_MEMBERS_NOTES_DERIVED | Notes-derived metrics | FH_ID, self_present_this_month, ... |

---

## 3. Feature Dictionary: Variables & Descriptions

Below are key variables created in each worksheet, with descriptions for ML modeling.

### Member Month Stratification
- `IS_ENROLLED`: Member is continuously enrolled in the month.
- `IS_REINSTATED`: Member was reinstated after disenrollment.
- `IS_DISENROLLED`: Member was disenrolled in the month.
- `is_jan` ... `is_dec`: Flags for each calendar month.
- `is_male`, `is_female`, `is_gender_unknown`: Gender flags.
- `age_bucket`, `is_age_15_19` ... `is_age_100_plus`: Age stratification flags.

### Insurance & Market Features
- `IS_ABD`, `IS_TANF`, `IS_EXPANSION`, `IS_DSNP`, `IS_OTHER`: Insurance product flags.
- `is_market_*`: Flags for each market region.

### Engagement & Contact Flags
- `is_ever_selected`, `is_selected_month`: Member was ever/selected in the month.
- `is_ever_engaged`, `is_engaged_month`: Member was ever/engaged in the month.
- `successful_contact_attempts`, `unsuccessful_contact_attempts`, `total_contact_attempts`: Contact metrics.
- `is_tried_contact_in_month`, `is_succefful_contact_in_month`: Contact attempt flags.
- `success_rate_this_month`: Contact success rate.
- `is_intense_attempt_in_month`, `is_intense_support_in_month`: Intensity flags.
- `is_potentially_not_responsive`: Phone responsiveness flag.
- `texted_in_month`, `likes_to_converse_by_text_in_month`: SMS engagement flags.
- `started_texting`, `stopped_texting`, `started_intense_texting`, `stopped_intense_texting`: SMS trend flags.
- `success_rate_increased`, `success_rate_decreased`: Contact success rate trends.

### ABD Condition & Diagnosis Flags
- `HAS_DIABETES`, `HAS_MENTAL_HEALTH`, ...: Flags for major ABD conditions.
- `FIRST_DIAGNOSIS_DIABETES`, ...: First diagnosis flags per month.

### ABD Treatment Metrics
- `DIABETES_TREATMENT_REQUIRED`, ...: Treatment requirement flags.
- `DIABETES_EVER_TREATED`, ...: Ever treated flags.
- `DIABETES_TREATED_LAST_3_MONTHS`, ...: Treated in last 3 months.
- `DIABETES_TREATED_THIS_MONTH`, ...: Treated in current month.

### Coordinated Care Metrics
- `DIABETES_COORDINATED_CARE_LAST_3_MONTHS`, ...: Coordinated care flags (single provider).
- `DIABETES_NEW_PROVIDER_THIS_MONTH`, ...: New provider flags.

### Pharmacy Metrics
- `INSULIN_EVER_PRESCRIBED`, ...: Ever prescribed flags.
- `NUM_INSULIN_DRUGS_LAST_3_MONTHS`, ...: Number of drugs prescribed in last 3 months.
- `INSULIN_MPR_LAST_3_MONTHS`, ...: Medication possession ratio (adherence).
- `INSULIN_MED_ADHERENT`, ...: Adherence flag (MPR >= 0.8).
- `INSULIN_MISSED_REFILL_THIS_MONTH`, ...: Missed refill flag.
- `INSULIN_NEW_DRUG_THIS_MONTH`, ...: New drug prescribed flag.

### Lab & SNF Metrics
- `LAB_LAST_3_MONTHS`, `LAB_LAST_6_MONTHS`, `LAB_LAST_12_MONTHS`: Lab work flags.
- `SNF_THIS_MONTH`, `SNF_LAST_3_MONTHS`: SNF/residential stay flags.

### Real-Time Alert Metrics
- `HAD_ED_EVENT_THIS_MONTH`, `HAD_IP_EVENT_THIS_MONTH`: ED/IP event flags.
- `HAD_ED_EVENT_LAST_3_MONTHS`, `HAD_IP_EVENT_LAST_3_MONTHS`: Rolling ED/IP event flags.
- `HAD_DETERIORATING_CONDITION_THIS_MONTH`: Health check flags.
- `HAD_MEDICATION_CONCERN_THIS_MONTH`, `HAD_MEDICAL_NEEDS_THIS_MONTH`: Medical concern flags.
- `NEEDED_TRIAGE_ESCALATION_THIS_MONTH`, `TRIAGE_ESCALATION_RESOLVED_THIS_MONTH`, `TRIAGE_ESCALATION_UNRESOLVED_THIS_MONTH`: Escalation flags.

### Notes-Derived Metrics
- `self_present_this_month`, ...: Category present flags.
- `self_score_high_this_month`, ...: Category score high flags (score >= 3).

---

## 4. Validation & Summary

- `99_validation_queries.sql` provides summary statistics and validation queries for all tables and key flags.

---

## Usage for ML Modeling

All features are integer flags or regression-ready metrics, stratified by member and month. Use these features for XGBoost and other ML models to predict risk, engagement, and clinical outcomes.

---
## Attribute Mapping: Attributed Members (Client to Internal)

Below is a stepwise mapping from the client file to internal attributes, with required transformations described in prose:

1. **Unique Identifier**
  - **Client Field:** CLIENT_ID
  - **Internal Attribute:** INTERNAL_MEMBER_ID
  - **Transformation:** Direct mapping; assign CLIENT_ID as the internal unique identifier for each member.

2. **Date of Birth (DOB)**
  - **Client Field:** DOB
  - **Internal Attribute:** DOB
  - **Transformation:** Direct mapping; retain DOB for age computation and other time-based features.

3. **Gender**
  - **Client Field:** GENDER
  - **Internal Attribute:** GENDER, IS_MALE, IS_FEMALE, IS_OTHER_GENDER
  - **Transformation:** Standardize gender values to 'Male', 'Female', or 'Other' by accounting for upper/lower case and alternate codes (e.g., 'M', 'F'). Create binary flags for each gender category for modeling convenience:
    - IS_MALE: 1 if gender is 'Male', 'M', 'male', 'm', else 0
    - IS_FEMALE: 1 if gender is 'Female', 'F', 'female', 'f', else 0
    - IS_OTHER_GENDER: 1 if gender is not recognized as male or female, else 0

4. **Insurance Product**
  - **Client Field:** INSURANCE_PRODUCT
  - **Internal Attribute:** INSURANCE_PRODUCT
  - **Transformation:** Map insurance product to one of four recognized categories, accounting for case and variants:
    - 'TANF', 'TANF W/SMI', 'tanf', 'tanf w/smi' → TANF
    - 'ABD', 'ABD W/SMI', 'abd', 'abd w/smi' → ABD
    - 'EXPANSION', 'expansion' → EXPANSION
    - 'DSNP', 'dsnp' → DSNP
    - Any other value (including null) → OTHER

5. **Address**
  - **Client Field:** ADDRESS
  - **Internal Attribute:** ADDRESS, SDoH indicators (IS_FOOD_DESERT, IS_MEDICAL_SHORTAGE, IS_LOW_INCOME_NEIGHBORHOOD)
  - **Transformation:** Retain address for geospatial joins. Use zip code or address to join with external public datasets (USDA, BLS, HRSA, Census) to flag SDoH indicators such as food desert, medical shortage area, and low income neighborhood.

6. **Phone Number**
  - **Client Field:** PHONE_NUMBER
  - **Internal Attribute:** PHONE_NUMBER
  - **Transformation:** Direct mapping; retain for contact and engagement features.

7. **Member Enrollment Status**
  - **Client Field:** MEMBER_ENROLLMENT_STATUS
  - **Internal Attribute:** ENROLLMENT_STATUS
  - **Transformation:** Map client enrollment status to standardized internal categories (e.g., 'Engaged', 'Not Selected', 'Disenrolled').

8. **Date First Engaged in Program**
  - **Client Field:** DATE_FIRST_ENGAGED
  - **Internal Attribute:** DATE_FIRST_ENGAGED
  - **Transformation:** Direct mapping; retain for engagement features and time-based calculations.

9. **Computed Age at Reference Date**
  - **Client Fields:** DOB, reference date (e.g., first of month)
  - **Internal Attribute:** AGE_AT_REF
  - **Transformation:** Compute age as the difference in years between DOB and the reference date (e.g., DATEDIFF(year, DOB, 'YYYY-MM-01')).

10. **SDoH Indicators**
   - **Client Field:** ADDRESS (or ZIP)
   - **Internal Attribute:** IS_FOOD_DESERT, IS_MEDICAL_SHORTAGE, IS_LOW_INCOME_NEIGHBORHOOD
   - **Transformation:** Use address or zip code to join with public datasets (USDA Food Access Atlas, BLS, HRSA, Census) to flag SDoH indicators. If direct mapping is not possible, use string matching as a placeholder.
## Attribute Mapping & Feature Logic

Below is a repository of required attributes, their logic, and data sources. Where external data is needed, public sources are suggested for integration.

| Category                | Attribute                | Logic/Transformation                                                                 | Source (Client/Internal/External) | Public Data Source (if external)                |
|-------------------------|--------------------------|--------------------------------------------------------------------------------------|-----------------------------------|------------------------------------------------|
| Unit of work            | Member ID                | Unique identifier for each member                                                    | Client                            |                                                |
| Unit of work            | Year & month             | Aggregation period (YYYY-MM)                                                         | Internal                          |                                                |
| Demographic features    | Age                      | Age at month start/end (computed from DOB)                                           | Client                            |                                                |
| Demographic features    | Gender                   | Standardized gender (male, female, other)                                            | Client                            |                                                |
| Demographic features    | Insurance product        | Insurance product type                                                              | Client                            |                                                |
| Demographic features    | Zip code                 | Member zip code                                                                     | Client                            |                                                |
| Demographic features    | SDoH flags               | Food desert, low income neighborhood, medical shortage area (join on zip code)       | External                          | USDA Food Access Atlas, BLS, HRSA, Census       |
| Demographic features    | Interest in program      | Status: did not prioritize, could not reach, not interested, engaged                | Client                            |                                                |
| Claim features          | Diagnosis (HCC)          | Major HCC categories (last 24 mo), count, new diagnosis in month                    | Client/Internal                    |                                                |
| Claim features          | Utilization (ED/IP)      | ED/IP events by HCC/generic, months since last event                                | Client/Internal                    |                                                |
| Claim features          | OP utilization           | OP utilization by HCC (last 24 mo, in month)                                        | Client/Internal                    |                                                |
| Claim features          | Lab work                 | Lab work in month                                                                   | Client/Internal                    |                                                |
| Claim features          | SNF/Residential stay     | SNF/residential stay in month                                                       | Client/Internal                    |                                                |
| Pharmacy features       | Number of meds           | Number of medications in month                                                      | Client/Internal                    |                                                |
| Pharmacy features       | Major class of meds      | Major medication class                                                              | Client/Internal                    |                                                |
| Pharmacy features       | Days in hand             | Days in hand for each major class                                                   | Client/Internal                    |                                                |
| Pharmacy features       | Med changes              | New or actual change in medication in month                                         | Client/Internal                    |                                                |
| Days since features     | Days since event         | Days since ED/IP/treatment for condition                                            | Internal                          |                                                |
| Engagement features     | Selection status         | Not selected/selected/engaged in program                                            | Internal                          |                                                |
| Engagement features     | Is batched in month      | Is member batched in month                                                          | Internal                                                                                      |                                                |
| Engagement features     | Is engaged in month      | Is member engaged in month                                                          | Internal                          |                                                |
| Engagement features     | Months since engagement  | Months since engagement                                                             | Internal                          |                                                |
| Engagement features     | Disenrolled/lost elig.   | Is disenrolled or lost plan eligibility                                             | Internal                          |                                                |
| Engagement features     | Interim coverage loss    | Interim coverage loss                                                               | Internal                          |                                                |
| Engagement features     | Reinstated coverage      | Reinstated coverage in month                                                        | Internal                          |                                                |
| Note features           | Engagement note          | Engagement or post-engagement note                                                  | Internal                          |                                                |
| Note features           | Risk flags               | Suicidal ideation, care engagement, social/clinical stability, new risk this month   | Internal                          |                                                |
| Health check features   | Med compliance           | Individual compliant with meds                                                      | Internal                          |                                                |
| Health check features   | Medical complaints       | Individual has medical complaints                                                   | Internal                          |                                                |
| Health check features   | Guide observed stability | Guide observed stability                                                            | Internal                          |                                                |
| Health check features   | Recent hospitalization   | Recent hospitalization                                                              | Internal                          |                                                |
| Health check features   | Escalated to triage      | Escalated to triage team                                                            | Internal                          |                                                |
| ADT features            | ED/IP event              | ED/IP event in month                                                                | Internal                          |                                                |
| Contact features        | Call/SMS engagement      | Picks up calls, returns in time, missed appointments                                | Internal                          |                                                |
| Predictive targets      | Likelihood to engage     | Model output: probability of engagement                                             | Output                            |                                                |
| Predictive targets      | Likelihood of ED/IP      | Model output: probability of ED/IP in next 30/60/90 days                            | Output                            |                                                |
| Predictive targets      | Likelihood to churn      | Model output: probability of churn                                                  | Output                            |                                                |
# Risk Grouper v2 - Monthly Aggregated Dataset PRD

## Scope
Create an aggregated monthly dataset for regression analysis to:
- Predict 30/60/90 day all-cause ED & IP events
- Predict likelihood of individual engagement with care programming
- Predict likelihood of individual churn

## Approach
- Build the dataset with monthly aggregation
- Engineer and stratify features in logical sections
- Treat each section as raw input requirements, followed by feature engineering steps to reach final attributes
- Validate each feature/section before moving to the next

## Customization Requirements
- Requirement 1: Test mode—select a section of the database for processing (by market)
- Requirement 2: Option to create interim tables (persistent) or use temp table structures
- Requirement 3: Create an attribute mapping catalogue. For every attribute engineered, map its source (client/internal/external) and its role (input/output) to ensure independence from client-specific naming and facilitate clear documentation.

SQL/scripts for these requirements will be written in the corresponding module as needed.

## Process
- For each section, list raw requirements and engineered features
- Build SQL/scripts for feature engineering
- Validate with sample/test outputs
- Store validation results for review

## Step 1: Attributed Members
- **Input:** Client sends a file with:
  - Unique identifier
  - DOB
  - Gender
  - Insurance product
  - Address
  - Phone number
  - Member enrollment status
  - Date first engaged in program
- **Feature Engineering:**
  - Transform to internal unique identifier
  - Retain DOB (for age computation at different points)
  - Standardize gender (male, female, other)
  - Use address to flag SDoH indicators (food desert, medical shortage, low income neighborhood, etc.)
  - Compute age at reference dates
  - Create gender flags (is_male, is_female, is_other_gender)
  - Map enrollment status and engagement date

## SQL Sections
- **Client (Firsthand) Queries:**
  - Queries and logic for processing raw client data as received
- **ETL (Internal) Queries:**
  - Queries and logic for transforming, aggregating, and engineering features for internal analysis


---

Expand each section with specific attributes and logic as you proceed. Let me know the first section to start with, and I’ll help generate the scripts and validation workflow.

## Internal Attribute Mapping: Stepwise Example

### 1. Age at Month Start
- **Source Table/Column:**
  - AL_REG_ATTRIBUTED_MEMBERS.DOB
  - AL_REG_ATTRIBUTED_MEMBERS_MONTHS.EFFECTIVE_MONTH_START
- **Transformation:**
  - Compute age as the difference in years between DOB and EFFECTIVE_MONTH_START.
- **Internal Field Name:**
  - AGE_AT_MONTH_START
- **SQL Logic:**
  ```sql
  SELECT
    M.FH_ID,
    DATEDIFF(year, M.DOB, MM.EFFECTIVE_MONTH_START) AS AGE_AT_MONTH_START
  FROM AL_REG_ATTRIBUTED_MEMBERS M
  JOIN AL_REG_ATTRIBUTED_MEMBERS_MONTHS MM ON M.FH_ID = MM.FH_ID
  ```

### 2. Gender Flags
- **Source Table/Column:**
  - AL_REG_ATTRIBUTED_MEMBERS.GENDER
- **Transformation:**
  - Standardize gender values (e.g., 'Male', 'Female', 'Other').
  - Create binary flags for each gender category:
    - IS_MALE: 1 if gender is 'Male', 'M', 'male', 'm', else 0
    - IS_FEMALE: 1 if gender is 'Female', 'F', 'female', 'f', else 0
    - IS_OTHER_GENDER: 1 if gender is not recognized as male or female, else 0
- **Internal Field Names:**
  - IS_MALE
  - IS_FEMALE
  - IS_OTHER_GENDER
- **SQL Logic:**
  ```sql
  SELECT
    FH_ID,
    CASE WHEN GENDER IN ('Male', 'M', 'male', 'm') THEN 1 ELSE 0 END AS IS_MALE,
    CASE WHEN GENDER IN ('Female', 'F', 'female', 'f') THEN 1 ELSE 0 END AS IS_FEMALE,
    CASE WHEN GENDER NOT IN ('Male', 'M', 'female', 'f') THEN 1 ELSE 0 END AS IS_OTHER_GENDER
  FROM AL_REG_ATTRIBUTED_MEMBERS
  ```

### Validation Requirements (for Attributed Members)

1. **Unique Identifier (INTERNAL_MEMBER_ID)**
   - Must be present and unique for each member.
   - Validate: No nulls, no duplicates.

2. **Date of Birth (DOB)**
   - Must be a valid date in the expected format (e.g., YYYY-MM-DD).
   - Validate: No nulls, valid date format, reasonable age range (e.g., member age between 0 and 120).

## Internal Module: Eligibility Time Frames

### Requirements
- For each individual, use attribution (client file) and member months (client file) to construct eligibility time frames.
- Identify the first year-month an individual is enrolled (IS_ENROLLED = TRUE).
- Build continuous spans of enrollment until IS_ENROLLED becomes FALSE.
- If there are multiple spans (i.e., periods of disenrollment and re-enrollment), mark the period when IS_ENROLLED = FALSE as individual disenrolled.
- The start of every new span (re-enrollment after disenrollment) should include an IS_REINSTATED flag.
- Track all spans for each individual.

### Transformation Steps
1. Read attribution and member months from client files (use named variables in SQL).
2. For each FH_ID, order member months by EFFECTIVE_MONTH_START.
3. Identify contiguous periods where IS_ENROLLED = TRUE (enrollment spans).
4. Mark periods where IS_ENROLLED = FALSE as disenrollment.
5. For individuals with multiple spans, set IS_REINSTATED = TRUE at the start of each new span after disenrollment.
6. Create interim tables to store eligibility spans and flags.

### Validation
- Count of unique members in eligibility spans (should match attribution count).
- Count of individuals with multiple spans (re-enrollment events).
- Count of disenrollment periods.
- Count of reinstatement flags.
