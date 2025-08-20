# Bug Log

## 2025-08-20: Gender Encoding Issue in `MODELING_DATASET`

**Description:**
The one-hot encoding for gender in the `MODELING_DATASET` table is not working as expected. The validation script consistently shows a large number of records where the sum of `IS_MALE`, `IS_FEMALE`, and `IS_GENDER_UNKNOWN` is not equal to 1.

**File:** `scripts/sql/data_prep.sql`

**Problematic Logic:**
The `CASE` statements for gender encoding are not correctly handling all cases, leading to data quality issues.

**Attempts to Fix:**
1.  Added `IS_GENDER_UNKNOWN` to handle `NULL` and other non-'M'/'F' values.
2.  Made the gender check case-insensitive using `UPPER()`.

**Status:**
Open. Further investigation is needed to identify the root cause of the issue.

## 2025-08-20: High Volume of Dropped Rows Due to Null Target Variable

**Description:**
During model training, a very large number of rows are being dropped from both the training and validation sets. For example, over 3.6 million rows were dropped from the training set when preparing data for the `y_ed_30d` target.

**File:** `src/RiskGrouper.py` (data cleaning step) and potentially upstream in `scripts/sql/data_prep.sql`.

**Problematic Logic:**
The script's data preparation phase includes `train_df.dropna(subset=[target], inplace=True)`. This is dropping rows where the target variable is `NULL`.

**Hypothesis:**
The root cause is likely in the SQL script that generates the target variables (`y_ed_30d`, etc.). When a member has no relevant event (e.g., no ED visit in 30 days), the target is being assigned `NULL` instead of `0`. This leads to massive data loss during the `dropna` step.

**Status:**
Open. The SQL logic for creating target variables needs to be reviewed and fixed to use `0` for non-events instead of `NULL`.

# Enhancements

## 2025-08-20: Include HCC Score as a Feature

**Description:**
The model's predictive power could be improved by including the Hierarchical Condition Category (HCC) score as a feature. This will require sourcing the HCC data and joining it into the main modeling dataset.

**Action Items:**
1.  Identify the source table for HCC scores.
2.  Update `scripts/sql/data_prep.sql` to join the HCC scores to the `MODELING_DATASET`.
3.  Ensure the new feature is included in the model training process.

**Status:**
Pending.
