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
Resolved. This was addressed during the pipeline re-architecture, which ensured a single, consistent source for member demographic information.

## 2025-08-20: High Volume of Dropped Rows Due to Null Target Variable

**Description:**
During model training, a very large number of rows were being dropped from both the training and validation sets. For example, over 3.6 million rows were dropped from the training set when preparing data for the `y_ed_30d` target.

**File:** `src/RiskGrouper.py` (data cleaning step) and `scripts/sql/daily_aggregation.sql`.

**Problematic Logic:**
The script's data preparation phase was dropping rows where the target variable was `NULL`. The root cause was that the SQL script generating the target variables was assigning `NULL` instead of `0` for non-events.

**Resolution:**
The `daily_aggregation.sql` script was updated to wrap all target labels with `COALESCE(label, 0)`. This ensures that all member-days have a non-null target value, preventing data loss.

**Status:**
Resolved.

## 2025-08-20: Data Type Mismatch for Thematic Labels

**Description:**
The initial implementation of the new thematic labels (`y_hiv_60d`, `y_smi_60d`, etc.) in `events.sql` created them as `BOOLEAN` values. However, the downstream aggregation in `daily_aggregation.sql` expected `NUMBER` values, causing a data type mismatch error when the view was created.

**File:** `scripts/sql/events.sql`

**Problematic Logic:**
The `MAX(...) OVER (...)` window function was creating boolean flags, but the final aggregation required numeric values for `MAX()` aggregation.

**Resolution:**
The logic in the `claim_daily_signal` CTE within `events.sql` was updated to `CAST(... AS NUMBER)` for all the new `y_*_60d` labels, ensuring they were created with the correct data type.

**Status:**
Resolved.

# Enhancements

## 2025-08-20: Include HCC Score as a Feature

**Description:**
The model's predictive power could be improved by including the Hierarchical Condition Category (HCC) score as a feature. This will require sourcing the HCC data and joining it into the main modeling dataset.

**Action Items:**
1.  Identify the source table for HCC scores.
2.  Update `scripts/sql/data_prep.sql` to join the HCC scores to the `MODELING_DATASET`.
3.  Ensure the new feature is included in the model training process.

**Status:**
Completed. HCC features (e.g., `cnt_hcc_diabetes_90d`) were integrated into the `daily_aggregation.sql` script.
