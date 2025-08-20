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
