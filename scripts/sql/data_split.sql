/*
================================================================================
CLAIMS ONLY DATASET
PURPOSE: Creates a dataset for the 'Claims Only' predictive analysis.
         This dataset includes all pre-period data, which means it contains
         all records where there were no care notes available.

KEY FILTERING LOGIC:
- We filter for records where HAS_CARE_NOTES_POST_PERIOD = 0.
- This effectively isolates the pre-period data for all members, as the
  flag is 0 for non-engaged members and for engaged members before their first note.
================================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.CLAIMS_ONLY_DATASET AS
SELECT
    *
FROM
    TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE
    HAS_CARE_NOTES_POST_PERIOD = 0;

/*
================================================================================
CLAIMS + NOTES DATASET
PURPOSE: Creates a dataset for the 'Claims + Notes' predictive analysis.
         This dataset includes all data for the 'engaged_in_program' group,
         allowing the model to learn from features in both the pre- and post-periods.

KEY FILTERING LOGIC:
- We filter for records where IS_ENGAGED = 1.
- This isolates the entire history of engaged individuals, and the
  HAS_CARE_NOTES_POST_PERIOD flag will now act as a powerful new feature for the model.
================================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.CLAIMS_AND_NOTES_DATASET AS
SELECT
    *
FROM
    TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE
    IS_ENGAGED = 1;
