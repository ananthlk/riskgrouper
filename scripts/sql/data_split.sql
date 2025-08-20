/*
================================================================================
MASTER DATASET
PURPOSE: Creates a single, comprehensive dataset containing all members from all
         engagement groups, including both claims and notes data. This dataset is
         ideal for analyzing engagement bias and building a general model.
================================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.MASTER_DATASET AS
SELECT
    *
FROM
    TRANSFORMED_DATA._TEMP.MODELING_DATASET;

/*
================================================================================
ENGAGED GROUP DATASET (CLAIMS + NOTES)
PURPOSE: Creates a dataset for the 'Claims + Notes' predictive analysis, but
         strictly limited to the 'engaged_in_program' group. This allows the
         model to learn from all available features for the cohort that
         receives the care notes intervention.

KEY FILTERING LOGIC:
- We filter for records where IS_ENGAGED = 1.
- This isolates the entire history of engaged individuals. The
  HAS_CARE_NOTES_POST_PERIOD flag will act as a powerful feature.
================================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.ENGAGED_GROUP_DATASET AS
SELECT
    *
FROM
    TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE
    IS_ENGAGED = 1;

/*
================================================================================
CLAIMS ONLY DATASET
PURPOSE: Creates a dataset for the 'Claims Only' predictive analysis.
         This dataset includes all pre-period data across all engagement
         groups, meaning it contains all records where there were no
         care notes available.

KEY FILTERING LOGIC:
- We filter for records where HAS_CARE_NOTES_POST_PERIOD = 0.
- This effectively isolates the pre-period data for all members, simulating
  a model that relies only on claims data.
================================================================================
*/
CREATE OR REPLACE TABLE TRANSFORMED_DATA._TEMP.CLAIMS_ONLY_DATASET AS
SELECT
    *
FROM
    TRANSFORMED_DATA._TEMP.MODELING_DATASET
WHERE
    HAS_CARE_NOTES_POST_PERIOD = 0;
