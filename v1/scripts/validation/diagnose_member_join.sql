
/*
============================================================================
DIAGNOSTIC SCRIPT: Investigate Failed Member Joins
PURPOSE: This script identifies why a large number of events in the
         'EVENTS_WITH_LABELS_RX' table are failing to join with the
         'members' CTE from the event creation script.
============================================================================
*/

-- Set the FQNs for the tables involved in the analysis
SET EVENTS_TABLE_FQN = 'TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX';
SET MEMBERLIST_FQN = 'TRANSFORMED_DATA.PROD.FH_MEMBERS';
SET MEMBER_QUAL_FQN = 'TRANSFORMED_DATA.prod.fh_member_selection_qualification';


-- Diagnostic Query 1: Summarize the reasons for join failure
WITH
-- Step 1: Get a distinct list of member_ids from the events table that failed to join
unmatched_member_ids AS (
    SELECT DISTINCT member_id
    FROM IDENTIFIER($EVENTS_TABLE_FQN)
    WHERE market IS NULL -- 'market' is a proxy for a successful join
      AND member_id IS NOT NULL
),

-- Step 2: Check the status of these unmatched members in the source tables
member_status_check AS (
    SELECT
        um.member_id,
        -- Check if the member exists in the main member list
        IFF(m.FH_ID IS NOT NULL, TRUE, FALSE) AS exists_in_member_list,
        -- Check their clinical qualification status
        COALESCE(q.is_fh_clinically_qualified, -1) AS clinical_qualification_status, -- Using -1 for 'not found'
        -- Check their coverage category
        COALESCE(UPPER(TRIM(m.fh_coverage_category)), 'NOT_IN_MEMBER_LIST') AS coverage_category
    FROM unmatched_member_ids um
    LEFT JOIN IDENTIFIER($MEMBERLIST_FQN) m ON um.member_id = m.FH_ID
    LEFT JOIN IDENTIFIER($MEMBER_QUAL_FQN) q ON um.member_id = q.fh_id
)

-- Step 3: Aggregate the results to get a summary of failure reasons
SELECT
    CASE
        WHEN NOT exists_in_member_list THEN 'Member ID not found in FH_MEMBERS list'
        WHEN clinical_qualification_status = 0 THEN 'Exists but is not clinically qualified'
        WHEN clinical_qualification_status = -1 THEN 'Exists but has no entry in qualification table'
        WHEN coverage_category IN ('NULL', 'EXCLUDE') THEN 'Exists but has an excluded coverage category'
        ELSE 'Other reason'
    END AS failure_reason,
    COUNT(DISTINCT member_id) AS count_of_unmatched_members
FROM member_status_check
GROUP BY 1
ORDER BY 2 DESC;


-- Diagnostic Query 2: Show a sample of 10 member IDs and their specific failure attributes
WITH
unmatched_member_ids AS (
    SELECT DISTINCT member_id
    FROM IDENTIFIER($EVENTS_TABLE_FQN)
    WHERE market IS NULL AND member_id IS NOT NULL
    LIMIT 10 -- Limit to a small sample for detailed review
)
SELECT
    um.member_id,
    m.FH_ID IS NOT NULL AS is_in_member_list,
    q.is_fh_clinically_qualified,
    m.fh_coverage_category
FROM unmatched_member_ids um
LEFT JOIN IDENTIFIER($MEMBERLIST_FQN) m ON um.member_id = m.FH_ID
LEFT JOIN IDENTIFIER($MEMBER_QUAL_FQN) q ON um.member_id = q.fh_id;
