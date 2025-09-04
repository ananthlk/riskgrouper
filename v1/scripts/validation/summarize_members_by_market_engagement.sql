-- Set the target table from which to query
SET TARGET_TABLE = 'TRANSFORMED_DATA._TEMP.EVENTS_WITH_LABELS_RX';

-- Validation: Count of unique members by market and engagement group
SELECT
    market,
    engagement_group,
    COUNT(DISTINCT member_id) AS unique_member_count
FROM IDENTIFIER($TARGET_TABLE)
GROUP BY 1, 2
ORDER BY 1, 2;
