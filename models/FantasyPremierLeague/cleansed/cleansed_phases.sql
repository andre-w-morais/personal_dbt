WITH
    deduplication AS (
        SELECT
            id
            , highest_score
            , start_event
            , stop_event
            , name
            , extraction_timestamp
            , ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY extraction_timestamp DESC
            ) AS sort_latest_record
            , ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY extraction_timestamp ASC
            ) AS sort_first_record
        FROM {{ source("fantasy_premier_league", "raw_fpl_phases") }}
    )
SELECT
    id AS phase_id
    , highest_score
    , start_event AS start_event_id
    , stop_event AS stop_event_id
    , name AS phase_name
    , CASE
        WHEN sort_first_record = 1 THEN CAST('2025-08-15 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE extraction_timestamp 
        END AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE LAG(extraction_timestamp) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
FROM deduplication
