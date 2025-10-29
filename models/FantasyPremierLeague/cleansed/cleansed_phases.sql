WITH
    deduplication AS (
        SELECT
            id,
            highest_score,
            start_event,
            stop_event,
            name,
            ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY extraction_timestamp DESC
            ) AS sort_latest_record
        FROM {{ source("fantasy_premier_league", "raw_fpl_phases") }}
    )
SELECT
    id AS phase_id,
    highest_score,
    start_event AS start_event_id,
    stop_event AS stop_event_id,
    name AS phase_name
FROM deduplication
WHERE sort_latest_record = 1
