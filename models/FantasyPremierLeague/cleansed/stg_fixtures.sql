with deduplication as (
  SELECT
    id
    , code
    , kickoff_time
    , provisional_start_time
    , started
    , finished
    , finished_provisional
    , event
    , pulse_id
    , team_h
    , team_h_difficulty
    , team_h_score
    , team_a
    , team_a_difficulty
    , team_a_score
    , minutes
    , extraction_timestamp
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
    , row_number() over(partition by id order by extraction_timestamp asc) as sort_first_record
  FROM {{ source("fantasy_premier_league", "raw_fpl_fixtures") }}
)
SELECT
      id AS fixture_id
    , code as fixture_code
    , kickoff_time AS kickoff_at
    , provisional_start_time
    , started AS is_started
    , finished AS is_finished
    , finished_provisional AS is_provisionally_finished
    , event AS event_id
    , pulse_id
    , team_h AS team_h_id
    , team_h_difficulty
    , team_a AS team_a_id
    , team_a_difficulty
    , team_h_score
    , team_a_score
    , minutes AS minutes_played
    , CASE
        WHEN sort_first_record = 1 THEN CAST('2025-08-15 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE extraction_timestamp 
        END AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE LAG(extraction_timestamp) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
FROM deduplication