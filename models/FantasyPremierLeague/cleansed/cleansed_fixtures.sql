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
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
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
    , team_h
    , team_h_difficulty
    , team_a
    , team_a_difficulty
    , team_h_score
    , team_a_score
    , minutes AS minutes_played
FROM deduplication
WHERE sort_latest_record = 1