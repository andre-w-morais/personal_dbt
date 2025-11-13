SELECT
    fixture_id
    , fixture_code
    , event_id
    , pulse_id
    , team_h_id
    , team_h_score
    , team_a_id
    , team_a_score
    , minutes_played
FROM {{ref("cleansed_fixtures")}}