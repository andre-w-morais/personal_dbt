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
WHERE valid_to = '9999-12-31 00:00:00.000+00:00'
