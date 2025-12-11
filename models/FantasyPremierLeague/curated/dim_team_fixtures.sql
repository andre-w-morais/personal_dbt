SELECT
    fixture_id
    , team_id
    , home_away
    , is_started
    , is_finished
    , valid_from
    , valid_to
FROM {{ ref("stg_team_fixtures") }}