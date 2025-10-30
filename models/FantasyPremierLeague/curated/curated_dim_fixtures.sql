SELECT
    fixture_code
    , fixture_id
    , event_id
    , kickoff_at
    , is_started
    , is_finished
    , is_provisionally_finished
    , valid_from
    , valid_to
FROM {{ref("cleansed_fixtures")}}