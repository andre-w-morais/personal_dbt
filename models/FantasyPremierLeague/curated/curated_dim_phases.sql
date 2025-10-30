SELECT
    phase_id
    , start_event_id
    , stop_event_id
    , phase_name
    , highest_score
    , valid_from
    , valid_to
FROM {{ref("cleansed_phases")}}