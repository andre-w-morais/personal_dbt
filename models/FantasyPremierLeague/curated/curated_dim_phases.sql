SELECT
    phase_id
    , start_event_id
    , stop_event_id
    , phase_name
    , highest_score
FROM {{ref("cleansed_phases")}}