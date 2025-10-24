SELECT
    id
    , start_event
    , stop_event
    , name
    , highest_score
FROM {{ref("cleansed_general_phases")}}