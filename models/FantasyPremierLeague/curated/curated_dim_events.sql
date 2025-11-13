SELECT
    event_id
    , event_name
    , deadline_at
    , deadline_time_epoch
    , is_current
    , is_previous
    , is_next
    , is_finished
    , valid_from
    , valid_to
FROM {{ref("cleansed_events")}}