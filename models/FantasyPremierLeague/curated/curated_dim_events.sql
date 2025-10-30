SELECT
    event_id
    , event_name
    , deadline_at
    , deadline_time_epoch
    , is_current
    , is_previous
    , is_next
    , is_finished
    , highest_score
    , average_entry_score
    , valid_from
    , valid_to
FROM {{ref("cleansed_events")}}