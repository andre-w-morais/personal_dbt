SELECT
    event_id
    , event_name
    , deadline_time
    , deadline_time_epoch
    , is_current
    , is_previous
    , is_next
    , finished
    , highest_score
    , average_entry_score
FROM {{ref("cleansed_events")}}