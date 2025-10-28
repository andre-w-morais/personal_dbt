With deduplication as (
  SELECT
    id
    , name
    , deadline_time
    , deadline_time_epoch
    , release_time
    , is_current
    , is_previous
    , is_next
    , released
    , finished
    , data_checked
    , can_enter
    , can_manage
    , most_captained
    , most_vice_captained
    , top_element
    , most_selected
    , highest_score
    , average_entry_score
    , highest_scoring_entry
    , ranked_count
    , transfers_made
    , most_transferred_in
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
  FROM {{ source("fantasy_premier_league", "raw_fpl_events") }}
)
  SELECT
    id
    , name
    , deadline_time
    , deadline_time_epoch
    , release_time
    , is_current
    , is_previous
    , is_next
    , released
    , finished
    , data_checked
    , can_enter
    , can_manage
    , most_captained
    , most_vice_captained
    , top_element
    , most_selected
    , highest_score
    , average_entry_score
    , highest_scoring_entry
    , ranked_count
    , transfers_made
    , most_transferred_in
  FROM deduplication
  WHERE sort_latest_record = 1