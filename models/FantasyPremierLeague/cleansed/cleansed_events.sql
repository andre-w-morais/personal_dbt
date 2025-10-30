With ordering as (
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
    , extraction_timestamp
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
  FROM {{ source("fantasy_premier_league", "raw_fpl_events") }}
)
  SELECT
    id AS event_id
    , name AS event_name
    , deadline_time AS deadline_at
    , deadline_time_epoch
    , release_time AS released_at
    , is_current
    , is_previous
    , is_next
    , released AS is_released
    , finished AS is_finished
    , data_checked AS is_data_checked
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
    , CAST(extraction_timestamp AS DATE) AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31' AS DATE)
        ELSE LAG(CAST(extraction_timestamp AS DATE)) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
  FROM ordering