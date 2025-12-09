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
    , row_number() over(partition by id order by extraction_timestamp asc) as sort_first_record
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
    , most_captained AS most_captained_element_id
    , most_vice_captained AS most_vice_captained_element_id
    , top_element AS top_element_id
    , most_selected AS most_selected_element_id
    , highest_score
    , average_entry_score
    , highest_scoring_entry
    , ranked_count AS total_fpl_teams
    , transfers_made
    , most_transferred_in AS most_transferred_in_element_id
    , CASE
        WHEN sort_first_record = 1 THEN CAST('2025-08-15 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE extraction_timestamp 
        END AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE LAG(extraction_timestamp) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
  FROM ordering