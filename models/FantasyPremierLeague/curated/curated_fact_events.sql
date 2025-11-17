SELECT
    event_id
    , most_captained_element_id
    , most_vice_captained_element_id
    , most_selected_element_id
    , most_transferred_in_element_id
    , top_element_id
    , average_entry_score
    , transfers_made
    , total_fpl_teams
    , highest_score
FROM {{ ref("cleansed_events") }}
WHERE valid_to = '9999-12-31 00:00:00.000+00:00'