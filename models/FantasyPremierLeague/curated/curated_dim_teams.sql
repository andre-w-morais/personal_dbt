SELECT
    code
    , id
    , pulse_id
    , name
    , short_name
    , strength
    , strength_overall_home
    , strength_overall_away
    , strength_attack_home
    , strength_attack_away
    , strength_defence_home
    , strength_defence_away
    , position
    , extraction_timestamp as valid_from
    , CASE
        WHEN sort_latest_record = 1 THEN NULL
        ELSE LEAD(extraction_timestamp) OVER(PARTITION BY code ORDER BY extraction_timestamp ASC)
        END AS valid_to
FROM {{ref("cleansed_general_teams")}}