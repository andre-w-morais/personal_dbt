WITH
    ordering AS (
        SELECT
            id
            , code
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
            , extraction_timestamp
            , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
            , row_number() over(partition by id order by extraction_timestamp asc) as sort_first_record
        FROM {{ source("fantasy_premier_league", "raw_fpl_teams") }}
    )
SELECT  
    id AS team_id
    , code AS team_code
    , pulse_id
    , name AS team_name
    , short_name AS short_team_name
    , strength
    , strength_overall_home
    , strength_overall_away
    , strength_attack_home
    , strength_attack_away
    , strength_defence_home
    , strength_defence_away
    , position
    , CASE
        WHEN sort_first_record = 1 THEN CAST('2025-08-15 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE extraction_timestamp 
        END AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31 00:00:00.000+00:00' AS TIMESTAMP)
        ELSE LAG(extraction_timestamp) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
FROM ordering