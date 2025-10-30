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
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
    , CAST(extraction_timestamp AS DATE) AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31' AS DATE)
        ELSE LAG(CAST(extraction_timestamp AS DATE)) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
  FROM {{ source("fantasy_premier_league", "raw_fpl_teams") }}