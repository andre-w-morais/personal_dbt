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
    , extraction_timestamp AS extracted_at
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
  FROM {{ source("fantasy_premier_league", "raw_fpl_teams") }}