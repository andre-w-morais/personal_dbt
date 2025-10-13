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
  FROM `raw_fpl.raw_fpl_teams`