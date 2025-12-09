SELECT
    team_code
    , team_id
    , pulse_id
    , team_name
    , short_team_name
    , strength
    , strength_overall_home
    , strength_overall_away
    , strength_attack_home
    , strength_attack_away
    , strength_defence_home
    , strength_defence_away
    , position
    , valid_from
    , valid_to
FROM {{ref("stg_teams")}}