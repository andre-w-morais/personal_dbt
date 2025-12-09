SELECT
    fes.element_id
    , de.element_web_name
    , CONCAT(de.element_first_name, ' ', de.element_second_name) AS element_full_name
    , det.short_element_type_name AS element_type
    , CASE 
        WHEN fes.was_home IS TRUE THEN 'Home'
        WHEN fes.was_home IS FALSE THEN 'Away'
        END AS home_away
    , dt.short_team_name AS team
    , dot.short_team_name AS opponent_team
    , fes.event_id AS gameweek
    , CASE
        WHEN was_home THEN fes.team_h_score
        ELSE fes.team_a_score
        END AS team_score
    , CASE
        WHEN was_home THEN fes.team_a_score
        ELSE fes.team_h_score
        END AS opponent_team_score
    , fes.kickoff_at
    , fes.total_points
    , fes.bps
    , fes.bonus
    , fes.starts
    , fes.minutes_played
    , fes.goals_scored
    , fes.expected_goals
    , fes.assists
    , fes.expected_assists
    , fes.expected_goal_involvements
    , fes.goals_conceded
    , fes.expected_goals_conceded
    , fes.clean_sheets
    , fes.saves
    , fes.own_goals
    , fes.penalties_saved
    , fes.penalties_missed
    , fes.tackles
    , fes.recoveries
    , fes.clearances_blocks_interceptions
    , fes.defensive_contribution
    , fes.yellow_cards
    , fes.red_cards
    , fes.ict_index
    , fes.threat
    , fes.creativity
    , fes.influence
    , dot.strength AS opponent_team_strength
    , dot.strength_overall_home AS opponent_team_strength_overall_home
    , dot.strength_overall_away AS opponent_team_strength_overall_away
    , dot.strength_attack_home AS opponent_team_strength_attack_home
    , dot.strength_attack_away AS opponent_team_strength_attack_away
    , dot.strength_defence_home AS opponent_team_strength_defence_home
    , dot.strength_defence_away AS opponent_team_strength_defence_away
    , fes.transfers_in
    , fes.transfers_out
    , fes.transfers_balance
    , fes.selected
FROM {{ ref("curated_fact_element_summaries_rounds") }} AS fes
    LEFT JOIN {{ref("curated_dim_elements")}} AS de ON de.element_id = fes.element_id AND (fes.kickoff_at BETWEEN de.valid_from AND de.valid_to)
    LEFT JOIN {{ref("curated_dim_element_types")}} AS det ON det.element_type_id = fes.element_type_id AND (fes.kickoff_at BETWEEN det.valid_from AND det.valid_to)
    LEFT JOIN {{ref("curated_dim_teams")}} AS dot ON dot.team_id = fes.opponent_team_id AND (fes.kickoff_at BETWEEN dot.valid_from AND dot.valid_to)
    LEFT JOIN {{ref("curated_dim_teams")}} AS dt ON dt.team_id = fes.team_id AND (fes.kickoff_at BETWEEN dt.valid_from AND dt.valid_to)
