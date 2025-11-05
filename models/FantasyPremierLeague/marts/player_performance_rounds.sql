SELECT
    fes.element_id
    , de.element_web_name
    , CONCAT(de.element_first_name, ' ', de.element_second_name) AS element_full_name
    , fes.event_id
    , fes.kickoff_at
    , dev.event_name
    , dt.short_team_name AS opponent_team
    , dt.strength AS opponent_team_strength
    , dt.strength_overall_home AS opponent_team_strength_overall_home
    , dt.strength_overall_away AS opponent_team_strength_overall_away
    , dt.strength_attack_home AS opponent_team_strength_attack_home
    , dt.strength_attack_away AS opponent_team_strength_attack_away
    , dt.strength_defence_home AS opponent_team_strength_defence_home
    , dt.strength_defence_away AS opponent_team_strength_defence_away
    , fes.was_home
    , fes.team_h_score
    , fes.team_a_score
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
    , fes.transfers_in
    , fes.transfers_out
    , fes.transfers_balance
    , fes.selected
FROM {{ ref("curated_fact_element_summaries_rounds") }} AS fes
    LEFT JOIN {{ref("curated_dim_elements")}} AS de ON de.element_id = fes.element_id AND (CAST(fes.kickoff_at AS DATE) BETWEEN de.valid_from AND de.valid_to)
    LEFT JOIN {{ref("curated_dim_element_types")}} AS det ON det.element_type_id = fes.element_type_id
    LEFT JOIN {{ref("curated_dim_events")}} AS dev ON dev.event_id = fes.event_id AND (DATE(fes.kickoff_at) BETWEEN dev.valid_from AND dev.valid_to)
    LEFT JOIN {{ref("curated_dim_teams")}} AS dt ON dt.team_id = fes.opponent_team_id AND (DATE(fes.kickoff_at) BETWEEN dt.valid_from AND dt.valid_to)
