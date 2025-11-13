SELECT
    fes.now_cost
    , fes.now_cost_rank_type
    , fes.cost_change_start
    , fes.selected_by_percent
    , fes.selected_rank_type
    , fes.transfers_in
    , fes.transfers_out
    , fes.transfers_in_event
    , fes.transfers_out_event
    , fes.selected_by_percent
    , fes.selected_rank
    , fes.selected_rank_type
    , fes.ict_index
    , fes.ict_index_rank_type
    , fes.threat
    , fes.threat_rank_type
    , fes.creativity
    , fes.creativity_rank_type
    , fes.influence
    , fes.influence_rank_type
    , fes.minutes_played
    , fes.starts
    , fes.total_points
    , fes.points_per_game
    , fes.points_per_game_rank_type
    , fes.form
    , fes.form_rank_type
    , fes.goals_scored
    , fes.expected_goals
    , fes.expected_goals_per_90
    , fes.assists
    , fes.expected_assists
    , fes.expected_assists_per_90
    , fes.expected_goal_involvements
    , fes.expected_goal_involvements_per_90
    , fes.clean_sheets
    , fes.clean_sheets_per_90
    , fes.goals_conceded
    , fes.goals_conceded_per_90
    , fes.expected_goals_conceded
    , fes.expected_goals_conceded_per_90
    , fes.defensive_contribution
    , fes.defensive_contribution_per_90
    , fes.clearances_blocks_interceptions
    , fes.tackles
    , fes.recoveries
    , fes.saves
    , fes.saves_per_90
    , fes.penalties_saved
    , fes.penalties_missed
    , fes.own_goals
    , fes.yellow_cards
    , fes.red_cards
    , fes.bps
    , fes.bonus
    , fes.ep_this
    , fes.ep_next
    , fes.snapshot_at
    , fes.sort_latest_record
    , de.element_web_name
    , CONCAT(de.element_first_name, ' ', element_second_name) AS element_full_name
    , de.status
    , de.news
    , det.short_element_type_name AS element_type
    , det.element_count
    , dt.short_team_name AS team
    , dt.strength
    , dt.strength_overall_home
    , dt.strength_overall_away
    , dt.strength_attack_home
    , dt.strength_attack_away
    , dt.strength_defence_home
    , dt.strength_defence_away
    , dt.position
FROM {{ ref("curated_fact_elements_snapshot") }} AS fes
    LEFT JOIN {{ ref("curated_dim_elements") }} AS de ON de.element_code = fes.element_code AND (fes.snapshot_at BETWEEN de.valid_from AND de.valid_to)
    LEFT JOIN {{ ref("curated_dim_element_types") }} AS det ON det.element_type_id = fes.element_type_id AND (fes.snapshot_at BETWEEN det.valid_from AND det.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dt ON dt.team_code = fes.team_code AND (fes.snapshot_at BETWEEN dt.valid_from AND dt.valid_to) 