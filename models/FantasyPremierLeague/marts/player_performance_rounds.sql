SELECT
    fes.element_id
    , fes.event_id
    , fes.fixture_id
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
    , fes.opponent_team_id
    , dt.strength AS oppenent_team_strength
    , dt.strength_overall_home AS oppenent_team_strength_overall_home
    , dt.strength_overall_away AS oppenent_team_strength_overall_away
    , dt.strength_attack_home AS oppenent_team_strength_attack_home
    , dt.strength_attack_away AS oppenent_team_strength_attack_away
    , dt.strength_defence_home AS oppenent_team_strength_defence_home
    , dt.strength_defence_away AS oppenent_team_strength_defence_away
    , dt.position AS oppenent_team_position
    , fes.transfers_in
    , fes.transfers_out
    , fes.transfers_balance
    , fes.selected
FROM {{ref("curated_fact_element_summaries_rounds")}} as fes
    LEFT JOIN {{ref("curated_dim_teams")}} as dt ON dt.team_id = fes.opponent_team_id AND CAST(dt.extracted_at AS DATE) = CAST(fes.kickoff_at AS DATE)