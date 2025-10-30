WITH
    teams AS (
        SELECT DISTINCT
            team_id
            , short_team_name
            , strength
            , strength_overall_home
            , strength_overall_away
            , strength_attack_home
            , strength_attack_away
            , strength_defence_home
            , strength_defence_away
        FROM {{ref("curated_dim_teams")}}
    )
SELECT
    fes.element_id
    , de.element_web_name
    , CONCAT(de.element_first_name, ' ', de.element_second_name) AS element_full_name
    , fes.event_id
    , dev.event_name
    , t.short_team_name AS opponent_team
    , t.strength
    , t.strength_overall_home
    , t.strength_overall_away
    , t.strength_attack_home
    , t.strength_attack_away
    , t.strength_defence_home
    , t.strength_defence_away
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
FROM {{ref("curated_fact_element_summaries_rounds")}} AS fes
    LEFT JOIN {{ ref("curated_dim_elements") }} AS de ON de.element_id = fes.element_id
    LEFT JOIN {{ ref("curated_dim_events")}} AS dev ON dev.event_id = fes.event_id
    LEFT JOIN teams AS t ON t.team_id = fes.opponent_team_id