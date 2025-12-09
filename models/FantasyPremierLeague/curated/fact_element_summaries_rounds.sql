SELECT
    cesr.element_id
    , cesr.event_id
    , cesr.fixture_id
    , ce.element_type_id
    , ce.team_id
    , cesr.opponent_team_id
    , cesr.kickoff_at
    , ce.valid_from
    , ce.valid_to
    , cesr.home_away
    , cesr.team_score
    , cesr.opponent_team_score
    , cesr.total_points
    , cesr.bps
    , cesr.bonus
    , cesr.starts
    , cesr.minutes_played
    , cesr.goals_scored
    , cesr.expected_goals
    , cesr.assists
    , cesr.expected_assists
    , cesr.expected_goal_involvements
    , cesr.goals_conceded
    , cesr.expected_goals_conceded
    , cesr.clean_sheets
    , cesr.saves
    , cesr.own_goals
    , cesr.penalties_saved
    , cesr.penalties_missed
    , cesr.tackles
    , cesr.recoveries
    , cesr.clearances_blocks_interceptions
    , cesr.defensive_contribution
    , cesr.yellow_cards
    , cesr.red_cards
    , cesr.ict_index
    , cesr.threat
    , cesr.creativity
    , cesr.influence
    , cesr.transfers_in
    , cesr.transfers_out
    , cesr.transfers_balance
    , cesr.selected
    , cesr.value
FROM {{ref("stg_element_summaries_round")}} AS cesr
    LEFT JOIN {{ref("cleansed_elements")}} AS ce ON ce.element_id = cesr.element_id AND (cesr.kickoff_at BETWEEN ce.valid_from AND ce.valid_to)