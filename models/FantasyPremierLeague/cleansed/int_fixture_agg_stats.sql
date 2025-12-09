SELECT
    esr.fixture_id
    , e.team_id
    , SUM(esr.expected_goals) AS expected_goals
    , SUM(esr.expected_assists) AS expected_assists
    , MAX(esr.expected_goals_conceded) AS expected_goals_conceded
    , SUM(esr.saves) AS saves
    , SUM(esr.own_goals) AS own_goals
    , SUM(esr.penalties_missed) AS penalties_missed
    , SUM(esr.penalties_saved) AS penalties_saved
    , SUM(esr.tackles) AS tackles
    , SUM(esr.recoveries) AS recoveries
    , SUM(esr.clearances_blocks_interceptions) AS clearances_blocks_interceptions
    , SUM(esr.yellow_cards) AS yellow_cards
    , SUM(esr.red_cards) AS red_cards
FROM {{ref("stg_element_summaries_round")}} AS esr
    LEFT JOIN {{ref("stg_elements")}} AS e ON e.element_id = esr.element_id AND (esr.kickoff_at >= e.valid_from AND esr.kickoff_at < e.valid_to)
GROUP BY esr.fixture_id, e.team_id