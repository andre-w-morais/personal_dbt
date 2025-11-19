WITH
    agg_values AS (
        SELECT DISTINCT
            esr.fixture_id
            , e.team_id
            , ROUND(SUM(esr.expected_goals), 2) AS expected_goals
            , ROUND(SUM(esr.expected_assists), 2) AS expected_assists
            , ROUND(MAX(esr.expected_goals_conceded), 2) AS expected_goals_conceded
            , SUM(esr.saves) AS saves
            , SUM(esr.own_goals) AS own_goals
            , SUM(esr.penalties_missed) AS penalties_missed
            , SUM(esr.penalties_saved) AS penalties_saved
            , SUM(esr.tackles) AS tackles
            , SUM(esr.recoveries) AS recoveries
            , SUM(esr.clearances_blocks_interceptions) AS clearances_blocks_interceptions
            , SUM(esr.yellow_cards) AS yellow_cards
            , SUM(esr.red_cards) AS red_cards
        FROM {{ref("cleansed_element_summaries_round")}} AS esr
            LEFT JOIN {{ref("cleansed_elements")}} AS e ON e.element_id = esr.element_id AND (esr.kickoff_at BETWEEN e.valid_from AND e.valid_to)
        GROUP BY esr.fixture_id, e.team_id
    )    
SELECT
    f.fixture_id
    , f.fixture_code
    , f.event_id
    , f.pulse_id
    , f.team_h_id
    , f.team_h_score
    , avh.expected_goals AS team_home_expected_goals
    , avh.expected_assists AS team_home_expected_assists
    , avh.expected_goals_conceded AS team_home_expected_goals_conceded
    , avh.saves AS team_home_saves
    , avh.own_goals AS team_home_own_goals
    , avh.penalties_missed AS team_home_penalties_missed
    , avh.penalties_saved AS team_home_penalties_saved
    , avh.tackles AS team_home_tackles
    , avh.recoveries AS team_home_recoveries
    , avh.clearances_blocks_interceptions AS team_home_clearances_blocks_interceptions
    , avh.yellow_cards AS team_home_yellow_cards
    , avh.red_cards AS team_home_red_cards
    , f.team_a_id
    , f.team_a_score
    , ava.expected_goals AS team_away_expected_goals
    , ava.expected_assists AS team_away_expected_assists
    , ava.expected_goals_conceded AS team_away_expected_goals_conceded
    , ava.saves AS team_away_saves
    , ava.own_goals AS team_away_own_goals
    , ava.penalties_missed AS team_away_penalties_missed
    , ava.penalties_saved AS team_away_penalties_saved
    , ava.tackles AS team_away_tackles
    , ava.recoveries AS team_away_recoveries
    , ava.clearances_blocks_interceptions AS team_away_clearances_blocks_interceptions
    , ava.yellow_cards AS team_away_yellow_cards
    , ava.red_cards AS team_away_red_cards
    , f.minutes_played
FROM {{ref("cleansed_fixtures")}} AS f
    LEFT JOIN agg_values AS avh ON avh.fixture_id = f.fixture_id AND avh.team_id = f.team_h_id
    LEFT JOIN agg_values AS ava ON ava.fixture_id = f.fixture_id AND ava.team_id = f.team_a_id
WHERE f.valid_to = '9999-12-31 00:00:00.000+00:00'