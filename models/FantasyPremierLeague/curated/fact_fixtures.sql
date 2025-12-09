SELECT
    f.fixture_id
    , f.fixture_code
    , f.event_id
    , f.pulse_id
    , f.team_h_id
    , f.team_h_score
    , fash.expected_goals AS team_home_expected_goals
    , fash.expected_assists AS team_home_expected_assists
    , fash.expected_goals_conceded AS team_home_expected_goals_conceded
    , fash.saves AS team_home_saves
    , fash.own_goals AS team_home_own_goals
    , fash.penalties_missed AS team_home_penalties_missed
    , fash.penalties_saved AS team_home_penalties_saved
    , fash.tackles AS team_home_tackles
    , fash.recoveries AS team_home_recoveries
    , fash.clearances_blocks_interceptions AS team_home_clearances_blocks_interceptions
    , fash.yellow_cards AS team_home_yellow_cards
    , fash.red_cards AS team_home_red_cards
    , f.team_a_id
    , f.team_a_score
    , fasa.expected_goals AS team_away_expected_goals
    , fasa.expected_assists AS team_away_expected_assists
    , fasa.expected_goals_conceded AS team_away_expected_goals_conceded
    , fasa.saves AS team_away_saves
    , fasa.own_goals AS team_away_own_goals
    , fasa.penalties_missed AS team_away_penalties_missed
    , fasa.penalties_saved AS team_away_penalties_saved
    , fasa.tackles AS team_away_tackles
    , fasa.recoveries AS team_away_recoveries
    , fasa.clearances_blocks_interceptions AS team_away_clearances_blocks_interceptions
    , fasa.yellow_cards AS team_away_yellow_cards
    , fasa.red_cards AS team_away_red_cards
    , f.minutes_played
FROM {{ref("cleansed_fixtures")}} AS f
    LEFT JOIN {{ref("int_fixture_agg_stats")}} AS fash ON fash.fixture_id = f.fixture_id AND fash.team_id = f.team_h_id
    LEFT JOIN {{ref("int_fixture_agg_stats")}} AS fasa ON fasa.fixture_id = f.fixture_id AND fasa.team_id = f.team_a_id
WHERE f.valid_to = '9999-12-31 00:00:00.000+00:00'