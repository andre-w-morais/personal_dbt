SELECT
    ff.team_h_id AS team_id
    , dt.short_team_name AS team
    , dt.strength AS team_strength
    , dt.strength_overall_home AS team_strength_overall
    , dt.strength_attack_home AS team_strength_attack
    , dt.strength_defence_home AS team_strength_defence
    , dt.position AS team_position
    , ff.event_id 
    , de.event_name
    , de.deadline_at AS event_deadline_at
    , de.is_current AS is_current_event
    , de.is_previous AS is_previous_event
    , de.is_next AS is_next_event
    , de.is_finished AS is_event_finished
    , ff.fixture_id
    , ff.fixture_code
    , df.kickoff_at
    , df.is_started AS is_fixture_started
    , df.is_finished AS is_fixture_finished
    , ff.team_h_score AS score
    , ff.team_home_expected_goals AS expected_goals
    , ff.team_home_expected_assists AS expected_assists
    , ff.team_home_expected_goals_conceded AS expected_goals_conceded
    , ff.team_home_saves AS saves
    , ff.team_home_own_goals AS own_goals
    , ff.team_home_penalties_missed AS penalties_missed
    , ff.team_home_penalties_saved AS penalties_saved
    , ff.team_home_tackles AS tackles
    , ff.team_home_recoveries AS recoveries
    , ff.team_home_clearances_blocks_interceptions AS clearances_blocks_interceptions
    , ff.team_home_yellow_cards AS yellow_cards
    , ff.team_home_red_cards AS red_cards
    , 'home' AS home_away
    , ff.team_a_id AS opponent_team_id
    , dto.short_team_name AS opponent_team_name
    , ff.team_a_score AS opponent_score
    , ff.team_away_expected_goals AS opponent_expected_goals
    , ff.team_away_expected_assists AS opponent_expected_assists
    , ff.team_away_expected_goals_conceded AS opponent_expected_goals_conceded
    , ff.team_away_saves AS opponent_saves
    , ff.team_away_own_goals AS opponent_own_goals
    , ff.team_away_penalties_missed AS opponent_penalties_missed
    , ff.team_away_penalties_saved AS opponent_penalties_saved
    , ff.team_away_tackles AS opponent_tackles
    , ff.team_away_recoveries AS opponent_recoveries
    , ff.team_away_clearances_blocks_interceptions AS opponent_clearances_blocks_interceptions
    , ff.team_away_yellow_cards AS opponent_yellow_cards
    , ff.team_away_red_cards AS opponent_red_cards
    , dto.strength AS opponent_team_strength
    , dto.strength_overall_away AS opponent_team_strength_overall
    , dto.strength_attack_away AS opponent_team_strength_attack
    , dto.strength_defence_away AS opponent_team_strength_defence
    , dto.position AS opponent_team_position
FROM {{ ref("curated_fact_fixtures") }} AS ff
    LEFT JOIN {{ ref("curated_dim_fixtures") }} AS df ON df.fixture_code = ff.fixture_code AND (CURRENT_TIMESTAMP() BETWEEN df.valid_from AND df.valid_to)
    LEFT JOIN {{ ref("curated_dim_events") }} AS de ON de.event_id = ff.event_id AND (df.kickoff_at BETWEEN de.valid_from AND de.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dt ON dt.team_id = ff.team_h_id AND (df.kickoff_at BETWEEN dt.valid_from AND dt.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dto ON dto.team_id = ff.team_a_id AND (df.kickoff_at BETWEEN dto.valid_from AND dto.valid_to)
--
UNION ALL
--
SELECT
    ff.team_a_id AS team_id
    , dt.short_team_name AS team
    , dt.strength AS team_strength
    , dt.strength_overall_away AS team_strength_overall
    , dt.strength_attack_away AS team_strength_attack
    , dt.strength_defence_away AS team_strength_defence
    , dt.position AS team_position
    , ff.event_id 
    , de.event_name
    , de.deadline_at AS event_deadline_at
    , de.is_current AS is_current_event
    , de.is_previous AS is_previous_event
    , de.is_next AS is_next_event
    , de.is_finished AS is_event_finished
    , ff.fixture_id
    , ff.fixture_code
    , df.kickoff_at
    , df.is_started AS is_fixture_started
    , df.is_finished AS is_fixture_finished
    , ff.team_a_score AS score
    , ff.team_away_expected_goals AS expected_goals
    , ff.team_away_expected_assists AS expected_assists
    , ff.team_away_expected_goals_conceded AS expected_goals_conceded
    , ff.team_away_saves AS saves
    , ff.team_away_own_goals AS own_goals
    , ff.team_away_penalties_missed AS penalties_missed
    , ff.team_away_penalties_saved AS penalties_saved
    , ff.team_away_tackles AS tackles
    , ff.team_away_recoveries AS recoveries
    , ff.team_away_clearances_blocks_interceptions AS clearances_blocks_interceptions
    , ff.team_away_yellow_cards AS yellow_cards
    , ff.team_away_red_cards AS red_cards
    , 'away' AS home_away
    , ff.team_h_id AS opponent_team_id
    , dto.short_team_name AS opponent_team_name
    , ff.team_h_score AS opponent_score
    , ff.team_home_expected_goals AS opponent_expected_goals
    , ff.team_home_expected_assists AS opponent_expected_assists
    , ff.team_home_expected_goals_conceded AS opponent_expected_goals_conceded
    , ff.team_home_saves AS opponent_saves
    , ff.team_home_own_goals AS opponent_own_goals
    , ff.team_home_penalties_missed AS opponent_penalties_missed
    , ff.team_home_penalties_saved AS opponent_penalties_saved
    , ff.team_home_tackles AS opponent_tackles
    , ff.team_home_recoveries AS opponent_recoveries
    , ff.team_home_clearances_blocks_interceptions AS opponent_clearances_blocks_interceptions
    , ff.team_home_yellow_cards AS opponent_yellow_cards
    , ff.team_home_red_cards AS opponent_red_cards
    , dto.strength AS opponent_team_strength
    , dto.strength_overall_home AS opponent_team_strength_overall
    , dto.strength_attack_home AS opponent_team_strength_attack
    , dto.strength_defence_home AS opponent_team_strength_defence
    , dto.position AS opponent_team_position
FROM {{ ref("curated_fact_fixtures") }} AS ff
    LEFT JOIN {{ ref("curated_dim_fixtures") }} AS df ON df.fixture_code = ff.fixture_code AND (CURRENT_TIMESTAMP() BETWEEN df.valid_from AND df.valid_to)
    LEFT JOIN {{ ref("curated_dim_events") }} AS de ON de.event_id = ff.event_id AND (df.kickoff_at BETWEEN de.valid_from AND de.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dt ON dt.team_id = ff.team_a_id AND (df.kickoff_at BETWEEN dt.valid_from AND dt.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dto ON dto.team_id = ff.team_h_id AND (df.kickoff_at BETWEEN dto.valid_from AND dto.valid_to)