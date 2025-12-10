SELECT
    tf.fixture_id
    , tf.fixture_code
    , df.home_away
    , df.is_started
    , df.is_finished
    , tf.kickoff_at
    , tf.event_id
    , de.event_name
    , de.deadline_at
    , de.is_current
    , de.is_previous
    , de.is_next
    , de.is_finished AS is_event_finished
    , tf.team_id
    , dt.short_team_name AS team
    , dt.strength AS team_strength
    , dt.position AS team_league_position
    , tf.team_score
    , tf.team_score_home_away_5game_form
    , tf.team_score_home_away_5game_form_rank
    , tf.team_score_5game_form
    , tf.team_score_5game_form_rank
    , tf.team_score_10game_form
    , tf.team_score_10game_form_rank
    , tf.team_expected_goals
    , tf.team_expected_goals_home_away_5game_form
    , tf.team_expected_goals_home_away_5game_form_rank
    , tf.team_expected_goals_5game_form
    , tf.team_expected_goals_5game_form_rank
    , tf.team_expected_goals_10game_form
    , tf.team_expected_goals_10game_form_rank
    , tf.team_expected_assists
    , tf.team_expected_assists_home_away_5game_form
    , tf.team_expected_assists_5game_form
    , tf.team_expected_assists_10game_form
    , tf.team_goals_conceded
    , tf.team_goals_conceded_home_away_5game_form
    , tf.team_goals_conceded_home_away_5game_form_rank
    , tf.team_goals_conceded_5game_form
    , tf.team_goals_conceded_5game_form_rank
    , tf.team_goals_conceded_10game_form  
    , tf.team_goals_conceded_10game_form_rank
    , tf.team_expected_goals_conceded
    , tf.team_expected_goals_conceded_home_away_5game_form
    , tf.team_expected_goals_conceded_home_away_5game_form_rank
    , tf.team_expected_goals_conceded_5game_form
    , tf.team_expected_goals_conceded_5game_form_rank
    , tf.team_expected_goals_conceded_10game_form  
    , tf.team_expected_goals_conceded_10game_form_rank
    , tf.yellow_cards
    , tf.team_yellow_cards_home_away_5game_form
    , tf.team_yellow_cards_5game_form
    , tf.team_yellow_cards_10game_form  
    , tf.red_cards
    , tf.team_red_cards_home_away_5game_form
    , tf.team_red_cards_5game_form
    , tf.team_red_cards_10game_form  
    , tf.opponent_team_id
    , do.short_team_name AS opponent
    , do.strength AS opponent_strength
    , do.position AS opponent_league_position
    , tf.minutes_played
FROM {{ ref("fact_team_fixtures") }} tf 
    LEFT JOIN {{ ref("dim_team_fixtures") }} df ON df.fixture_id = tf.fixture_id AND (tf.kickoff_at >= df.valid_from AND tf.kickoff_at < df.valid_to)
    LEFT JOIN {{ ref("dim_events") }} de ON de.event_id = tf.event_id AND (tf.kickoff_at >= de.valid_from AND tf.kickoff_at < de.valid_to)
    LEFT JOIN {{ ref("dim_teams") }} dt ON dt.team_id = tf.team_id AND (tf.kickoff_at >= dt.valid_from AND tf.kickoff_at < dt.valid_to)
    LEFT JOIN {{ ref("dim_teams") }} do ON do.team_id = tf.team_id AND (tf.kickoff_at >= do.valid_from AND tf.kickoff_at < do.valid_to)