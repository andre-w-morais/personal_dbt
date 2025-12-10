WITH
    team_form AS (
        SELECT
            tf.fixture_id
            , tf.fixture_code
            , tf.kickoff_at
            , tf.provisional_start_time
            , tf.is_started
            , tf.is_finished
            , tf.is_provisionally_finished
            , tf.event_id
            , tf.pulse_id
            , tf.home_away
            , tf.team_id
            , tf.team_difficulty
            , tf.team_score
            , SUM(tf.team_score) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_score_home_away_5game_form
            , SUM(tf.team_score) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_score_5game_form
            , SUM(tf.team_score) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_score_10game_form
            , fas.expected_goals AS team_expected_goals
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_goals END) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_expected_goals_home_away_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_goals END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_expected_goals_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_goals END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_expected_goals_10game_form
            , fas.expected_assists AS team_expected_assists
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_assists END) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_expected_assists_home_away_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_assists END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_expected_assists_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_assists END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_expected_assists_10game_form
            , tf.opponent_score AS team_goals_conceded
            , SUM(tf.opponent_score) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_goals_conceded_home_away_5game_form
            , SUM(tf.opponent_score) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_goals_conceded_5game_form
            , SUM(tf.opponent_score) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_goals_conceded_10game_form
            , fas.expected_goals_conceded AS team_expected_goals_conceded
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_goals_conceded END) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_expected_goals_conceded_home_away_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_goals_conceded END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_expected_goals_conceded_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.expected_goals_conceded END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_expected_goals_conceded_10game_form  
            , fas.yellow_cards
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.yellow_cards END) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_yellow_cards_home_away_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.yellow_cards END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_yellow_cards_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.yellow_cards END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_yellow_cards_10game_form  
            , fas.red_cards
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.red_cards END) OVER(PARTITION BY tf.team_id, tf.home_away ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_red_cards_home_away_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.red_cards END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS team_red_cards_5game_form
            , SUM(CASE WHEN tf.team_score IS NULL THEN NULL ELSE fas.red_cards END) OVER(PARTITION BY tf.team_id ORDER BY tf.event_id ASC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_red_cards_10game_form  
            , tf.opponent_team_id
            , tf.opponent_difficulty
            , tf.minutes_played
        FROM {{ ref("stg_team_fixtures") }} tf
            LEFT JOIN {{ref("int_fixture_agg_stats")}} AS fas ON fas.team_id = tf.team_id AND fas.fixture_id = tf.fixture_id
        WHERE tf.valid_to = '9999-12-31 00:00:00.000'
    )
SELECT
    fixture_id
    , fixture_code
    , kickoff_at
    , provisional_start_time
    , is_started
    , is_finished
    , is_provisionally_finished
    , event_id
    , pulse_id
    , home_away
    , team_id
    , team_difficulty
    , team_score
    , team_score_home_away_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_score_home_away_5game_form DESC) AS team_score_home_away_5game_form_rank
    , team_score_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_score_5game_form DESC) AS team_score_5game_form_rank
    , team_score_10game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_score_10game_form DESC) AS team_score_10game_form_rank
    , ROUND(team_expected_goals, 2) AS team_expected_goals
    , ROUND(team_expected_goals_home_away_5game_form, 2) AS team_expected_goals_home_away_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_expected_goals_home_away_5game_form DESC) AS team_expected_goals_home_away_5game_form_rank
    , ROUND(team_expected_goals_5game_form, 2) AS team_expected_goals_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_expected_goals_5game_form DESC) AS team_expected_goals_5game_form_rank
    , ROUND(team_expected_goals_10game_form, 2) AS team_expected_goals_10game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_expected_goals_10game_form DESC) AS team_expected_goals_10game_form_rank
    , ROUND(team_expected_assists, 2) AS team_expected_assists
    , ROUND(team_expected_assists_home_away_5game_form, 2) AS team_expected_assists_home_away_5game_form
    , ROUND(team_expected_assists_5game_form, 2) AS team_expected_assists_5game_form
    , ROUND(team_expected_assists_10game_form, 2) AS team_expected_assists_10game_form
    , team_goals_conceded
    , team_goals_conceded_home_away_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_goals_conceded_home_away_5game_form DESC) AS team_goals_conceded_home_away_5game_form_rank
    , team_goals_conceded_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_goals_conceded_5game_form DESC) AS team_goals_conceded_5game_form_rank
    , team_goals_conceded_10game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_goals_conceded_10game_form DESC) AS team_goals_conceded_10game_form_rank
    , ROUND(team_expected_goals_conceded, 2) AS team_expected_goals_conceded
    , ROUND(team_expected_goals_conceded_home_away_5game_form, 2) AS team_expected_goals_conceded_home_away_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_expected_goals_conceded_home_away_5game_form DESC) AS team_expected_goals_conceded_home_away_5game_form_rank
    , ROUND(team_expected_goals_conceded_5game_form, 2) AS team_expected_goals_conceded_5game_form
    , RANK() OVER(PARTITION BY event_id ORDER BY team_expected_goals_conceded_5game_form DESC) AS team_expected_goals_conceded_5game_form_rank
    , ROUND(team_expected_goals_conceded_10game_form  , 2) AS team_expected_goals_conceded_10game_form  
    , RANK() OVER(PARTITION BY event_id ORDER BY team_expected_goals_conceded_10game_form DESC) AS team_expected_goals_conceded_10game_form_rank
    , yellow_cards
    , team_yellow_cards_home_away_5game_form
    , team_yellow_cards_5game_form
    , team_yellow_cards_10game_form  
    , red_cards
    , team_red_cards_home_away_5game_form
    , team_red_cards_5game_form
    , team_red_cards_10game_form  
    , opponent_team_id
    , opponent_difficulty
    , opponent_score
    , minutes_played
FROM team_form