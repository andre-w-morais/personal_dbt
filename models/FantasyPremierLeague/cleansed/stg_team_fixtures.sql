WITH 
    fixtures_scan as (
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
            , team_h_id
            , team_h_difficulty
            , team_a_id
            , team_a_difficulty
            , team_h_score
            , team_a_score
            , minutes_played
            , valid_from
            , valid_to
        FROM {{ ref("stg_fixtures")}}
    )
    SELECT
        fs.fixture_id
        , fs.fixture_code
        , fs.kickoff_at
        , fs.provisional_start_time
        , fs.is_started
        , fs.is_finished
        , fs.is_provisionally_finished
        , fs.event_id
        , fs.pulse_id
        , fs.team_h_id AS team_id
        , 'Home' AS home_away
        , team_h_difficulty AS team_difficulty
        , fs.team_h_score AS team_score
        , fs.team_a_id AS opponent_team_id
        , fs.team_a_difficulty AS opponent_difficulty
        , fs.team_a_score AS opponent_score
        , fs.minutes_played
        , fs.valid_from
        , fs.valid_to
    FROM fixtures_scan AS fs
    --
    UNION ALL
    --
    SELECT
        fs.fixture_id
        , fs.fixture_code
        , fs.kickoff_at
        , fs.provisional_start_time
        , fs.is_started
        , fs.is_finished
        , fs.is_provisionally_finished
        , fs.event_id
        , fs.pulse_id
        , fs.team_a_id AS team_id
        , 'Away' AS home_away
        , fs.team_a_difficulty AS team_difficulty
        , fs.team_a_score AS team_score
        , fs.team_h_id AS opponent_team_id
        , fs.team_h_difficulty AS opponent_difficulty
        , fs.team_h_score AS opponent_score
        , fs.minutes_played
        , fs.valid_from
        , fs.valid_to
    FROM fixtures_scan AS fs