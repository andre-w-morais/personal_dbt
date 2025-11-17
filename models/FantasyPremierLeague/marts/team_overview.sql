SELECT
    ff.fixture_id
    , ff.fixture_code
    , df.kickoff_at
    , df.is_started AS is_fixture_started
    , df.is_finished AS is_fixture_finished
    , ff.event_id
    , de.event_name
    , de.deadline_at AS event_deadline_at
    , de.is_current AS is_current_event
    , de.is_previous AS is_previous_event
    , de.is_next AS is_next_event
    , de.is_finished AS is_event_finished
    , ff.team_h_id
    , dth.short_team_name AS home_team
    , dth.strength AS home_team_strength
    , dth.strength_overall_home AS home_team_strength_overall_home
    , dth.strength_attack_home AS home_team_strength_attack_home
    , dth.strength_defence_home AS home_team_strength_defence_home
    , dth.position AS home_team_position
    , ff.team_h_score
    , ff.team_a_id
    , dta.short_team_name AS away_team
    , dta.strength AS away_team_strength
    , dta.strength_overall_away AS away_team_strength_overall_away
    , dta.strength_attack_away AS away_team_strength_attack_away
    , dta.strength_defence_away AS away_team_strength_defence_away
    , dta.position AS away_team_position
    , ff.team_a_score
FROM {{ ref("curated_fact_fixtures") }} AS ff
    LEFT JOIN {{ ref("curated_dim_fixtures") }} AS df ON df.fixture_code = ff.fixture_code AND (CURRENT_TIMESTAMP() BETWEEN df.valid_from AND df.valid_to)
    LEFT JOIN {{ ref("curated_dim_events") }} AS de ON de.event_id = ff.event_id AND (df.kickoff_at BETWEEN de.valid_from AND de.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dth ON dth.team_id = ff.team_h_id AND (df.kickoff_at BETWEEN dth.valid_from AND dth.valid_to)
    LEFT JOIN {{ ref("curated_dim_teams") }} AS dta ON dta.team_id = ff.team_a_id AND (df.kickoff_at BETWEEN dta.valid_from AND dta.valid_to)