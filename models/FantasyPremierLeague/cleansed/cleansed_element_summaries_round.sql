with deduplication as (
  SELECT
    element
    , round
    , fixture
    , was_home
    , opponent_team
    , team_h_score
    , team_a_score
    , kickoff_time AS kickoff_at
    , total_points
    , bps
    , bonus
    , starts
    , minutes
    , goals_scored
    , expected_goals
    , assists
    , expected_assists
    , expected_goal_involvements
    , goals_conceded
    , expected_goals_conceded
    , clean_sheets
    , saves
    , own_goals
    , penalties_saved
    , penalties_missed
    , tackles
    , recoveries
    , clearances_blocks_interceptions
    , defensive_contribution
    , yellow_cards
    , red_cards
    , ict_index
    , threat
    , creativity
    , influence
    , transfers_in
    , transfers_out
    , transfers_balance
    , selected
    , value
    , modified
    , row_number() over(partition by element, round order by extraction_timestamp desc) as sort_latest_record
  FROM {{ source("fantasy_premier_league", "raw_fpl_element_summaries_round")}}
)
SELECT
    element AS element_id
    , round AS event_id
    , fixture AS fixture_id
    , was_home
    , opponent_team AS opponent_team_id
    , team_h_score
    , team_a_score
    , kickoff_time
    , total_points
    , bps
    , bonus
    , starts
    , minutes AS minutes_played
    , goals_scored
    , expected_goals
    , assists
    , expected_assists
    , expected_goal_involvements
    , goals_conceded
    , expected_goals_conceded
    , clean_sheets
    , saves
    , own_goals
    , penalties_saved
    , penalties_missed
    , tackles
    , recoveries
    , clearances_blocks_interceptions
    , defensive_contribution
    , yellow_cards
    , red_cards
    , ict_index
    , threat
    , creativity
    , influence
    , transfers_in
    , transfers_out
    , transfers_balance
    , selected
    , value
    , modified AS is_modified
FROM deduplication
WHERE sort_latest_record = 1