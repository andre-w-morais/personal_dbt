with deduplication as (
  SELECT
    element_id
    , element_code
    , season_name
    , start_cost
    , end_cost
    , starts
    , minutes
    , total_points
    , goals_scored
    , expected_goals
    , assists
    , expected_assists
    , expected_goal_involvements
    , clean_sheets
    , goals_conceded
    , expected_goals_conceded
    , saves
    , tackles
    , recoveries
    , clearances_blocks_interceptions
    , defensive_contribution
    , penalties_missed
    , penalties_saved
    , own_goals
    , bps
    , bonus
    , yellow_cards
    , red_cards
    , ict_index
    , threat
    , creativity
    , influence
    , row_number() over(partition by element_id, season_name order by extraction_timestamp desc) as sort_latest_record
  FROM {{ source("fantasy_premier_league", "raw_fpl_element_summaries_past_season")}}
)
SELECT
    element_id
    , element_code
    , season_name
    , start_cost
    , end_cost
    , starts
    , minutes
    , total_points
    , goals_scored
    , expected_goals
    , assists
    , expected_assists
    , expected_goal_involvements
    , clean_sheets
    , goals_conceded
    , expected_goals_conceded
    , saves
    , tackles
    , recoveries
    , clearances_blocks_interceptions
    , defensive_contribution
    , penalties_missed
    , penalties_saved
    , own_goals
    , bps
    , bonus
    , yellow_cards
    , red_cards
    , ict_index
    , threat
    , creativity
    , influence
FROM deduplication
WHERE sort_latest_record = 1