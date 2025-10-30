WITH
    ordering AS (
        SELECT
        id
        , code
        , has_temporary_code
        , opta_code
        , element_type
        , web_name
        , first_name
        , second_name
        , squad_number
        , birth_date
        , region
        , team
        , team_code
        , team_join_date
        , can_select
        , can_transact
        , removed
        , corners_and_indirect_freekicks_order
        , corners_and_indirect_freekicks_text
        , penalties_order
        , penalties_text
        , direct_freekicks_order
        , direct_freekicks_text
        , status
        , news
        , news_added
        , now_cost
        , now_cost_rank
        , now_cost_rank_type
        , cost_change_start
        , cost_change_start_fall
        , cost_change_event
        , cost_change_event_fall
        , selected_by_percent
        , selected_rank
        , selected_rank_type
        , transfers_out_event
        , transfers_out
        , transfers_in_event
        , transfers_in
        , ict_index
        , ict_index_rank
        , ict_index_rank_type
        , threat
        , threat_rank
        , threat_rank_type
        , creativity
        , creativity_rank
        , creativity_rank_type
        , influence
        , influence_rank
        , influence_rank_type
        , minutes
        , starts
        , starts_per_90
        , total_points
        , points_per_game
        , points_per_game_rank
        , points_per_game_rank_type
        , value_season
        , event_points
        , form
        , form_rank
        , form_rank_type
        , value_form
        , goals_scored
        , expected_goals
        , expected_goals_per_90
        , assists
        , expected_assists
        , expected_assists_per_90
        , expected_goal_involvements
        , expected_goal_involvements_per_90
        , clean_sheets
        , clean_sheets_per_90
        , goals_conceded
        , goals_conceded_per_90
        , expected_goals_conceded
        , expected_goals_conceded_per_90
        , defensive_contribution
        , defensive_contribution_per_90
        , clearances_blocks_interceptions
        , tackles
        , recoveries
        , saves
        , saves_per_90
        , penalties_saved
        , penalties_missed
        , own_goals
        , yellow_cards
        , red_cards
        , bps
        , bonus
        , chance_of_playing_this_round
        , chance_of_playing_next_round
        , ep_this
        , ep_next
        , in_dreamteam
        , dreamteam_count
        , special
        , photo
        , extraction_timestamp
        , extraction_source
        , extraction_method
        , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
        FROM {{ source("fantasy_premier_league", "raw_fpl_elements") }}
    )
SELECT
    id AS element_id
    , code AS element_code
    , has_temporary_code
    , opta_code
    , element_type AS element_type_id
    , web_name AS element_web_name
    , first_name AS element_first_name
    , second_name AS element_second_name
    , squad_number
    , birth_date
    , region
    , team AS team_id
    , team_code
    , team_join_date
    , can_select
    , can_transact
    , removed AS is_removed
    , corners_and_indirect_freekicks_order
    , corners_and_indirect_freekicks_text
    , penalties_order
    , penalties_text
    , direct_freekicks_order
    , direct_freekicks_text
    , status
    , news
    , news_added
    , now_cost
    , now_cost_rank
    , now_cost_rank_type
    , cost_change_start
    , cost_change_start_fall
    , cost_change_event
    , cost_change_event_fall
    , selected_by_percent
    , selected_rank
    , selected_rank_type
    , transfers_out_event
    , transfers_out
    , transfers_in_event
    , transfers_in
    , ict_index
    , ict_index_rank
    , ict_index_rank_type
    , threat
    , threat_rank
    , threat_rank_type
    , creativity
    , creativity_rank
    , creativity_rank_type
    , influence
    , influence_rank
    , influence_rank_type
    , minutes AS minutes_played
    , starts
    , starts_per_90
    , total_points
    , points_per_game
    , points_per_game_rank
    , points_per_game_rank_type
    , value_season
    , event_points
    , form
    , form_rank
    , form_rank_type
    , value_form
    , goals_scored
    , expected_goals
    , expected_goals_per_90
    , assists
    , expected_assists
    , expected_assists_per_90
    , expected_goal_involvements
    , expected_goal_involvements_per_90
    , clean_sheets
    , clean_sheets_per_90
    , goals_conceded
    , goals_conceded_per_90
    , expected_goals_conceded
    , expected_goals_conceded_per_90
    , defensive_contribution
    , defensive_contribution_per_90
    , clearances_blocks_interceptions
    , tackles
    , recoveries
    , saves
    , saves_per_90
    , penalties_saved
    , penalties_missed
    , own_goals
    , yellow_cards
    , red_cards
    , bps
    , bonus
    , chance_of_playing_this_round
    , chance_of_playing_next_round
    , ep_this
    , ep_next
    , in_dreamteam
    , dreamteam_count
    , special
    , photo
    , row_number() over(partition by id order by extraction_timestamp desc) as sort_latest_record
    , CAST(extraction_timestamp AS DATE) AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31' AS DATE)
        ELSE LAG(CAST(extraction_timestamp AS DATE)) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
FROM ordering