SELECT
    code --PRIMARY KEY
    , extraction_timestamp --PRIMARY KEY
    , element_type --FOREIGN KEY
    , id --PRIMARY KEY?
    , team_code --FOREIGN KEY
    , team --FOREIGN KEY
    , can_select
    , now_cost
    , now_cost_rank
    , now_cost_rank_type
    , selected_by_percent
    , selected_rank
    , selected_rank_type
    , transfers_out_event
    , transfers_in_event
    , transfers_in_event - transfers_out_event as transfer_balance_event
    , transfers_in
    , transfers_out
    , transfers_in - transfers_out as transfer_balance
    , extraction_timestamp as extracted_at
FROM {{ ref('cleansed_general_elements') }}