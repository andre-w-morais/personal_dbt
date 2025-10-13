SELECT
    code
    , id
    , event
    , kickoff_time
    , started
    , finished
    , finished_provisional
FROM {{ref("cleansed_fixtures")}}