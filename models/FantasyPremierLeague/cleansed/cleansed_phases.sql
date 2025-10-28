with
    deduplication as (
        select
            id
            , highest_score
            , start_event
            , stop_event
            , name
            , row_number() over (
                partition by id order by extraction_timestamp desc
            ) as sort_latest_record
        from {{ source("fantasy_premier_league", "raw_fpl_phases") }}
    )
select 
    id
    , highest_score
    , start_event
    , stop_event
    , name
from deduplication
where sort_latest_record = 1