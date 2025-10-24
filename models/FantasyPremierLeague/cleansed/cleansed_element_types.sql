with
    deduplication as (
        select
            id,
            singular_name_short,
            singular_name,
            element_count,
            row_number() over (
                partition by id order by extraction_timestamp desc
            ) as sort_latest_record
        from {{ source('fantasy_premier_league', 'raw_fpl_element_types') }}
    )
select 
    id
    , singular_name_short
    , singular_name
    , element_count
from deduplication
where sort_latest_record = 1
