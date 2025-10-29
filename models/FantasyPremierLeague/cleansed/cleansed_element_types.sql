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
    id AS element_type_id
    , singular_name_short AS short_element_type_name
    , singular_name AS element_type_name
    , element_count AS element_type_count
from deduplication
where sort_latest_record = 1
