With
  deduplication as (
    SELECT
      id
      , singular_name_short
      , singular_name
      , element_count
      , row_number() over (partition by id order by extraction_timestamp desc) as sort_latest_record
    FROM `raw_fpl.raw_fpl_element_types`
  )
  SELECT
    id
    , singular_name_short
    , singular_name
    , element_count
  FROM deduplication
  WHERE sort_latest_record = 1