SELECT
    element_type_id
    , short_element_type_name
    , element_type_name
    , element_type_count
    , valid_from
    , valid_to
FROM {{ref("stg_element_types")}}