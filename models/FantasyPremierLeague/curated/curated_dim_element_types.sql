SELECT
    element_type_id
    , singular_name_short as short_element_type_name
    , singular_name as element_type_name
    , element_type_count
FROM {{ref("cleansed_element_types")}}