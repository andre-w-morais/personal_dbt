SELECT
    element_type_id
    , short_element_type_name
    , element_type_name
    , element_type_count
FROM {{ref("cleansed_element_types")}}