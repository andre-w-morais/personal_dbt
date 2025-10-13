SELECT
    id
    , singular_name_short as short_type_name
    , singular_name as type_name
    , element_count
FROM {{ref("cleansed_general_element_types")}}