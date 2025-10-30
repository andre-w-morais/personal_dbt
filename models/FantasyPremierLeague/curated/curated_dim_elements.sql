SELECT
    element_code
    , element_id
    , element_web_name
    , element_first_name
    , element_second_name
    , birth_date
    , DATE_DIFF(CAST(extracted_at AS DATE), birth_date, YEAR) as element_age
    , opta_code
    , region
    , team_join_date
    , status
    , valid_from
    , valid_to
FROM {{ref('cleansed_elements')}}