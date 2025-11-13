SELECT
    element_code
    , element_id
    , element_web_name
    , element_first_name
    , element_second_name
    , birth_date
    , DATE_DIFF(CAST(valid_from AS DATE), birth_date, YEAR) as element_age
    , opta_code
    , region
    , team_join_date
    , CASE 
        WHEN status = 'a' THEN 'Available'
        WHEN status = 'u' THEN 'Unvailable'
        WHEN status = 'i' THEN 'Injured'
        WHEN status = 'd' THEN 'Doubt'
        WHEN status = 'n' THEN 'Ineligible to face parent club'
        WHEN status = 's' THEN 'Suspended'
        END as status
    , news
    , valid_from
    , valid_to
FROM {{ref('cleansed_elements')}}