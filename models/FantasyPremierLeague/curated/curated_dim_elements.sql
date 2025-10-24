SELECT
    code
    , id
    , web_name
    , first_name
    , second_name
    , birth_date
    , DATE_DIFF(CAST(extraction_timestamp AS DATE), birth_date, YEAR) as age
    , opta_code
    , region
    , team_join_date
    , status
    , extraction_timestamp as valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN NULL
        ELSE LEAD(extraction_timestamp) OVER(PARTITION BY code ORDER BY extraction_timestamp ASC)
        end AS valid_to
FROM {{ref('cleansed_general_elements')}}
order by code, extraction_timestamp ASC
