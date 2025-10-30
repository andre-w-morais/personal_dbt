WITH
    ordering AS (
        SELECT
            id
            , singular_name_short
            , singular_name
            , element_count
            , extraction_timestamp
            , ROW_NUMBER() OVER (PARTITION BY id ORDER BY extraction_timestamp DESC) AS sort_latest_record
        FROM {{ source("fantasy_premier_league", "raw_fpl_element_types") }}
    )
SELECT 
    id AS element_type_id
    , singular_name_short AS short_element_type_name
    , singular_name AS element_type_name
    , element_count AS element_type_count
    , CAST(extraction_timestamp AS DATE) AS valid_from
    , CASE 
        WHEN sort_latest_record = 1 THEN CAST('9999-12-31' AS DATE)
        ELSE LAG(CAST(extraction_timestamp AS DATE)) OVER(PARTITION BY id ORDER BY extraction_timestamp DESC)
        END AS valid_to
FROM ordering