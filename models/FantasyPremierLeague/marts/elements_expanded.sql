SELECT DISTINCT
    de.element_id
    , CONCAT(de.element_first_name, " ", de.element_second_name) AS element_full_name
    , de.valid_from
    , de.valid_to
    , dt.team_id
    , dt.short_team_name
    , det.element_type_id
    , det.short_element_type_name
FROM {{ref("curated_fact_element_summaries_rounds")}} as fesr
    LEFT JOIN {{ref("curated_dim_elements")}} as de ON fesr.element_id = de.element_id AND fesr.kickoff_at BETWEEN de.valid_from AND de.valid_to
    LEFT JOIN {{ref("curated_dim_teams")}} as dt ON dt.team_id = fesr.team_id AND fesr.kickoff_at BETWEEN dt.valid_from AND dt.valid_to
    LEFT JOIN {{ref("curated_dim_element_types")}} as det ON fesr.element_type_id = det.element_type_id AND fesr.kickoff_at BETWEEN det.valid_from AND det.valid_to