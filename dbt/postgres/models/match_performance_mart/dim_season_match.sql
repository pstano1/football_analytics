{{ config(
    alias='dim_season',
    schema='match_performance_mart'
) }}

SELECT
    s.season_id,
    s.name
FROM {{ source('core', 'seasons') }} s 
