{{ config(
    alias='dim_season',
    schema='team_standings_mart'
) }}

SELECT
    s.season_id,
    s.name
FROM 
    {{ source('core', 'seasons') }} s
