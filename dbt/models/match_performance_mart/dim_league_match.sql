{{ config(
    alias='dim_league',
    schema='match_performance_mart'
) }}

SELECT
    l.league_id,
    l.name
FROM 
    {{ source('core', 'leagues') }} l
