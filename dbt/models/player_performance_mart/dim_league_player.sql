{{ config(
    alias='dim_league',
    schema='player_performance_mart'
) }}

SELECT
    l.league_id,
    l.name,
    l.tier
FROM 
    {{ source('core', 'leagues') }} l
