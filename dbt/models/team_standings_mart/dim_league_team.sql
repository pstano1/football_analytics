{{ config(
    alias='dim_league',
    schema='team_standings_mart'
) }}

SELECT
    l.league_id,
    l.name,
    l.tier
FROM
    {{ source('core', 'leagues') }} l
