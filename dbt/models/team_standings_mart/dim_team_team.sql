{{ config(
    alias='dim_team',
    schema='team_standings_mart'
) }}

SELECT
    t.team_id,
    t.team_name,
    t.common_name
FROM
    {{ source('core', 'teams') }} t
