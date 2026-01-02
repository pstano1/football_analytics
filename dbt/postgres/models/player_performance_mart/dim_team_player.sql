{{ config(
    alias='dim_team',
    schema='player_performance_mart'
) }}

SELECT
    t.team_id,
    t.team_name,
    t.common_name
FROM 
    {{ source('core', 'teams') }} t
