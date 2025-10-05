{{ config(
    alias='dim_team',
    schema='match_performance_mart'
) }}

SELECT 
    t.team_id,
    t.team_name
FROM 
    {{ source('core', 'teams') }} t
