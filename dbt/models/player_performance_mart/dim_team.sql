SELECT
    t.team_id,
    t.team_name,
    t.common_name
FROM 
    {{ source('core', 'teams') }} t
