SELECT 
    t.team_id,
    t.team_name
FROM 
    {{ source('core', 'teams') }} t
