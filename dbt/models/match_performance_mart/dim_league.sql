SELECT
    l.league_id,
    l.name
FROM 
    {{ source('core', 'leagues') }} l
