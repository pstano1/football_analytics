SELECT
    l.league_id,
    l.name,
    l.tier
FROM
    {{ source('core', 'leagues') }} l
