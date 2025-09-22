SELECT
    p.player_id,
    p.full_name,
    p.birthday,
    p.primary_position,
    p.nationality
FROM
    {{ source('core', 'players') }} p
