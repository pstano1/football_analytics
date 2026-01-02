{{ config(
    alias='dim_player',
    schema='player_performance_mart'
) }}

SELECT
    p.player_id,
    p.full_name,
    p.birthday,
    p.primary_position,
    p.nationality
FROM
    {{ source('core', 'players') }} p
