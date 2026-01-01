{{ config(
    alias='dim_match',
    schema='match_performance_mart'
) }}

SELECT
    m.match_id,
    m.attendance,
    s.name as stadium,
    r.full_name as referee,
    m.date
FROM 
    {{ source('core', 'matches') }} m
LEFT JOIN {{ source('core', 'stadiums') }} s
    ON m.stadium_id = s.stadium_id
LEFT JOIN {{ source('core', 'referees') }} r
    ON m.referee_id = r.referee_id
