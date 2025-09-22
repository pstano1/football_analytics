SELECT
    s.season_id,
    s.name
FROM {{ source('core', 'seasons') }} s 
