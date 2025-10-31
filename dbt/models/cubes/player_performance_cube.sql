{{ config(materialized='table') }}

SELECT 
    player_id,
    player_name,
    date_trunc('year', game_date) AS year 
FROM {{ source('player_performance_mart', 'fact_player_performance') }}
GROUP BY 1,2,3; 
