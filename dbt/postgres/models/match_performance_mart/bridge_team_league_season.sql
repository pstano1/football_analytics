{{ config(
    alias='bridge_team_league_season',
    schema='match_performance_mart'
) }}

SELECT 
    tls.team_id,
    tls.league_id,
    tls.season_id
FROM 
    {{ source('core', 'teams_leagues_seasons') }} tls
