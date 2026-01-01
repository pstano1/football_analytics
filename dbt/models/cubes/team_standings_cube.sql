{{ config(
    materialized='table'
) }}

SELECT
    t.team_id,
    tls.season_id,
    tls.league_id,
    m.date,
    COUNT(*) AS matches_played,
    SUM(
        CASE
            WHEN t.team_id = m.home_team_id AND m.home_team_goals > m.away_team_goals THEN 1
            WHEN t.team_id = m.away_team_id AND m.away_team_goals > m.home_team_goals THEN 1
            ELSE 0
        END
    ) AS wins,
    SUM(
        CASE
            WHEN t.team_id = m.home_team_id AND m.home_team_goals < m.away_team_goals THEN 1
            WHEN t.team_id = m.away_team_id AND m.away_team_goals < m.home_team_goals THEN 1
            ELSE 0
        END
    ) AS losses,
    SUM(
        CASE WHEN m.home_team_goals = m.away_team_goals THEN 1 ELSE 0 END
    ) AS draws,
    SUM(
        CASE
            WHEN t.team_id = m.home_team_id AND m.home_team_goals > m.away_team_goals THEN 3
            WHEN t.team_id = m.away_team_id AND m.away_team_goals > m.home_team_goals THEN 3
            WHEN m.home_team_goals = m.away_team_goals THEN 1
            ELSE 0
        END
    ) AS points,
    SUM(
        CASE
            WHEN t.team_id = m.home_team_id THEN m.home_team_goals
            WHEN t.team_id = m.away_team_id THEN m.away_team_goals
        END
    ) AS goals,
    SUM(
        CASE
            WHEN t.team_id = m.home_team_id THEN m.away_team_goals
            WHEN t.team_id = m.away_team_id THEN m.home_team_goals
        END
    ) AS goals_conceded
FROM 
    {{ source('core', 'teams') }} t
LEFT JOIN {{ source('core', 'matches') }} m
    ON t.team_id = m.home_team_id OR t.team_id = m.away_team_id
LEFT JOIN {{ source('core', 'teams_leagues_seasons') }} tls
    ON tls.team_id = t.team_id AND tls.season_id = m.season_id
GROUP BY
    t.team_id,
    tls.season_id,
    tls.league_id,
    m.date
ORDER BY
    2,  -- season_id
    3,  -- league_id
    8 DESC,  -- points
    (9 - 10) DESC  -- goals - goals_conceded

