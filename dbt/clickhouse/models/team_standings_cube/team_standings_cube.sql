{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(season_id, league_id, team_id, date)',
    schema='team_standings_cube'
) }}

WITH match_results AS (
    SELECT
        t.team_id AS team_id,
        tls.season_id AS season_id,
        tls.league_id AS league_id,
        m.date AS date,
        CASE
            WHEN t.team_id = m.home_team_id THEN m.home_team_goals
            WHEN t.team_id = m.away_team_id THEN m.away_team_goals
            ELSE 0
        END AS goals_for,
        CASE
            WHEN t.team_id = m.home_team_id THEN m.away_team_goals
            WHEN t.team_id = m.away_team_id THEN m.home_team_goals
            ELSE 0
        END AS goals_against,
        CASE
            WHEN (t.team_id = m.home_team_id AND m.home_team_goals > m.away_team_goals) OR
                 (t.team_id = m.away_team_id AND m.away_team_goals > m.home_team_goals) THEN 1
            ELSE 0
        END AS is_win,
        CASE
            WHEN (t.team_id = m.home_team_id AND m.home_team_goals < m.away_team_goals) OR
                 (t.team_id = m.away_team_id AND m.away_team_goals < m.home_team_goals) THEN 1
            ELSE 0
        END AS is_loss,
        CASE
            WHEN m.home_team_goals = m.away_team_goals THEN 1
            ELSE 0
        END AS is_draw
    FROM {{ source('core', 'teams') }} t
    INNER JOIN {{ source('core', 'matches') }} m
        ON t.team_id = m.home_team_id OR t.team_id = m.away_team_id
    INNER JOIN {{ source('core', 'teams_leagues_seasons') }} tls
        ON tls.team_id = t.team_id AND tls.season_id = m.season_id
)

SELECT
    team_id,
    season_id,
    league_id,
    date,
    count() AS matches_played,
    sum(is_win) AS wins,
    sum(is_loss) AS losses,
    sum(is_draw) AS draws,
    sum(is_win * 3 + is_draw * 1) AS points,
    sum(goals_for) AS goals,
    sum(goals_against) AS goals_conceded,
    sum(goals_for - goals_against) AS goal_difference
FROM match_results
GROUP BY
    team_id,
    season_id,
    league_id,
    date
ORDER BY
    season_id,
    league_id,
    points DESC,
    goal_difference DESC