{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='tuple()',
    schema='team_standings_cube'
) }}

WITH home_matches AS (
    SELECT
        m.match_id AS match_id,
        m.date AS date,
        m.home_team_id AS team_id,
        m.away_team_id AS opponent_id,
        m.season_id AS season_id,
        tls.league_id AS league_id,
        
        t.team_name AS team_name,
        opp.team_name AS opponent_name,
        l.name AS league_name,
        s.name AS season_name,
        m.attendance AS attendance,
        stad.name AS stadium_name,
        ref.full_name AS referee_name,
        
        m.home_team_goals AS goals,
        m.home_team_goals_at_half_time AS goals_at_half_time,
        m.home_team_corners AS corners,
        m.home_team_yellow_cards AS yellow_cards,
        m.home_team_red_cards AS red_cards,
        m.home_team_shots_on_target AS shots_on_target,
        m.home_team_shots_off_target AS shots_off_target,
        m.home_team_fouls AS fouls,
        m.home_team_possession AS possession,
        m.home_team_xg AS xg,
        
        m.away_team_goals AS goals_conceded,
        m.away_team_goals_at_half_time AS goals_conceded_at_half_time,
        m.away_team_corners AS corners_against,
        m.away_team_xg AS xg_against,
        
        (m.home_team_goals - m.away_team_goals) AS goal_difference,
        
        CASE 
            WHEN m.home_team_goals > m.away_team_goals THEN 'W'
            WHEN m.home_team_goals < m.away_team_goals THEN 'L'
            ELSE 'D'
        END AS result,
        
        CASE 
            WHEN m.home_team_goals > m.away_team_goals THEN 3
            WHEN m.home_team_goals < m.away_team_goals THEN 0
            ELSE 1
        END AS points,
        
        CASE WHEN m.away_team_goals = 0 THEN 1 ELSE 0 END AS clean_sheet,
        CASE WHEN m.home_team_goals = 0 THEN 1 ELSE 0 END AS failed_to_score,
        
        CASE WHEN m.home_team_goals > m.away_team_goals THEN 1 ELSE 0 END AS is_win,
        CASE WHEN m.home_team_goals < m.away_team_goals THEN 1 ELSE 0 END AS is_loss,
        CASE WHEN m.home_team_goals = m.away_team_goals THEN 1 ELSE 0 END AS is_draw,
        
        'H' AS home_away
        
    FROM {{ source('core', 'matches') }} m
    INNER JOIN {{ source('core', 'teams_leagues_seasons') }} tls
        ON tls.team_id = m.home_team_id 
        AND tls.season_id = m.season_id
    INNER JOIN {{ source('core', 'teams') }} t
        ON t.team_id = m.home_team_id
    INNER JOIN {{ source('core', 'teams') }} opp
        ON opp.team_id = m.away_team_id
    INNER JOIN {{ source('core', 'leagues') }} l
        ON l.league_id = tls.league_id
    INNER JOIN {{ source('core', 'seasons') }} s
        ON s.season_id = m.season_id
    LEFT JOIN {{ source('core', 'stadiums') }} stad
        ON stad.stadium_id = m.stadium_id
    LEFT JOIN {{ source('core', 'referees') }} ref
        ON ref.referee_id = m.referee_id
),

away_matches AS (
    SELECT
        m.match_id AS match_id,
        m.date AS date,
        m.away_team_id AS team_id,
        m.home_team_id AS opponent_id,
        m.season_id AS season_id,
        tls.league_id AS league_id,
        
        t.team_name AS team_name,
        opp.team_name AS opponent_name,
        l.name AS league_name,
        s.name AS season_name,
        m.attendance AS attendance,
        stad.name AS stadium_name,
        ref.full_name AS referee_name,
        
        m.away_team_goals AS goals,
        m.away_team_goals_at_half_time AS goals_at_half_time,
        m.away_team_corners AS corners,
        m.away_team_yellow_cards AS yellow_cards,
        m.away_team_red_cards AS red_cards,
        m.away_team_shots_on_target AS shots_on_target,
        m.away_team_shots_off_target AS shots_off_target,
        m.away_team_fouls AS fouls,
        m.away_team_possession AS possession,
        m.away_team_xg AS xg,
        
        m.home_team_goals AS goals_conceded,
        m.home_team_goals_at_half_time AS goals_conceded_at_half_time,
        m.home_team_corners AS corners_against,
        m.home_team_xg AS xg_against,
        
        (m.away_team_goals - m.home_team_goals) AS goal_difference,
        
        CASE 
            WHEN m.away_team_goals > m.home_team_goals THEN 'W'
            WHEN m.away_team_goals < m.home_team_goals THEN 'L'
            ELSE 'D'
        END AS result,
        
        CASE 
            WHEN m.away_team_goals > m.home_team_goals THEN 3
            WHEN m.away_team_goals < m.home_team_goals THEN 0
            ELSE 1
        END AS points,
        
        CASE WHEN m.home_team_goals = 0 THEN 1 ELSE 0 END AS clean_sheet,
        CASE WHEN m.away_team_goals = 0 THEN 1 ELSE 0 END AS failed_to_score,
        
        CASE WHEN m.away_team_goals > m.home_team_goals THEN 1 ELSE 0 END AS is_win,
        CASE WHEN m.away_team_goals < m.home_team_goals THEN 1 ELSE 0 END AS is_loss,
        CASE WHEN m.away_team_goals = m.home_team_goals THEN 1 ELSE 0 END AS is_draw,
        
        'A' AS home_away
        
    FROM {{ source('core', 'matches') }} m
    INNER JOIN {{ source('core', 'teams_leagues_seasons') }} tls
        ON tls.team_id = m.away_team_id 
        AND tls.season_id = m.season_id
    INNER JOIN {{ source('core', 'teams') }} t
        ON t.team_id = m.away_team_id
    INNER JOIN {{ source('core', 'teams') }} opp
        ON opp.team_id = m.home_team_id
    INNER JOIN {{ source('core', 'leagues') }} l
        ON l.league_id = tls.league_id
    INNER JOIN {{ source('core', 'seasons') }} s
        ON s.season_id = m.season_id
    LEFT JOIN {{ source('core', 'stadiums') }} stad
        ON stad.stadium_id = m.stadium_id
    LEFT JOIN {{ source('core', 'referees') }} ref
        ON ref.referee_id = m.referee_id
)

SELECT * FROM home_matches
UNION ALL
SELECT * FROM away_matches