SELECT
    m.match_id,
    m.team_id,
    m.opponent_id,
    m.season_id,
    m.league_id,
    m.home_team_goals as goals,
    m.home_team_goals_at_half_time as goals_at_half_time,
    m.home_team_corners as corners,
    m.home_team_yellow_cards as yellow_cards,
    m.home_team_red_cards as red_cards,
    m.home_team_shots_on_target as shots_on_target,
    m.home_team_shots_off_target as shots_off_target,
    m.home_team_fouls as fouls,
    m.home_team_possession as possession,
    m.home_team_xg as xg
FROM 
    {{ source('core', 'matches') }} m
