SELECT
    p.player_id,
    p.season_id,
    pt.team_id,
    tls.season_id,
    p.minutes_played,
    p.appearances,
    p.goals,
    p.assists,
    p.penalty_goals,
    p.penalty_misses,
    p.clean_sheets,
    p.conceded_goals,
    p.yellow_cards,
    p.red_cards
FROM
    {{ source('core', 'player_statistics') }} p
LEFT JOIN {{ source('core', 'player_teams') }} pt
    ON p.season_id = pt.season_id AND p.player_id = pt.player_id
LEFT JOIN {{ source('core', 'teams_leagues_seasons') }} tls
    ON p.season_id = tls.season_id AND pt.team_id = tls.team_id

