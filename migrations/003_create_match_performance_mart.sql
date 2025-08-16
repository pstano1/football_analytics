CREATE SCHEMA mart_match_performance;

CREATE TABLE mart_match_performance.dim_team(
    team_id   UUID PRIMARY KEY,
    team_name VARCHAR(256) NOT NULL
);

CREATE TABLE mart_match_performance.dim_league(
    league_id UUID PRIMARY KEY,
    name      VARCHAR(256) NOT NULL
);

CREATE TABLE mart_match_performance.dim_match(
    match_id   UUID PRIMARY KEY,
    attendence SMALLINT,
    stadium    VARCHAR(256),
    referee    VARCHAR(128)
);

CREATE TABLE mart_match_performance.dim_season(
    season_id UUID PRIMARY KEY,
    name      VARCHAR(32)
);

CREATE TABLE mart_match_performance.fact_match_performance(
    match_id           UUID,
    team_id            UUID,
    opponent_id        UUID,
    season_id          UUID,
    league_id          UUID,
    goals              SMALLINT,
    goals_at_half_time SMALLINT,
    corners            SMALLINT,
    yellow_cards       SMALLINT,
    red_cards          SMALLINT,
    shots_on_target    SMALLINT,
    shots_off_target   SMALLINT,
    fouls              SMALLINT,
    possiession        SMALLINT,
    xg                 SMALLINT,

    PRIMARY KEY (match_id, team_id, opponent_id, season_id, league_id),

    CONSTRAINT dim_match_fk FOREIGN KEY (match_id) REFERENCES mart_match_performance.dim_match(match_id),
    CONSTRAINT dim_team_fk FOREIGN KEY (team_id) REFERENCES mart_match_performance.dim_team(team_id),
    CONSTRAINT dim_opponent_fk FOREIGN KEY (opponent_id) REFERENCES mart_match_performance.dim_team(team_id),
    CONSTRAINT dim_season_fk FOREIGN KEY (season_id) REFERENCES mart_match_performance.dim_season(season_id),
    CONSTRAINT dim_league_fk FOREIGN KEY (league_id) REFERENCES mart_match_performance.dim_league(league_id)
);
