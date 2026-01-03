CREATE SCHEMA mart_player_performance;

CREATE TABLE mart_player_performance.dim_player(
    player_id        UUID PRIMARY KEY,
    full_name        VARCHAR(256),
    birthday         DATE,
    primary_position VARCHAR(64),
    nationality      VARCHAR(128)
);

CREATE TABLE mart_player_performance.dim_season(
    season_id UUID PRIMARY KEY,
    name      VARCHAR(32)
);

CREATE TABLE mart_player_performance.dim_league(
    league_id UUID PRIMARY KEY,
    name      VARCHAR(256),
    tier      SMALLINT
);

CREATE TABLE mart_player_performance.dim_team(
    team_id     UUID PRIMARY KEY,
    team_name   VARCHAR(256),
    common_name VARCHAR(256)
);

CREATE TABLE mart_player_performance.fact_player_performance(
    player_id      UUID,
    team_id        UUID,
    season_id      UUID,
    league_id      UUID,
    minutes_played SMALLINT,
    appearances    SMALLINT,
    goals          SMALLINT,
    assists        SMALLINT,
    penalty_goals  SMALLINT,
    penalty_misses SMALLINT,
    clean_sheets   SMALLINT,
    conceded_goals SMALLINT,
    yellow_cards   SMALLINT,
    red_cards      SMALLINT,

    PRIMARY KEY (player_id, team_id, season_id, league_id),

    CONSTRAINT dim_player_fk FOREIGN KEY (player_id) REFERENCES mart_player_performance.dim_player(player_id),
    CONSTRAINT dim_team_fk FOREIGN KEY (team_id) REFERENCES mart_player_performance.dim_team(team_id),
    CONSTRAINT dim_season_fk FOREIGN KEY (season_id) REFERENCES mart_player_performance.dim_season(season_id),
    CONSTRAINT dim_league_fk FOREIGN KEY (league_id) REFERENCES mart_player_performance.dim_league(league_id)
);
