CREATE SCHEMA mart_team_standings;

CREATE TABLE mart_team_standings.dim_team(
    team_id     UUID PRIMARY KEY,
    team_name   VARCHAR(256),
    common_name VARCHAR(256)
);

CREATE TABLE mart_team_standings.dim_season(
    season_id UUID PRIMARY KEY,
    naem      VARCHAR(32)
);

CREATE TABLE mart_team_standings.dim_league(
    league_id UUID PRIMARY KEY,
    name      VARCHAR(256),
    tier      SMALLINT
);

CREATE TABLE mart_team_standings.fact_team_standings(
    team_id        UUID,
    season_id      UUID,
    league_id      UUID,
    matches_played SMALLINT,
    wins           SMALLINT,
    loses          SMALLINT,
    draws          SMALLINT,
    points         SMALLINT,
    goals          SMALLINT,
    goals_conceded SMALLINT,

    PRIMARY KEY (team_id, season_id, league_id),

    CONSTRAINT dim_team_fk FOREIGN KEY (team_id) REFERENCES mart_team_standings.dim_team(team_id),
    CONSTRAINT dim_season_fk FOREIGN KEY (season_id) REFERENCES mart_team_standings.dim_season(season_id),
    CONSTRAINT dim_league_fk FOREIGN KEY (league_id) REFERENCES mart_team_standings.dim_league(league_id)
);
