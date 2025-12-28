CREATE SCHEMA core;

CREATE TABLE core.seasons(
    season_id UUID PRIMARY KEY,
    name      VARCHAR(32) NOT NULL
);

CREATE TABLE core.countries(
    name VARCHAR(128) PRIMARY KEY,
    code VARCHAR(2)   UNIQUE NOT NULL
);

CREATE TABLE core.stadiums(
    stadium_id UUID PRIMARY KEY,
    name       VARCHAR(256) UNIQUE NOT NULL,
    capacity   INTEGER
);

CREATE TABLE core.associations(
    association_id UUID PRIMARY KEY,
    full_name      VARCHAR(256) NOT NULL,
    short_name     VARCHAR(32),
    country        VARCHAR(128),

    CONSTRAINT association_country_fk FOREIGN KEY (country) REFERENCES core.countries(name)
);

CREATE TABLE core.referees(
    referee_id     UUID PRIMARY KEY,
    full_name      VARCHAR(128) NOT NULL,
    association_id UUID,

    CONSTRAINT referee_association_fk FOREIGN KEY (association_id) REFERENCES core.associations(association_id)
);

CREATE TABLE core.associations_self_relations(
    parent_association UUID,
    child_association  UUID,

    CONSTRAINT parent_association_associations_fk FOREIGN KEY (parent_association) REFERENCES core.associations(association_id),
    CONSTRAINT child_association_associations_fk FOREIGN KEY (child_association) REFERENCES core.associations(association_id),
    
    PRIMARY KEY (parent_association, child_association)
);

CREATE TABLE core.leagues(
    league_id      UUID PRIMARY KEY,
    name           VARCHAR(256) NOT NULL,
    association_id UUID,
    tier           SMALLINT,

    CONSTRAINT league_association_fk FOREIGN KEY (association_id) REFERENCES core.associations(association_id)
);

CREATE TABLE core.leagues_seasons(
    league_id UUID,
    season_id UUID,

    CONSTRAINT leagues_seasons_league_fk FOREIGN KEY (league_id) REFERENCES core.leagues(league_id),
    CONSTRAINT leagues_seasons_season_fk FOREIGN KEY (season_id) REFERENCES core.seasons(season_id),

    PRIMARY KEY (league_id, season_id)
);

CREATE TABLE core.positions(
    position VARCHAR(64) PRIMARY KEY
);

CREATE TABLE core.players(
    player_id        UUID PRIMARY KEY,
    full_name        VARCHAR(256) NOT NULL,
    birthday        DATE,
    primary_position VARCHAR(64),
    nationality      VARCHAR(128),

    CONSTRAINT player_position_fk FOREIGN KEY (primary_position) REFERENCES core.positions(position),
    CONSTRAINT player_country_fk FOREIGN KEY (nationality) REFERENCES core.countries(name)
);

CREATE TABLE core.player_statistics(
    player_id      UUID,
    season_id      UUID,
    appearances    SMALLINT,
    minutes_played SMALLINT,
    goals          SMALLINT,
    assists        SMALLINT,
    penalty_goals  SMALLINT,
    penalty_misses SMALLINT,
    clean_sheets   SMALLINT,
    conceded_goals SMALLINT,
    yellow_cards   SMALLINT,
    red_cards      SMALLINT,

    PRIMARY KEY (player_id, season_id)
);

CREATE TABLE core.teams(
    team_id UUID PRIMARY KEY,
    team_name VARCHAR(256) NOT NULL,
    common_name VARCHAR(256),
    association_id UUID,
    home_stadium UUID,

    CONSTRAINT team_association_fk FOREIGN KEY (association_id) REFERENCES core.associations(association_id),
    CONSTRAINT team_stadium_fk FOREIGN KEY (home_stadium) REFERENCES core.stadiums(stadium_id)
);

CREATE TABLE core.players_teams(
    player_id UUID,
    team_id   UUID,
    season_id UUID,

    PRIMARY KEY(player_id, team_id, season_id)
);

CREATE TABLE core.matches(
    match_id                     UUID PRIMARY KEY,
    date                         DATE NOT NULL,
    home_team_id                 UUID,
    away_team_id                 UUID,
    season_id                    UUID,
    referee_id                   UUID,
    stadium_id                   UUID,
    attendence                   SMALLINT,
    home_team_goals              SMALLINT,
    away_team_goals              SMALLINT,
    home_team_goals_at_half_time SMALLINT,
    away_team_goals_at_half_time SMALLINT,
    home_team_corners            SMALLINT,
    away_team_corners            SMALLINT,
    home_team_yellow_cards       SMALLINT,
    away_team_yellow_cards       SMALLINT,
    home_team_red_cards          SMALLINT,
    away_team_red_cards          SMALLINT,
    home_team_shots_on_target    SMALLINT,
    away_team_shots_on_target    SMALLINT,
    home_team_shots_off_target   SMALLINT,
    away_team_shots_off_target   SMALLINT,
    home_team_fouls              SMALLINT,
    away_team_fouls              SMALLINT,
    home_team_possession         SMALLINT,
    away_team_possession         SMALLINT,
    home_team_xg                 DECIMAL(8),
    away_team_xg                 DECIMAL(8),

    CONSTRAINT match_home_team_fk FOREIGN KEY (home_team_id) REFERENCES core.teams(team_id),
    CONSTRAINT match_away_team_fk FOREIGN KEY (away_team_id) REFERENCES core.teams(team_id),
    CONSTRAINT match_season_fk FOREIGN KEY (season_id) REFERENCES core.seasons(season_id),
    CONSTRAINT match_referee_fk FOREIGN KEY (referee_id) REFERENCES core.referees(referee_id),
    CONSTRAINT match_stadium_fk FOREIGN KEY (stadium_id) REFERENCES core.stadiums(stadium_id)
);

CREATE TABLE core.teams_leagues_seasons (
    team_id   UUID NOT NULL,
    league_id UUID NOT NULL,
    season_id UUID NOT NULL,

    CONSTRAINT teams_leagues_seasons_team_fk FOREIGN KEY (team_id) REFERENCES core.teams(team_id),
    CONSTRAINT teams_leagues_seasons_league_fk FOREIGN KEY (league_id) REFERENCES core.leagues(league_id),
    CONSTRAINT teams_leagues_seasons_season_fk FOREIGN KEY (season_id) REFERENCES core.seasons(season_id),
    
    PRIMARY KEY (team_id, season_id)
);
