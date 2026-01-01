from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os
import uuid


@op
def extract_players() -> pd.DataFrame:
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.environ['SOURCE_MSSQL_HOST']},{os.environ['SOURCE_MSSQL_PORT']};"
        f"DATABASE={os.environ['SOURCE_MSSQL_DB']};"
        f"UID={os.environ['SOURCE_MSSQL_USER']};"
        f"PWD={os.environ['SOURCE_MSSQL_PASSWORD']};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )
    query = """
        SELECT 
            p.*
        FROM 
            dbo.T_DIM_Player p;
    """

    return pd.read_sql(query, engine)


@op
def transform_players(players: pd.DataFrame) -> pd.DataFrame:
    players = players.drop_duplicates()
    players = players.drop(columns=[
        "PlayerSK",
        "EffectiveStartDate",
        "EffectiveEndDate",
        "IsCurrent"
    ])

    return players


@op
def extract_players_teams() -> pd.DataFrame:
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.environ['SOURCE_MSSQL_HOST']},{os.environ['SOURCE_MSSQL_PORT']};"
        f"DATABASE={os.environ['SOURCE_MSSQL_DB']};"
        f"UID={os.environ['SOURCE_MSSQL_USER']};"
        f"PWD={os.environ['SOURCE_MSSQL_PASSWORD']};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )
    query = """
        SELECT
            pls.PlayerSK,
            pls.TeamSK,
            s.SeasonSK,
            p.PlayerName,
            t.TeamName,
            t.CommonName,
            s.StartingYear,
            s.EndingYear
        FROM 
            dbo.T_F_PlayerLeague_Stats pls
        JOIN 
            dbo.T_DIM_Player p ON p.PlayerSK = pls.PlayerSK
        JOIN
            dbo.T_DIM_Team t ON t.TeamSK = pls.TeamSK
        JOIN
            dbo.T_DIM_Season s ON s.SeasonSK = t.SeasonSK;
    """

    return pd.read_sql(query, engine)


@op
def extract_player_statistics() -> pd.DataFrame:
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.environ['SOURCE_MSSQL_HOST']},{os.environ['SOURCE_MSSQL_PORT']};"
        f"DATABASE={os.environ['SOURCE_MSSQL_DB']};"
        f"UID={os.environ['SOURCE_MSSQL_USER']};"
        f"PWD={os.environ['SOURCE_MSSQL_PASSWORD']};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )
    query = """
        SELECT
            pls.*,
            p.PlayerName,
            s.StartingYear,
            s.EndingYear
        FROM 
            dbo.T_F_PlayerLeague_Stats pls
        JOIN 
            dbo.T_DIM_Player p ON p.PlayerSK = pls.PlayerSK
        JOIN
            dbo.T_F_LeagueSeason_Stats ls ON ls.competition_id = pls.competition_id
        JOIN
            dbo.T_DIM_Season s ON s.SeasonSK = ls.SeasonSK;
    """

    return pd.read_sql(query, engine)


def normalize_name(name: str) -> str:
    return (
        name.strip()
            .lower()
            .replace("  ", " ")
    )


def normalize_season_name(start, end) -> str:
    if pd.isna(start) or pd.isna(end):
        return None
        
    ending = int(end)
    starting = int(start)
    if starting == ending:
        ending += 1

    return f"{starting}/{ending}"


def load_players_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT player_id, full_name FROM core.players")
    )

    lookup = {}
    for row in result:
        if row.full_name:
            lookup[normalize_name(row.full_name)] = row.player_id

    return lookup


def load_teams_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT team_id, team_name, common_name FROM core.teams")
    )

    lookup = {}
    for row in result:
        if row.team_name:
            lookup[normalize_name(row.team_name)] = row.team_id

        if row.common_name:
            lookup[normalize_name(row.common_name)] = row.team_id

    return lookup


def load_seasons_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT season_id, name FROM core.seasons")
    )

    lookup = {}
    for row in result:
        if row.name:
            lookup[row.name] = row.season_id

    return lookup


@op
def load_players(players: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        for _, row in players.iterrows():
            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.players(
                player_id,
                full_name,
                birthday,
                primary_position,
                nationality
            ) VALUES (
                :id,
                :full_name,
                :birthday,
                :primary_position,
                :nationality
            )'''), {
                "id": id,
                "full_name": row["PlayerName"],
                "birthday": row["DateOfBirth"],
                "primary_position": row["Position"],
                "nationality": row["Nationality"] if pd.notna(row["Nationality"]) else None
            })

            inserted_rows += 1

    return inserted_rows


@op
def load_players_teams(data: pd.DataFrame, _upstream: int) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        player_lookup = load_players_lookup(connection)
        team_lookup = load_teams_lookup(connection)
        season_lookup = load_seasons_lookup(connection)

        for _, row in data.iterrows():
            player_id = player_lookup.get(normalize_name(row["PlayerName"]))
            
            team_id = team_lookup.get(normalize_name(row["TeamName"])) or \
                      team_lookup.get(normalize_name(row["CommonName"]))
            
            season_id = season_lookup.get(
                normalize_season_name(row["StartingYear"], row["EndingYear"])
            )

            if not player_id or not team_id or not season_id:
                continue

            connection.execute(text('''INSERT INTO core.players_teams(
                player_id,
                team_id,
                season_id
            ) VALUES (
                :player_id,
                :team_id,
                :season_id
            ) ON CONFLICT (player_id, team_id, season_id) DO NOTHING'''), {
                "player_id": player_id,
                "team_id": team_id,
                "season_id": season_id
            })

            inserted_rows += 1

    return inserted_rows


@op
def load_player_statistics(data: pd.DataFrame, _upstream: int) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        player_lookup = load_players_lookup(connection)
        season_lookup = load_seasons_lookup(connection)

        for _, row in data.iterrows():
            player_id = player_lookup.get(normalize_name(row["PlayerName"]))
            season_id = season_lookup.get(
                normalize_season_name(row["StartingYear"], row["EndingYear"])
            )

            if not player_id or not season_id:
                continue

            connection.execute(text('''INSERT INTO core.player_statistics(
                player_id,
                season_id,
                appearances,
                minutes_played,
                goals,
                assists,
                penalty_goals,
                clean_sheets,
                conceded_goals,
                yellow_cards,
                red_cards
            ) VALUES (
                :player_id,
                :season_id,
                :appearances,
                :minutes_played,
                :goals,
                :assists,
                :penalty_goals,
                :clean_sheets,
                :conceded_goals,
                :yellow_cards,
                :red_cards
            ) ON CONFLICT (player_id, season_id) DO UPDATE SET
                appearances = EXCLUDED.appearances,
                minutes_played = EXCLUDED.minutes_played,
                goals = EXCLUDED.goals,
                assists = EXCLUDED.assists,
                penalty_goals = EXCLUDED.penalty_goals,
                clean_sheets = EXCLUDED.clean_sheets,
                conceded_goals = EXCLUDED.conceded_goals,
                yellow_cards = EXCLUDED.yellow_cards,
                red_cards = EXCLUDED.red_cards
            '''), {
                "player_id": player_id,
                "season_id": season_id,
                "appearances": row["Appearances"],
                "minutes_played": row["MinutesPlayed"],
                "goals": row["Goals"],
                "assists": row["Assists"],
                "penalty_goals": 0,
                "clean_sheets": row["CleanSheetsOverall"],
                "conceded_goals": 0,
                "yellow_cards": row["YellowCards"],
                "red_cards": row["RedCards"]
            })

            inserted_rows += 1

    return inserted_rows


@job
def players_etl():
    players_df = transform_players(extract_players())
    players_teams_df = extract_players_teams()
    player_stats_df = extract_player_statistics()

    loaded_players = load_players(players_df)
    _loaded_players_teams = load_players_teams(players_teams_df, loaded_players) 
    _loaded_player_stats = load_player_statistics(player_stats_df, loaded_players)