from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os


@op
def extract_teams_leagues_seasons() -> pd.DataFrame:
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
        SELECT DISTINCT
            m.home_team_name AS team_name,
            ln.League_name AS league_name,
            l.Country,
            l.Division,
            s.StartingYear,
            s.EndingYear
        FROM dbo.T_F_Match_Stats m
        JOIN dbo.T_DIM_Season s ON s.SeasonSK = m.SeasonSK
        JOIN dbo.T_DIM_League l ON l.competition_id = m.Competition_id
        JOIN dbo.T_DIM_League_name ln ON ln.Competition_id = l.competition_id
    """
    df = pd.read_sql(query, engine)
    return df


@op
def transform_teams_leagues_seasons(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def normalize_name(name: str) -> str:
    if not name:
        return None
    return name.strip().lower().replace("  ", " ")

def normalize_season_name(start, end) -> str:
    starting = int(start)
    ending = int(end)
    if starting == ending:
        ending += 1
    return f"{starting}/{ending}"

def load_teams_lookup(engine) -> dict:
    result = engine.execute(text("SELECT team_id, team_name, common_name FROM core.teams"))
    lookup = {}
    for row in result:
        if row.team_name:
            lookup[normalize_name(row.team_name)] = row.team_id
        if row.common_name:
            lookup[normalize_name(row.common_name)] = row.team_id
    return lookup

def load_leagues_lookup(engine) -> dict:
    result = engine.execute(text("SELECT league_id, key_name FROM core.leagues"))
    lookup = {}
    for row in result:
        if row.key_name:
            lookup[normalize_name(row.key_name)] = row.league_id
    return lookup

def load_seasons_lookup(engine) -> dict:
    result = engine.execute(text("SELECT season_id, name FROM core.seasons"))
    lookup = {}
    for row in result:
        if row.name:
            lookup[row.name] = row.season_id
    return lookup

@op
def load_teams_leagues_seasons(df: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}/"
        f"{os.environ['POSTGRES_DB']}"
    )

    with engine.begin() as connection:
        team_lookup = load_teams_lookup(connection)
        league_lookup = load_leagues_lookup(connection)
        season_lookup = load_seasons_lookup(connection)

        for _, row in df.iterrows():
            team_id = team_lookup.get(normalize_name(row.get("team_name")))
            league_id = league_lookup.get(normalize_name(row.get("league_name")))
            season_id = season_lookup.get(
                normalize_season_name(row.get("StartingYear"), row.get("EndingYear"))
            )

            if not team_id or not league_id or not season_id:
                continue

            connection.execute(
                text("""
                    INSERT INTO core.teams_leagues_seasons(
                        team_id, 
                        league_id, 
                        season_id
                    ) VALUES (
                        :team_id, 
                        :league_id, 
                        :season_id
                    )
                    ON CONFLICT (team_id, season_id, league_id) DO NOTHING
                """),
                {
                    "team_id": team_id,
                    "league_id": league_id,
                    "season_id": season_id
                }
            )
            inserted_rows += 1

    return inserted_rows

@job
def teams_leagues_seasons_etl():
    load_teams_leagues_seasons(
        transform_teams_leagues_seasons(
            extract_teams_leagues_seasons()
        )
    )
