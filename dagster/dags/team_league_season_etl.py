from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
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
        SELECT
            fls.League_id,
            fls.SeasonSK,
            t.TeamSK,
            t.TeamName,
            t.CommonName,
            l.LeagueName,
            s.StartingYear,
            s.EndingYear
        FROM 
            dbo.T_F_LeagueSeason_Stats fls
        JOIN
            dbo.T_DIM_League l ON l.League_id = fls.League_id
        JOIN
            dbo.T_DIM_Season s ON s.SeasonSK = fls.SeasonSK
        JOIN 
            dbo.T_DIM_Team t ON t.SeasonSK = s.SeasonSK;
    """

    return pd.read_sql(query, engine)


@op
def transform_teams_leagues_seasons(data: pd.DataFrame) -> pd.DataFrame:
    data = data.drop_duplicates()
    return data


def normalize_name(name: str) -> str:
    return (
        name.strip()
            .lower()
            .replace("  ", " ")
    )


def normalize_season_name(start, end) -> str:
    ending = int(end)
    starting = int(start)
    if starting == ending:
        ending += 1

    return f"{starting}/{ending}"


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


def load_leagues_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT league_id, name FROM core.leagues")
    )

    lookup = {}
    for row in result:
        if row.name:
            lookup[normalize_name(row.name)] = row.league_id

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
def load_teams_leagues_seasons(data: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        team_lookup = load_teams_lookup(connection)
        league_lookup = load_leagues_lookup(connection)
        season_lookup = load_seasons_lookup(connection)

        for _, row in data.iterrows():
            team_id = team_lookup.get(normalize_name(row["TeamName"])) or \
                      team_lookup.get(normalize_name(row["CommonName"]))
            
            league_id = league_lookup.get(normalize_name(row["LeagueName"]))
            
            season_id = season_lookup.get(
                normalize_season_name(row["StartingYear"], row["EndingYear"])
            )

            if not team_id or not league_id or not season_id:
                continue

            connection.execute(text('''INSERT INTO core.teams_leagues_seasons(
                team_id,
                league_id,
                season_id
            ) VALUES (
                :team_id,
                :league_id,
                :season_id
            ) ON CONFLICT (team_id, season_id) DO NOTHING'''), {
                "team_id": team_id,
                "league_id": league_id,
                "season_id": season_id
            })

            inserted_rows += 1

    return inserted_rows


@job
def teams_leagues_seasons_etl():
    load_teams_leagues_seasons(transform_teams_leagues_seasons(extract_teams_leagues_seasons()))