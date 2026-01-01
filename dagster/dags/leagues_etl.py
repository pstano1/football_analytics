from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
import urllib
import os
import uuid

@op
def extract_leagues() -> pd.DataFrame:
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
            ln.League_name AS key_name,
            ln.name AS actual_name,
            l.Country,
            l.Division
        FROM 
            dbo.T_DIM_League l
        JOIN 
            dbo.T_DIM_League_name ln ON ln.Competition_id = l.competition_id
        ORDER BY l.Country, l.Division;
    """

    return pd.read_sql(query, engine)


@op
def extract_leagues_seasons() -> pd.DataFrame:
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
            ln.League_name AS league_name,
            l.Country,
            l.Division,
            fls.SeasonSK,
            s.StartingYear,
            s.EndingYear
        FROM 
            dbo.T_F_LeagueSeason_Stats fls
        JOIN
            dbo.T_DIM_League l ON l.competition_id = fls.competition_id
        JOIN 
            dbo.T_DIM_League_name ln ON ln.Competition_id = l.competition_id
        JOIN
            dbo.T_DIM_Season s ON s.SeasonSK = fls.SeasonSK;
    """

    return pd.read_sql(query, engine)


@op
def transform_leagues(leagues: pd.DataFrame) -> pd.DataFrame:
    return leagues.drop_duplicates()


def normalize_name(name: str) -> str:
    return name.strip().lower().replace("  ", " ")


def normalize_season_name(start, end) -> str:
    ending = int(end)
    starting = int(start)
    if starting == ending:
        ending += 1
    return f"{starting}/{ending}"


@op
def load_leagues(leagues: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        federations = pd.read_sql(
          "SELECT association_id, country FROM core.associations", 
          connection
        )
        country_to_association_map = dict(zip(federations["country"], federations["association_id"]))
        
        for _, row in leagues.iterrows():
            if row["Country"] not in country_to_association_map:
                continue

            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.leagues(
                league_id,
                name,
                key_name,
                tier,
                association_id
            ) VALUES (
                :id,
                :full_name,
                :key_name,
                :tier,
                :association_id
            ) ON CONFLICT (name, association_id) DO NOTHING'''), {
                "id": id,
                "full_name": row["actual_name"],
                "key_name": row["key_name"],
                "tier": row["Division"],
                "association_id": country_to_association_map[row["Country"]]
            })

            inserted_rows += 1

    return inserted_rows


@op
def load_leagues_seasons(data: pd.DataFrame, _upstream: int) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        leagues_df = pd.read_sql("SELECT league_id, key_name FROM core.leagues", connection)
        seasons_df = pd.read_sql("SELECT season_id, name FROM core.seasons", connection)
        
        league_lookup = dict(zip(leagues_df["key_name"].apply(normalize_name), leagues_df["league_id"]))
        season_lookup = dict(zip(seasons_df["name"], seasons_df["season_id"]))
        
        for _, row in data.iterrows():
            league_id = league_lookup.get(normalize_name(row["league_name"]))
            season_id = season_lookup.get(
                normalize_season_name(row["StartingYear"], row["EndingYear"])
            )
            
            if not league_id or not season_id:
                continue
                
            connection.execute(text('''INSERT INTO core.leagues_seasons(
                league_id,
                season_id
            ) VALUES (
                :league_id,
                :season_id
            ) ON CONFLICT DO NOTHING'''), {
                "league_id": league_id,
                "season_id": season_id
            })
            
            inserted_rows += 1
    
    return inserted_rows


@job
def leagues_etl():
    leagues_df = transform_leagues(extract_leagues())
    leagues_seasons_df = extract_leagues_seasons()

    loaded_leagues = load_leagues(leagues_df)
    load_leagues_seasons(leagues_seasons_df, loaded_leagues)