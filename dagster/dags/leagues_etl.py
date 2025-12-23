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
        SELECT * 
        FROM dbo.T_DIM_League;
    """

    return pd.read_sql(query, engine)


@op
def transform_leagues(leagues: pd.DataFrame) -> pd.DataFrame:
    leagues.drop_duplicates()
    leagues = leagues.drop(columns=[
        "InternationalScale", 
        "DomesticScale", 
        "LegueSK", 
    ])

    return leagues


@op
def load_leagues(leagues: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        federations = pd.read_sql(
          "SELECT association_id, country FROM core.federations", 
          connection
        )
        country_to_association_map = dict(zip(federations["country"], federations["association_id"]))
        for _, row in leagues.iterrows():
            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.teams(
                league_id,
                name,
                tier,
                association_id
            ) VALUES (
                :id,
                :full_name,
                :tier,
                :association_id
            )'''), {
                "id": id,
                "full_name": row["LeagueName"],
                "shorttier_name": row["Division"],
                "association_id": country_to_association_map[row["Country"]]
            })

            inserted_rows += 1

    return inserted_rows

@op
def load_league_seasons(leagues: pd.DataFrame) -> int:
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        result = connection.execute(text("""
            INSERT INTO core.league_seasons (
                league_id, 
                season_id
            ) SELECT 
                l.league_id, 
                s.season_id
            FROM core.leagues l
            CROSS JOIN core.seasons s
            ON CONFLICT DO NOTHING;
        """))

        return result.rowcount

@job
def leagues_etl():
    leagues_df = transform_leagues(extract_leagues())

    load_leagues(leagues_df)
    load_league_seasons(leagues_df)

