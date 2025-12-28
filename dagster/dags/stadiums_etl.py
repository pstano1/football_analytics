from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
import urllib
import os
import uuid

@op
def extract_stadiums() -> pd.DataFrame:
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
        FROM dbo.T_DIM_Venue;
    """

    return pd.read_sql(query, engine)


@op
def transform_stadiums(stadiums: pd.DataFrame) -> pd.DataFrame:
    stadiums.drop_duplicates()
    stadiums = stadiums.drop(columns=[
        "VenueSK", 
        "Location",
        "City",
        "EffectiveStartDate",
        "EffectiveEndDate",
        "IsCapacity"
    ])

    return stadiums


@op
def load_stadiums(stadiums: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        for _, row in stadiums.iterrows():
            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.stadiums(
                stadium_id,
                name,
            ) VALUES (
                :id,
                :full_name,
            )'''), {
                "id": id,
                "full_name": row["VenueName"],
            })

            inserted_rows += 1

    return inserted_rows

@job
def stadiums_etl():
    load_stadiums(transform_stadiums(extract_stadiums()))
