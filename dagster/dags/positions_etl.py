from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
import urllib
import os
import uuid

@op
def extract_positions() -> pd.DataFrame:
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
            DISTINCT Position
        FROM 
            dbo.T_DIM_Player;
    """

    return pd.read_sql(query, engine)


@op
def transform_positions(positions: pd.DataFrame) -> pd.DataFrame:
    positions.drop_duplicates()

    return positions


@op
def load_positions(positions: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
        for _, row in positions.iterrows():
            connection.execute(text('''INSERT INTO core.positions(
                position
            ) VALUES (
                :position
            )'''), {
                "position": row["Position"]
            })

            inserted_rows += 1

    return inserted_rows

@job
def positions_etl():
    load_positions(transform_positions(extract_positions()))

