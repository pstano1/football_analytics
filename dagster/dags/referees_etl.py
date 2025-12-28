from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
import urllib
import os
import uuid

@op
def extract_referees() -> pd.DataFrame:
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
        FROM dbo.T_DIM_Referees;
    """

    return pd.read_sql(query, engine)


@op
def transform_referees(referees: pd.DataFrame) -> pd.DataFrame:
    referees.drop_duplicates()
    referees = referees.drop(columns=[
        "Referee_Id", 
        "birthdayGMT"
    ])

    return referees


@op
def load_referees(referees: pd.DataFrame) -> int:
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
        for _, row in referees.iterrows():
            if row["Country"] not in country_to_association_map:
                continue

            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.referees(
                referee_id,
                full_name,
                association_id
            ) VALUES (
                :id,
                :full_name,
                :association_id
            )'''), {
                "id": id,
                "full_name": f"{row['FirstName']} {row['LastName']}",
                "association_id": country_to_association_map[row["Nationality"]]
            })

            inserted_rows += 1

    return inserted_rows

@job
def referees_etl():
    load_referees(transform_referees(extract_referees()))
