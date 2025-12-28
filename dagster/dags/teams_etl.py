from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
import urllib
import os
import uuid

@op
def extract_teams() -> pd.DataFrame:
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
        FROM dbo.T_DIM_Team;
    """

    return pd.read_sql(query, engine)


@op
def transform_teams(teams: pd.DataFrame) -> pd.DataFrame:
    teams.drop_duplicates()
    teams = teams.drop(columns=[
        "TeamSK", 
        "CoachName", 
        "Founded", 
        "EffectiveStartDate", 
        "EffectiveEndDate", 
        "IsCurrent"
    ])

    return teams


@op
def load_teams(teams: pd.DataFrame) -> int:
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
        for _, row in teams.iterrows():
            if row["Country"] not in country_to_association_map:
                continue

            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.teams(
                team_id,
                team_name,
                common_name,
                association_id
            ) VALUES (
                :id,
                :full_name,
                :short_name,
                :association_id
            )'''), {
                "id": id,
                "full_name": row["TeamName"],
                "short_name": row["CommonName"],
                "association_id": country_to_association_map[row["Country"]]
            })

            inserted_rows += 1

    return inserted_rows

@job
def teams_etl():
    load_teams(transform_teams(extract_teams()))

