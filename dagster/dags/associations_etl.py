from dagster import job, op
import pandas as pd
import requests
from sqlalchemy import create_engine, text
import os
import uuid

@op
def extract_associations() -> pd.DataFrame:
    URL = "https://raw.githubusercontent.com/openpotato/fifa-codes/refs/heads/main/src/fifa-member-associations.csv"
    res = requests.get(URL)
    res.raise_for_status()
    from io import StringIO

    return pd.read_csv(StringIO(res.text))


@op
def transform_associations(federations: pd.DataFrame) -> pd.DataFrame:
    federations = federations.drop_duplicates()
    federations[["associationName", "associationCode"]] = federations["FIFA.MemberAssociation"].str.extract(r"^(.*) \((.*)\)$")
    federations = federations.drop(columns=[
        "ContinentalConfederation.Name",
        "Subdivision.Iso3166.Code", 
        "FIFA.MemberAssociation",
        "Country.Name",
        "FIFA.Joined",
        "Established"
    ])
    federations.rename(columns={
        "Country.Iso3166.Alpha2Code": "countryCode",
        "FIFA.Code": "codeName",
        "ContinentalConfederation.Code": "continentalConfederationCode"
    }, inplace=True)

    return federations

@op
def load_associations(federations: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
      countries = pd.read_sql(
          "SELECT code, name FROM core.countries", 
          connection
      )
      code_to_country_name_map = dict(zip(countries["code"], countries["name"]))

      associations = pd.read_sql("SELECT association_id, short_name FROM core.associations", connection)
      name_to_id = dict(zip(associations["short_name"], associations["association_id"]))

      fifa_id = name_to_id["FIFA"]
      continentals = ["UEFA", "AFC", "CAF", "CONCACAF", "CONMEBOL", "OFC"]
      for continental in continentals:
        connection.execute(text('''INSERT INTO core.associations_self_relations(
            parent_association, 
            child_association
        ) VALUES (
          :parent_association, 
          :child_association
        ) ON CONFLICT (parent_association, child_association) DO NOTHING'''), {
          "parent_association": fifa_id,
          "child_association": name_to_id[continental]
        })

      for _, row in federations.iterrows():
        if row["countryCode"] not in code_to_country_name_map:
            continue
        id = str(uuid.uuid4())
        connection.execute(text('''INSERT INTO core.associations(
          association_id,
          full_name,
          short_name,
          country
        ) VALUES (
          :id,
          :full_name,
          :short_name,
          :country
        )'''), {
            "id": id,
            "full_name": row["associationName"],
            "short_name": row["associationCode"],
            "country": code_to_country_name_map[row["countryCode"]]
        })

        connection.execute(text('''INSERT INTO core.associations_self_relations(
          parent_association, 
          child_association
        ) VALUES (
            :parent_association, 
            :child_association
        ) ON CONFLICT (parent_association, child_association) DO NOTHING'''), {
            "parent_association": name_to_id[row["continentalConfederationCode"]], 
            "child_association": id
        })
        inserted_rows += 1

    print(f"Inserted {inserted_rows} federations")
    return inserted_rows


@job
def associations_etl():
    load_associations(transform_associations(extract_associations()))

