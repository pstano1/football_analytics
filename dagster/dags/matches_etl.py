from dagster import job, op
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
import urllib
import os
import uuid


@op
def extract_matches() -> pd.DataFrame:
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
        FROM dbo.T_F_Match_Stats;
    """

    return pd.read_sql(query, engine)


@op
def transform_matches(matches: pd.DataFrame) -> pd.DataFrame:
    matches.drop_duplicates()
    required_columns = [
        'MatchSK', 'DateSK', 'SeasonSK', 'HomeTeamSK', 'AwayTeamSK', 'VenueSK',
        'HomeGoalCount', 'AwayGoalCount', 'HomeGoalsHT', 'AwayGoalsHT',
        'HomeCorners', 'AwayCorners',
        'HomeYellowCards', 'AwayYellowCards',
        'HomeRedCards', 'AwayRedCards',
        'HomeShotsOnTarget', 'AwayShotsOnTarget',
        'HomeShotsOffTarget', 'AwayShotsOffTarget',
        'HomeFouls', 'AwayFouls',
        'HomePossession', 'AwayPossession',
        'HomeXG', 'AwayXG', 
        'HomeTeamName', 'AwayTeamName'
    ]
    keep_cols = [col for col in required_columns if col in matches.columns]
    if 'DateSK' in keep_cols.columns:
        keep_cols['date'] = pd.to_datetime(matches['DateSK'], format='%Y%m%d').dt.date
        keep_cols = keep_cols.drop(columns=['DateSK'])
    matches_cleaned = matches.drop_duplicates().loc[:, keep_cols]

    return matches_cleaned


@op
def load_matches(matches: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
       for _, row in matches.iterrows():
            id = str(uuid.uuid4())
            connection.execute(text('''INSERT INTO core.matches(
                match_id,
                date,
                home_team_id,
                away_team_id,
                season_id,
                referee_id,
                stadium_id,
                attendence,
                home_team_goals,
                away_team_goals,
                home_team_goals_at_half_time,
                away_team_goals_at_half_time,
                home_team_corners,
                away_team_corners,
                home_team_yellow_cards,
                away_team_yellow_cards,
                home_team_red_cards,
                away_team_red_cards,
                home_team_shots_on_target,
                away_team_shots_on_target,
                home_team_shots_off_target,
                away_team_shots_off_target,
                home_team_fouls,
                away_team_fouls,
                home_team_possession,
                away_team_possession,
                home_team_xg,
                away_team_xg
            ) VALUES (
                :id,
                :date,
                :home_team_id,
                :away_team_id
                :referee_id,
                :stadium_id,
                :attendence,
                :home_team_goals,
                :away_team_goals,
                :home_team_goals_at_half_time,
                :away_team_goals_at_half_time,
                :home_team_corners,
                :away_team_corners,
                :home_team_yellow_cards,
                :away_team_yellow_cards,
                :home_team_red_cards,
                :away_team_red_cards,
                :home_team_shots_on_target,
                :away_team_shots_on_target,
                :home_team_shots_off_target,
                :away_team_shots_off_target,
                :home_team_fouls,
                :away_team_fouls,
                :home_team_possession,
                :away_team_possession,
                :home_team_xg,
                :away_team_xg
            )'''), {
                "id": id,
                "date": row["date"],
                "home_team_id": "do a lookup here",
                "away_team_id": "do a lookup here as well",
                "season_id": "here need a lookup as well",
                "referee_id": None,
                "stadium_id": None,
                "attendence": 0,
                "home_team_goals": row["HomeGoalCount"],
                "away_team_goals": row["AwayGoalCount"],
                "home_team_goals_at_half_time": row["HomeGoalsHT"],
                "away_team_goals_at_half_time": row["AwayGoalsHT"],
                "home_team_corners": row["HomeCorners"],
                "away_team_corners": row["AwayCorners"],
                "home_team_yellow_cards": row["HomeYellowCards"],
                "away_team_yellow_cards": row["AwayYellowCards"],
                "home_team_red_cards": row["HomeRedCards"],
                "away_team_red_cards": row["AwayRedCards"],
                "home_team_shots_on_target": row["HomeShotsOnTarget"],
                "away_team_shots_on_target": row["AwayShotsOnTarget"],
                "home_team_shots_off_target": row["HomeShotsOffTarget"],
                "away_team_shots_off_target": row["AwayShotsOffTarget"],
                "home_team_fouls": row["HomeFouls"],
                "away_team_fouls": row["AwayFouls"],
                "home_team_possession": row["HomePossession"],
                "away_team_possession": row["AwayPossession"],
                "home_team_xg": 0,
                "away_team_xg": 0
            })

            inserted_rows += 1

    return inserted_rows

@job
def matches_etl():
    load_matches(transform_matches(extract_matches()))

