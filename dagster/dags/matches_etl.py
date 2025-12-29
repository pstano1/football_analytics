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
        SELECT
            m.*,
            r.FirstName AS ref_name,
            r.LastName AS ref_surname,
            v.VenueName AS stadium_name,
            s.StartingYear AS season_starting_year,
            s.EndingYear AS season_ending_year
        FROM 
            dbo.T_F_Match_Stats m
        JOIN 
            dbo.T_DIM_Referees r ON r.Referee_Id = m.Referee_id
        JOIN
            dbo.T_DIM_Venue v ON v.VenueSK = m.VenueSK
        JOIN
            dbo.T_DIM_Season s ON s.SeasonSK = m.SeasonSK;
    """

    return pd.read_sql(query, engine)


@op
def transform_matches(matches: pd.DataFrame) -> pd.DataFrame:
    matches = matches.drop_duplicates()
    
    required_columns = [
        'MatchSK', 'DateSK', 'SeasonSK', 'HomeTeam_id', 'AwayTeam_id', 'VenueSK',
        'homeGoalCount', 'awayGoalCount', 'ht_goals_team_a', 'ht_goals_team_b',
        'team_a_corners', 'team_b_corners',
        'team_a_yellow_cards', 'team_b_yellow_cards',
        'team_a_red_cards', 'team_b_red_cards',
        'team_a_shotsOnTarget', 'team_b_shotsOnTarget',
        'team_a_shotsOffTarget', 'team_b_shotsOffTarget',
        'team_a_fouls', 'team_b_fouls',
        'team_a_possession', 'team_b_possession',
        'team_a_xg', 'team_b_xg', 
        'ref_name', 'ref_surname', 'stadium_name',
        'season_starting_year', 'season_ending_year',
        'home_team_name', 'away_team_name', 'attendance'
    ]
    
    keep_cols = [col for col in required_columns if col in matches.columns]
    matches_cleaned = matches[keep_cols].copy()
    
    if 'DateSK' in matches_cleaned.columns:
        matches_cleaned['date'] = pd.to_datetime(matches_cleaned['DateSK'], format='%Y%m%d').dt.date
        matches_cleaned = matches_cleaned.drop(columns=['DateSK'])

    return matches_cleaned


def normalize_name(name: str) -> str:
    return (
        name.strip()
            .lower()
            .replace("  ", " ")
    )


def load_referee_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT referee_id, full_name FROM core.referees")
    )

    lookup = {}
    for row in result:
        lookup[normalize_name(row.full_name)] = row.referee_id

    return lookup


def load_stadium_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT stadium_id, name FROM core.stadiums")
    )

    lookup = {}
    for row in result:
        lookup[normalize_name(row.name)] = row.stadium_id

    return lookup


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


def normalize_season_name(start, end) -> str:
    ending = int(end)
    staring = int(start)
    if (staring == ending):
        ending += 1

    return f"{staring}/{ending}"


def load_season_lookup(engine) -> dict:
    result = engine.execute(
        text("SELECT season_id, name FROM core.seasons")
    )

    lookup = {}
    for row in result:
        if row.name:
            lookup[row.name] = row.season_id

    return lookup


@op
def load_matches(matches: pd.DataFrame) -> int:
    inserted_rows = 0
    engine = create_engine(f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:" \
        f"{os.environ['POSTGRES_PASSWORD']}@minerva_postgres:{os.environ['POSTGRES_PORT']}" \
        f"/{os.environ['POSTGRES_DB']}"
    )
    with engine.begin() as connection:
       referee_lookup = load_referee_lookup(connection)
       stadium_lookup = load_stadium_lookup(connection)
       team_lookup = load_teams_lookup(connection)
       season_lookup = load_season_lookup(connection)

       for _, row in matches.iterrows():
            id = str(uuid.uuid4())

            referee_full_name = normalize_name(
                f"{row['ref_name']} {row['ref_surname']}"
            )
            referee_id = referee_lookup.get(referee_full_name)
            stadium_id = stadium_lookup.get(
                normalize_name(row["stadium_name"])
            )
            home_team_id = team_lookup.get(
                normalize_name(row["home_team_name"])
            )
            away_team_id = team_lookup.get(
                normalize_name(row["away_team_name"])
            )
            season_id = season_lookup.get(
                normalize_season_name(row["season_starting_year"], row["season_ending_year"])
            )

            connection.execute(text('''INSERT INTO core.matches(
                match_id,
                date,
                home_team_id,
                away_team_id,
                season_id,
                referee_id,
                stadium_id,
                attendance,
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
                :away_team_id,
                :season_id,
                :referee_id,
                :stadium_id,
                :attendance,
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
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "season_id": season_id,
                "referee_id": referee_id,
                "stadium_id": stadium_id,
                "attendance": row.get("attendance"),
                "home_team_goals": row["homeGoalCount"],
                "away_team_goals": row["awayGoalCount"],
                "home_team_goals_at_half_time": row["ht_goals_team_a"],
                "away_team_goals_at_half_time": row["ht_goals_team_b"],
                "home_team_corners": row["team_a_corners"],
                "away_team_corners": row["team_b_corners"],
                "home_team_yellow_cards": row["team_a_yellow_cards"],
                "away_team_yellow_cards": row["team_b_yellow_cards"],
                "home_team_red_cards": row["team_a_red_cards"],
                "away_team_red_cards": row["team_b_red_cards"],
                "home_team_shots_on_target": row["team_a_shotsOnTarget"],
                "away_team_shots_on_target": row["team_b_shotsOnTarget"],
                "home_team_shots_off_target": row["team_a_shotsOffTarget"],
                "away_team_shots_off_target": row["team_b_shotsOffTarget"],
                "home_team_fouls": row["team_a_fouls"],
                "away_team_fouls": row["team_b_fouls"],
                "home_team_possession": row["team_a_possession"],
                "away_team_possession": row["team_b_possession"],
                "home_team_xg": row["team_a_xg"],
                "away_team_xg": row["team_b_xg"]
            })

            inserted_rows += 1

    return inserted_rows

@job
def matches_etl():
    load_matches(transform_matches(extract_matches()))

