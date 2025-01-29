import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Database connection settings
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# File paths and schema mappings
DATA_FILES = {
    "salaries": "data/MLS_TeamSalaries.csv",
    "xgoals_games": "data/MLS_xGoals_Games.csv",
    "xg": "data/MLS_xGoals_Players.csv",
    "goals_added": "data/MLS_GoalsAdded_Players.csv",
    "xp": "data/MLS_xPass_Players.csv",
}

RENAME_MAPPINGS = {
    "salaries": {
        "Team": "team",
        "N": "num_players",
        "TotalGuar": "total_guaranteed",
        "AvgGuar": "avg_guaranteed",
        "MedGuar": "median_guaranteed",
        "StdDevGuar": "stddev_guaranteed",
    },
    "xgoals_games": {
        "Date": "date",
        "Time": "time",
        "Home": "home_team",
        "HG": "home_goals",
        "HxGt": "home_xg",
        "HxGp": "home_xgp",
        "Away": "away_team",
        "AG": "away_goals",
        "AxGt": "away_xg",
        "AxGp": "away_xgp",
        "GD": "goal_difference",
        "xGDt": "xg_difference",
        "xGDp": "xgp_difference",
        "Final": "final_result",
        "HxPts": "home_xpts",
        "AxPts": "away_xpts",
    },
    "xg": {
        "Player": "player",
        "Team": "team",
        "Season": "season",
        "Position": "position",
        "Minutes": "minutes",
        "xG": "xg",
        "xA": "xa",
        "G-xG": "g_minus_xg",
        "A-xA": "a_minus_xa",
        "xG+xA": "xg_plus_xa",
    },
    "goals_added": {
        "Player": "player",
        "Team": "team",
        "Season": "season",
        "Dribbling": "dribbling",
        "Fouling": "fouling",
        "Interrupting": "interrupting",
        "Passing": "passing",
        "Receiving": "receiving",
        "Shooting": "shooting",
        "Goals Added": "goals_added",
    },
    "xp": {
        "Player": "player",
        "Team": "team",
        "Season": "season",
        "Passes": "passes",
        "Pass %": "pass_percentage",
        "xPass %": "xpass_percentage",
        "Score": "score",
        "Per100": "per100",
        "Distance": "distance",
        "Vertical": "vertical",
    },
}

FORMAT_CURRENCY_COLUMNS = {
    "salaries": ["total_guaranteed", "avg_guaranteed", "median_guaranteed", "stddev_guaranteed"]
}

TABLE_SCHEMAS = {
    "salaries": """
        CREATE TABLE IF NOT EXISTS salaries (
            id SERIAL PRIMARY KEY,
            team VARCHAR(50),
            num_players INT,
            total_guaranteed FLOAT,
            avg_guaranteed FLOAT,
            median_guaranteed FLOAT,
            stddev_guaranteed FLOAT
        );
    """,
    "xgoals_games": """
        CREATE TABLE IF NOT EXISTS xgoals_games (
            id SERIAL PRIMARY KEY,
            date DATE,
            time VARCHAR(50),
            home_team VARCHAR(50),
            home_goals INT,
            home_xg FLOAT,
            home_xgp FLOAT,
            away_team VARCHAR(50),
            away_goals INT,
            away_xg FLOAT,
            away_xgp FLOAT,
            goal_difference INT,
            xg_difference FLOAT,
            xgp_difference FLOAT,
            final_result INT,
            home_xpts FLOAT,
            away_xpts FLOAT
        );
    """,
    "xg": """
        CREATE TABLE IF NOT EXISTS xg (
            id SERIAL PRIMARY KEY,
            player VARCHAR(50),
            team VARCHAR(50),
            season VARCHAR(20),
            position VARCHAR(20),
            minutes INT,
            xg FLOAT,
            xa FLOAT,
            g_minus_xg FLOAT,
            a_minus_xa FLOAT,
            xg_plus_xa FLOAT
        );
    """,
    "goals_added": """
        CREATE TABLE IF NOT EXISTS goals_added (
            id SERIAL PRIMARY KEY,
            player VARCHAR(50),
            team VARCHAR(50),
            season VARCHAR(20),
            dribbling FLOAT,
            fouling FLOAT,
            interrupting FLOAT,
            passing FLOAT,
            receiving FLOAT,
            shooting FLOAT,
            goals_added FLOAT
        );
    """,
    "xp": """
        CREATE TABLE IF NOT EXISTS xp (
            id SERIAL PRIMARY KEY,
            player VARCHAR(50),
            team VARCHAR(50),
            season VARCHAR(20),
            passes INT,
            pass_percentage FLOAT,
            xpass_percentage FLOAT,
            score FLOAT,
            per100 FLOAT,
            distance FLOAT,
            vertical FLOAT
        );
    """,
}

def preprocess_data(file_path=None, rename_mapping=None, format_currency_columns=None, test_df=None):
    """
    General preprocessing for any CSV file with optional currency formatting.
    Args:
        file_path (str): Path to the CSV file.
        rename_mapping (dict): Column rename mapping.
        format_currency_columns (list): List of columns to format as currency.
        test_df (pd.DataFrame): Optional DataFrame for testing.
    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    try:
        if test_df is not None:  # Use the test DataFrame if provided
            df = test_df
        else:
            df = pd.read_csv(file_path)

        # Drop miscellaneous columns
        if df.columns[0].lower() in ["", "unnamed: 0"]:
            df = df.iloc[:, 1:]

        # Drop columns not in the rename_mapping
        valid_columns = [col for col in rename_mapping.keys() if col in df.columns]
        df = df[valid_columns]

        # Rename columns
        df = df.rename(columns=rename_mapping)

        # Format currency columns if provided
        if format_currency_columns:
            for col in format_currency_columns:
                if col in df.columns:
                    df[col] = df[col].replace(r"[\$,]", "", regex=True).astype(float)
                    

        # Fill missing values, drop duplicates
        df = df.fillna(0).drop_duplicates()
        return df
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        return pd.DataFrame()

def load_data_to_postgres(df, table_name):
    """
    Load DataFrame into PostgreSQL using pandas.
    Args:
        df (pd.DataFrame): DataFrame to load.
        table_name (str): Target table name in the database.
    """
    try:
        with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"Data successfully loaded into table: {table_name}\n")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}\n")

def setup_tables():
    """
    Create tables dynamically based on the schema.
    """
    try:
        with engine.connect() as conn:
            for table_name, schema in TABLE_SCHEMAS.items():
                conn.execute(text(schema))
        print("Tables created or verified.")
    except Exception as e:
        print(f"Error creating tables: {e}")

def load_all_data():
    """
    Preprocess and load all datasets into PostgreSQL.
    """
    for table_name, file_path in DATA_FILES.items():
        print(f"Processing {table_name}...")
        format_currency_columns = FORMAT_CURRENCY_COLUMNS.get(table_name, None)
        df = preprocess_data(file_path, RENAME_MAPPINGS[table_name], format_currency_columns)
        if not df.empty:
            load_data_to_postgres(df, table_name)

def main(): 
    setup_tables()
    load_all_data()

if __name__ == "__main__":
   main()