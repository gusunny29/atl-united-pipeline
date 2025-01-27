import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import os


# Database connection settings
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "sunnygu"
DB_PASSWORD = "Lke385gu!"

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def preprocess_salaries(file_path):
    """
    Preprocess the salaries data.
    Args:
        file_path (str): Path to the CSV file.
    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """

    # read in the csv into a pandas dataframe 
    df = pd.read_csv(file_path)

    # Drop miscellaneous columns if necessary
    if df.columns[0].lower() in ["", "unnamed: 0"]:
        df = df.iloc[:, 1:]

    # Rename columns to match PostgreSQL schema
    df = df.rename(columns={
        "Team": "team",
        "N": "num_players",
        "TotalGuar": "total_guaranteed",
        "AvgGuar": "avg_guaranteed",
        "MedGuar": "median_guaranteed",
        "StdDevGuar": "stddev_guaranteed"
    })

    # Clean salary columns and convert to floats
    salary_columns = ["total_guaranteed", "avg_guaranteed", "median_guaranteed", "stddev_guaranteed"]
    for col in salary_columns:
        if col in df.columns:
            df[col] = df[col].replace(r"[\$,]", "", regex=True).astype(float)

    # Ensure num_players is an integer
    if "num_players" in df.columns:
        df["num_players"] = df["num_players"].astype(int)

         # Remove invalid rows (e.g., negative salaries)
    df = df[df[salary_columns].min(axis=1) >= 0]

    # Fill missing values
    df = df.fillna(0)
    # Remove duplicates
    df = df.drop_duplicates()


    return df


def preprocess_xgoals(file_path):
    """
    Preprocess the xGoalsGames data.
    Args:
        file_path (str): Path to the CSV file.
    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    df = pd.read_csv(file_path)
    # Rename columns to ensure they match the PostgreSQL schema
    df = df.rename(columns={
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
        "AxPts": "away_xpts"
    })
    # Fill missing values with 0
    df = df.fillna(0)
    # Remove duplicates
    df = df.drop_duplicates()
    return df

def load_data_to_postgres(df, table_name):
    """
    Load DataFrame into PostgreSQL using pandas.
    Args:
        df (pd.DataFrame): DataFrame to load.
        table_name (str): Target table name in the database.
    """
    try:
        # Write DataFrame to PostgreSQL
        with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"Data successfully loaded into table: {table_name}")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

def setup_tables():
    """
    Create the PostgreSQL tables if they do not exist.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS salaries (
                id SERIAL PRIMARY KEY,
                team VARCHAR(50),
                num_players INT,
                total_guaranteed FLOAT,
                avg_guaranteed FLOAT,
                median_guaranteed FLOAT,
                stddev_guaranteed FLOAT
            );
            """))
            conn.execute(text("""
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
            """))
            print("Tables created or verified.")
    except Exception as e:
        print(f"Error creating tables: {e}")


if __name__ == "__main__":
    # Paths to the input CSV files
    salaries_file = "../data/MLS_TeamSalaries.csv"
    xgoals_file = "../data/MLS_xGoals_Games.csv"
    
    # Ensure tables exist
    setup_tables()

    # Preprocess and load salaries data
    salaries_df = preprocess_salaries(salaries_file)
    load_data_to_postgres(salaries_df, "salaries")

    # Preprocess and load xGoalsGames data
    xgoals_df = preprocess_xgoals(xgoals_file)
    load_data_to_postgres(xgoals_df, "xgoals_games")
