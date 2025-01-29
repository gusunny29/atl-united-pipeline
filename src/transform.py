import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import numpy as np

# Load environment variables
load_dotenv()

# Database connection settings
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def create_player_performance_metrics():
    """
    Combine goalsadded, xGoals, and xPass into a single table: player_performance_metrics.
    """
    try:
        print("Fetching data from the database...")
        # Fetch data from the database
        goalsadded_query = "SELECT * FROM goals_added"
        xgoals_query = "SELECT * FROM xg"
        xpass_query = "SELECT * FROM xp"

        goalsadded_df = pd.read_sql(goalsadded_query, engine)
        xgoals_df = pd.read_sql(xgoals_query, engine)
        xpass_df = pd.read_sql(xpass_query, engine)

        goalsadded_df = goalsadded_df.drop_duplicates(subset=["player", "team", "season"])
        xgoals_df = xgoals_df.drop_duplicates(subset=["player", "team", "season"])
        xpass_df = xpass_df.drop_duplicates(subset=["player", "team", "season"])

        for df in [goalsadded_df, xgoals_df, xpass_df]:
            df["player"] = df["player"].str.strip()
            df["team"] = df["team"].str.strip()
            df["season"] = df["season"].astype(str).str.strip()


        print("Merging datasets...")
        # Merge datasets on player, team, and season
        merged_df = (
            goalsadded_df.merge(xgoals_df, on=["player", "team", "season"], how="outer")
            .merge(xpass_df, on=["player", "team", "season"], how="outer")
        )

        # Fill NaN values with 0 for easier analysis
        merged_df = merged_df.fillna(0)

        if "Position" in merged_df.columns:
            merged_df = merged_df.drop(columns=["Position"])

        # Assign unique player IDs
        merged_df = assign_player_ids(merged_df)

        # Save the merged table into PostgreSQL
        print("Saving combined player performance metrics to the database...")
        merged_df.to_sql("player_performance_metrics", engine, if_exists="replace", index=False)
        print("Player performance metrics table created successfully.\n")
    except Exception as e:
        print(f"Error creating player_performance_metrics: {e}")

def assign_player_ids(merged_df): 
    print("Assigning unique player IDs...")
    # Assign unique player IDs using groupby and ngroup
    merged_df["player_id"] = (
        merged_df.groupby(["player"])
        .ngroup()  # Assigns a unique number to each player
        + 1  # Start IDs from 1 instead of 0
    )

    # Reorder columns to place player_id at the front (optional)
    columns = ["player_id"] + [col for col in merged_df.columns if col != "player_id"]
    merged_df = merged_df[columns]

    return merged_df[columns]


def add_per_90_and_efficiency_metrics():
    """
    Add per 90 stats and efficiency metrics to the player_performance_metrics table in one step.
    """
    try:
        print("Fetching player performance metrics data...")
        # Fetch the player performance metrics table
        query = "SELECT * FROM player_performance_metrics"
        df = pd.read_sql(query, engine)

        # Calculate per 90 stats
        print("Calculating per 90 stats...")
        df = calculate_per_90_stats(df)

        # Calculate efficiency metrics
        print("Calculating efficiency metrics...")
        df = calculate_efficiency_metrics(df)

        # Save the updated table back to the database
        print("Saving updated player performance metrics...\n")
        save_to_database(df, "player_performance_metrics")
        print("Per 90 stats and efficiency metrics added successfully!")
    except Exception as e:
        print(f"Error adding per 90 and efficiency metrics: {e}\n")

def calculate_per_90_stats(df):
    """
    Calculate per 90 stats for the given DataFrame.
    Args:
        df (pd.DataFrame): Player performance metrics DataFrame.
    Returns:
        pd.DataFrame: DataFrame with per 90 stats added.
    """
    per_90_metrics = [
        "goals_added",  # Example from goals_added
        "xg",           # Expected Goals
        "xa",           # Expected Assists
        "passes",       # Total Passes
        "dribbling",    # Dribbling metric
        "shooting",     # Shooting metric (if available)
    ]

    # Ensure minutes are non-zero to avoid division errors
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0).astype(int)


    for metric in ["xg", "xa"]:
        df[f"{metric}_per_90"] = (df[metric] / df["minutes"] * 90).replace([np.inf, -np.inf], 0).fillna(0).round(3)


    return df

def calculate_efficiency_metrics(df):
    """
    Calculate efficiency metrics for the given DataFrame.
    Args:
        df (pd.DataFrame): Player performance metrics DataFrame.
    Returns:
        pd.DataFrame: DataFrame with efficiency metrics added.
    """
    # Goal Conversion Rate
    if "G" in df.columns and "Shots" in df.columns:
        df["goal_conversion_rate"] = (df["G"] / df["Shots"]).round(3)

    # Shot Accuracy
    if "SoT" in df.columns and "Shots" in df.columns:
        df["shot_accuracy"] = (df["SoT"] / df["Shots"]).round(3)

    # Key Pass to Assist Ratio
    if "A" in df.columns and "KeyP" in df.columns:
        df["key_pass_to_assist_ratio"] = (df["A"] / df["KeyP"]).round(3)

    # xG Conversion Rate
    if "G" in df.columns and "xg" in df.columns:
        df["xg_conversion_rate"] = (df["G"] / df["xg"]).round(3)

    # Goals Added Efficiency (Per 90 minutes)
    if "goals_added" in df.columns and "minutes" in df.columns:
        df["goals_added_per_90"] = ((df["goals_added"] / df["minutes"]) * 90).round(3)

    # Expected Assist Efficiency
    if "A" in df.columns and "xa" in df.columns:
        df["xa_conversion_rate"] = (df["A"] / df["xa"]).round(3)

    # Handle NaN or infinite values caused by division by zero
    df = df.fillna(0).replace([float("inf"), -float("inf")], 0)

    return df

def save_to_database(df, table_name):
    """
    Save the given DataFrame to the database.
    Args:
        df (pd.DataFrame): DataFrame to save.
        table_name (str): Table name in the database.
    """
    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Data saved to table: {table_name}")
    except Exception as e:
        print(f"Error saving to table {table_name}: {e}")


def main(): 
    create_player_performance_metrics()
    add_per_90_and_efficiency_metrics()
    print("Data transformation complete!")


if __name__ == "__main__":
    main()
