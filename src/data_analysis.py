import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

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

def plot_four_quadrant_goals_added_vs_xg(output_folder="output", label_percentile = 0.97):
    """
    Plot a four-quadrant scatter plot of goals_added vs. xg and save it to the output folder.
    """
    try:
        print("Fetching player performance metrics data...")
        # Fetch the player performance metrics table
        query = "SELECT player, team, goals_added, xg FROM player_performance_metrics"
        df = pd.read_sql(query, engine)

        # Filter out rows with missing or zero values for goals_added and xg
        df = df[(df["goals_added"] != 0) & (df["xg"] != 0)]

        # Calculate the means of goals_added and xg to determine quadrant boundaries
        xg_mean = df["xg"].mean()
        goals_added_mean = df["goals_added"].mean()

        # Calculate thresholds for standout players (e.g., 95th percentile)
        goals_added_threshold = df["goals_added"].quantile(label_percentile)
        xg_threshold = df["xg"].quantile(label_percentile)
        neg_goals_added_threshold = df["goals_added"].quantile(1 - label_percentile)
        neg_xg_threshold = df["xg"].quantile(1 - label_percentile)

        print("Creating four-quadrant scatter plot...")
        plt.figure(figsize=(12, 8))

        # Scatter plot with team-based color coding
        sns.scatterplot(data=df, x="xg", y="goals_added", hue="team", palette="tab10", s=100, legend = False)

        # Draw vertical and horizontal lines for the quadrants
        plt.axvline(x=xg_mean, color="black", linestyle="--", linewidth=1, alpha=0.7, label="xG Mean")
        plt.axhline(y=goals_added_mean, color="black", linestyle="--", linewidth=1, alpha=0.7, label="Goals Added Mean")

        for i, row in df.iterrows():
            if row["goals_added"] > goals_added_threshold or row["xg"] > xg_threshold:
                plt.text(row["xg"], row["goals_added"], row["player"], fontsize=7, alpha=0.7)
            elif row["goals_added"] < neg_goals_added_threshold or row["xg"] < neg_xg_threshold:  # Optionally label negative outliers
                plt.text(row["xg"], row["goals_added"], row["player"], fontsize=7, alpha=0.7)


        # Add titles, labels, and legend
        plt.title("Four-Quadrant Analysis: Goals Added vs. Expected Goals (xG)", fontsize=16)
        plt.xlabel("Expected Goals (xG)", fontsize=14)
        plt.ylabel("Goals Added", fontsize=14)
        plt.grid(True, alpha=0.3)

        # Save the plot to the output folder
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, "four_quadrant_goals_added_vs_xg.png")
    
        plt.savefig(output_file, dpi=300)
        plt.close()
        print(f"Four-quadrant plot saved to {output_file}.\n")
    except Exception as e:
        print(f"Error creating four-quadrant plot: {e}")


def plot_top_players(output_folder="output", top_n=20):
    """
    Fetch and plot the top players for xG and goals_added.
    
    Args:
        output_folder (str): Folder to save the plots.
        top_n (int): Number of top players to fetch and plot for each metric.
    """
    try:
        print("Fetching player performance metrics data...")
        # Query the database
        query = "SELECT player, team, goals_added, xg FROM player_performance_metrics"
        df = pd.read_sql(query, engine)

        # Filter out rows with missing or zero values
        df = df[(df["goals_added"] != 0) & (df["xg"] != 0)]

        # Get top players for goals_added
        top_goals_added = df.sort_values(by="goals_added", ascending=False).head(top_n)

        # Get top players for xG
        top_xg = df.sort_values(by="xg", ascending=False).head(top_n)

        print("Creating plots...")
        os.makedirs(output_folder, exist_ok=True)

        # Plot top players by goals_added
        plt.figure(figsize=(12, 6))
        sns.barplot(data=top_goals_added, x="goals_added", y="player",  hue="player",palette="Blues_d")
        plt.title(f"Top {top_n} Players by Goals Added", fontsize=16)
        plt.xlabel("Goals Added", fontsize=14)
        plt.ylabel("Player", fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(output_folder, "top_goals_added_players.png"), dpi=300)
        plt.close()
        print(f"Top goals added players plot saved to {output_folder}.")
        # Plot top players by xG
        plt.figure(figsize=(12, 6))
        sns.barplot(data=top_xg, x="xg", y="player",  hue="player", palette="Greens_d")
        plt.title(f"Top {top_n} Players by Expected Goals (xG)", fontsize=16)
        plt.xlabel("Expected Goals (xG)", fontsize=14)
        plt.ylabel("Player", fontsize=14)
        plt.tight_layout()
        plt.savefig(os.path.join(output_folder, "top_xg_players.png"), dpi=300)
        plt.close()

        print(f"xGoals saved to {output_folder}.")
    except Exception as e:
        print(f"Error creating plots: {e}")   


def calculate_team_points():
    """
    Calculate the total points for each team from the xGoals_games table.
    Points:
        - 3 points for a win
        - 1 point for a tie
        - 0 points for a loss
    """
    # Fetch the xGoals_games table
    query = """
    SELECT home_team, home_goals, away_team, away_goals
    FROM xgoals_games
    """
    games_df = pd.read_sql(query, engine)

    # Calculate points for home teams
    games_df["home_points"] = games_df.apply(
        lambda row: 3 if row["home_goals"] > row["away_goals"] else (1 if row["home_goals"] == row["away_goals"] else 0),
        axis=1
    )

    # Calculate points for away teams
    games_df["away_points"] = games_df.apply(
        lambda row: 3 if row["away_goals"] > row["home_goals"] else (1 if row["away_goals"] == row["home_goals"] else 0),
        axis=1
    )

    # Group by home and away teams to sum up points
    home_points = games_df.groupby("home_team")["home_points"].sum().reset_index(name="home_points")
    away_points = games_df.groupby("away_team")["away_points"].sum().reset_index(name="away_points")

    # Merge home and away points into a single DataFrame
    total_points = pd.merge(
        home_points, away_points, left_on="home_team", right_on="away_team", how="outer"
    ).fillna(0)

    # Calculate total points
    total_points["total_points"] = total_points["home_points"] + total_points["away_points"]

    # Rename columns for clarity and drop redundant column
    total_points = total_points.rename(columns={"home_team": "team"}).drop(columns=["away_team"])

    # Sort by total_points for better readability
    total_points = total_points.sort_values(by="total_points", ascending=False).reset_index(drop=True)

    return total_points


def compare_points_and_salaries():
    """
    Compare team points with their total salaries.
    """
    # Calculate team points
    total_points = calculate_team_points()

    # Fetch salaries data
    query = "SELECT team, total_guaranteed FROM salaries"
    salaries_df = pd.read_sql(query, engine)

    # Merge points and salaries data
    comparison_df = pd.merge(total_points, salaries_df, on="team", how="inner")

    # Convert salaries to millions for better readability
    comparison_df["total_guaranteed"] = comparison_df["total_guaranteed"] / 1e6

    # Plot points vs salaries
    plt.figure(figsize=(12, 8))
    plt.scatter(comparison_df["total_guaranteed"], comparison_df["total_points"], color="blue", alpha=0.7)
    plt.title("Team Points vs Total Salaries", fontsize=16)
    plt.xlabel("Total Salaries (in millions)", fontsize=14)
    plt.ylabel("Total Points", fontsize=14)
    plt.grid(True, alpha=0.3)

    # Annotate team names
    for _, row in comparison_df.iterrows():
        plt.text(row["total_guaranteed"], row["total_points"], row["team"], fontsize=10, alpha=0.7)

    # Save the plot
    output_path = "output/points_vs_salaries.png"
    os.makedirs("output", exist_ok=True)
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Plot saved to {output_path}.")

def main():
    plot_four_quadrant_goals_added_vs_xg()
    plot_top_players() 
    compare_points_and_salaries()


if __name__ == "__main__":
    main()