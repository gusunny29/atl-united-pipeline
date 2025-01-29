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

def get_atlanta_united_players():
    """
    Fetch and filter Atlanta United players' data from the player_performance_metrics table.
    """
    try:
        query = """
            SELECT player, team, xg, xa, goals_added, dribbling, shooting, minutes
            FROM player_performance_metrics
            WHERE team = 'ATL'
        """
        atl_df = pd.read_sql(query, engine)

        # Ensure no NaN values in key metrics
        atl_df = atl_df.fillna(0)

        print(f"Retrieved {len(atl_df)} players for Atlanta United.")
        return atl_df
    except Exception as e:
        print(f"Error fetching Atlanta United player data: {e}")
        return pd.DataFrame()

def analyze_impact(atl_df):
    """
    Analyze and rank all Atlanta United players based on key metrics.
    """
    try:
        # Add an overall impact score (you can adjust weights based on importance)
        atl_df["impact_score"] = (
            atl_df["xg"] * 0.4 +     # Weight 40% for xG
            atl_df["xa"] * 0.3 +     # Weight 30% for xA
            atl_df["goals_added"] * 0.3  # Weight 30% for Goals Added
        )

        # Sort by impact_score (descending)
        atl_df = atl_df.sort_values(by="impact_score", ascending=False)

        return atl_df
    except Exception as e:
        print(f"Error analyzing impact metrics: {e}")
        return atl_df

def plot_minutes_vs_impact(atl_df):
    """
    Plot a scatter plot of minutes played vs. impact_score for Atlanta United players.
    """
    try:
        plt.figure(figsize=(12, 8))

        # Scatter plot
        sns.scatterplot(
            data=atl_df,
            x="minutes",
            y="impact_score",
            size="impact_score",
            sizes=(50, 300),
            hue="player",
            palette="viridis",
            legend=False
        )

        # Add labels for players
        for i, row in atl_df.iterrows():
            plt.text(
                row["minutes"],
                row["impact_score"],
                row["player"],
                fontsize=9,
                ha="center",
                va="bottom"
            )

        # Titles and labels
        plt.title("Minutes Played vs. Impact Score (Atlanta United Players)", fontsize=16)
        plt.xlabel("Minutes Played", fontsize=14)
        plt.ylabel("Impact Score", fontsize=14)
        plt.grid(alpha=0.3)
        plt.tight_layout()

        # Save the plot to the output folder
        os.makedirs("output", exist_ok=True)
        output_path = "output/minutes_vs_impact_score.png"
        plt.savefig(output_path, dpi=300)
        plt.close()

        print(f"Scatter plot saved to {output_path}")
    except Exception as e:
        print(f"Error creating scatter plot: {e}")

def main():
    # Get the list of all Atlanta United players
    atl_df = get_atlanta_united_players()
    # Analyze and rank players numerically
    atl_df = analyze_impact(atl_df)
    # Plot scatter plot with minutes played vs. impact score
    plot_minutes_vs_impact(atl_df)

if __name__ == "__main__":
    main()
