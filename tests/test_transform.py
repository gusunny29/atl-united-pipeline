import pytest
import pandas as pd
import src.transform as transform

def test_add_per_90_stats():
    # Sample input
    sample_data = {
        "player_id": [1, 2],
        "minutes": [900, 0],
        "xg": [5, 3],
        "xa": [2, 1]
    }
    df = pd.DataFrame(sample_data)

    # Call function
    df_with_per_90 = transform.calculate_per_90_stats(df)

    # Expected output
    expected_data = {
        "player_id": [1, 2],
        "minutes": [900, 0],
        "xg": [5, 3],
        "xa": [2, 1],
        "xg_per_90": [0.5, 0],
        "xa_per_90": [0.2, 0]
    }
    expected_df = pd.DataFrame(expected_data)

    # Assert
    pd.testing.assert_frame_equal(df_with_per_90, expected_df)

def test_add_efficiency_metrics():
    # Sample input
    sample_data = {
        "player_id": [1],
        "G": [10],
        "Shots": [50],
        "SoT": [20],
        "KeyP": [5],
        "A": [3],
        "xg": [8],
        "xa": [2]
    }
    df = pd.DataFrame(sample_data)

    # Call function
    df_with_efficiency = transform.calculate_efficiency_metrics(df)

    # Expected output
    expected_data = {
        "player_id": [1],
        "G": [10],
        "Shots": [50],
        "SoT": [20],
        "KeyP": [5],
        "A": [3],
        "xg": [8],
        "xa": [2],
        "goal_conversion_rate": [0.2],
        "shot_accuracy": [0.4],
        "key_pass_to_assist_ratio": [0.6],
        "xg_conversion_rate": [1.25],
        "xa_conversion_rate": [1.5]
    }
    expected_df = pd.DataFrame(expected_data)

    # Assert
    pd.testing.assert_frame_equal(df_with_efficiency, expected_df)