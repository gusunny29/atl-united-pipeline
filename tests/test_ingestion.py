import pytest
import pandas as pd
import src.ingestion as ingestion


def test_preprocess_data():
    # Input data
    sample_data = {
        "Team": ["ATL", "TOR", "NYC", ""],
        "TotalGuar": ["100000", "200000", "150000", "0"],
        "N": [1, 2, 3, None]
    }
    df = pd.DataFrame(sample_data)

    # Rename mapping
    rename_mapping = {
        "Team": "team",
        "TotalGuar": "total_guaranteed",
        "N": "num_players"
    }

    # Expected output
    expected_data = {
        "team": ["ATL", "TOR", "NYC", ""],
        "total_guaranteed": [100000.0, 200000.0, 150000.0, 0.0], 
        "num_players": [1.0, 2.0, 3.0, 0.0]  
    }
    expected_df = pd.DataFrame(expected_data)

    # Call the preprocessing function with test DataFrame
    processed_df = ingestion.preprocess_data(
        rename_mapping=rename_mapping,
        format_currency_columns=["TotalGuar"],
        test_df=df
    )


    # Convert column to float to ensure a match with object
    processed_df["total_guaranteed"] = processed_df["total_guaranteed"].astype(float)
    processed_df["num_players"] = processed_df["num_players"].astype(float)

    # Assert the processed dataframe matches the expected output
    pd.testing.assert_frame_equal(processed_df, expected_df, check_dtype=True)

