import pandas as pd
from pathlib import Path

def extract(path: str):
    """
    Extracting data from a CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    input_path = Path(path)
    df = pd.read_csv(input_path)
    print("Looking for file at:", input_path.resolve())
    return df