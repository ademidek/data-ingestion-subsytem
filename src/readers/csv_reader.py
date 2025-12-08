import pandas as pd
from pathlib import Path

def extract(path: str):
    """
    Extracting data from a CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    input_path = Path(path)

    print("Looking for file at:", input_path.resolve())

    if not input_path.exists():
        raise FileNotFoundError(f"The file {input_path.resolve()} could not be found.")
    
    try:
        df = pd.read_csv(input_path)
        print(f"Successfully read {len(df)} rows and {len(df.columns)} columns from {input_path.name}")
        return df
    except pd.errors.EmptyDataError:
        raise ValueError(f"The file {input_path.resolve()} is empty.")
    except Exception as e:
        raise RuntimeError(f"An error occurred while reading the CSV file: {e}")