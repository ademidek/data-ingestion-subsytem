import pandas as pd

def extract(path: str):
    """
    Extracting data from a CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    df = pd.read_csv(path)
    return df