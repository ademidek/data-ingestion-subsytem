import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger("etl.extract")

def extract(path: str):
    """
    Extracting data from a CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    input_path = Path(path)

    logger.info("Looking for file at: %s", input_path.resolve())

    if not input_path.exists():
        raise FileNotFoundError(f"The file {input_path.resolve()} could not be found.")
    
    try:
        df = pd.read_csv(input_path, low_memory=False)
        logger.info("Successfully read %d rows and %d columns from %s", len(df), len(df.columns), input_path.name)
        return df
    except pd.errors.EmptyDataError:
        raise ValueError(f"The file {input_path.resolve()} is empty.")
    except Exception as e:
        raise RuntimeError(f"An error occurred while reading the CSV file: {e}")