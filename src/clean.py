import pandas as pd
from .rules import PLACEHOLDER_COLUMNS, COLUMN_NULL_THRESHOLD
import logging

logger = logging.getLogger("etl.clean")

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform generic, schema-agnostic cleaning on the input DataFrame.

    Steps:
    1. Work on a copy of the original DataFrame and normalize column names.
    2. Trim leading/trailing whitespace from all string columns.
    3. Remove duplicate rows.
    4. Drop columns that are completely empty.
    5. Drop placeholder/index columns like 'Unnamed: 0' or 'index' if present.
    6. Normalize blank strings to null values (NaN).
    7. Dropping columns that are missing more than a threshold percentage of their values.

    NOTE: This function does NOT decide which rows are valid vs rejected.
          That is handled by validation logic in validate.py.

    Returns:
        pd.DataFrame: cleaned DataFrame (same number of rows unless duplicates removed).
    """
    logger.info("Starting data cleaning process.")

    # 1. Creates a copy of our DatFrame to avoid mutating the original data.
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # 2. Trims whitespace from string/object columns.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.lower()

    # 3. Removes duplicate rows.
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    logger.info(f"Removed {before - after} duplicate rows")

    # 4. Removes all columns that are completely empty.
    df = df.dropna(axis=1, how="all")

    # 5. Removes placeholder/index columns if present.
    for col in PLACEHOLDER_COLUMNS:
        if col in df.columns:
            df = df.drop(columns=[col])

    # 6. Normalizes blank strings / whitespace-only values to NaN.
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # 7. Dropping columns that are missing more than a null threshold of their values.
    null_pct = df.isnull().mean() * 100
    cols_to_drop = null_pct[null_pct > COLUMN_NULL_THRESHOLD].index
    if len(cols_to_drop) > 0:
        logger.info(f"Dropping {len(cols_to_drop)} columns for > {COLUMN_NULL_THRESHOLD}% nulls: {list(cols_to_drop)}")

    df = df.drop(columns=cols_to_drop, errors="ignore")
    
    logger.info(f"Cleaning finished. Final shape: {df.shape}")

    return df