import pandas as pd

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform generic, schema-agnostic cleaning on the input DataFrame.

    Steps:
    1. Work on a copy of the original DataFrame.
    2. Remove duplicate rows.
    3. Drop columns that are completely empty.
    4. Drop placeholder/index columns like 'Unnamed: 0' or 'index' if present.
    5. Normalize blank strings to null values (NaN).
    6. Trim leading/trailing whitespace from all string columns.

    NOTE: This function does NOT decide which rows are valid vs rejected.
          That is handled by validation logic in validate.py.

    Returns:
        pd.DataFrame: cleaned DataFrame (same number of rows unless duplicates removed).
    """

    # 1. Creates a copy of our DatFrame to avoid mutating the original data.
    df = df.copy()

    # 2. Removes duplicate rows.
    df = df.drop_duplicates()

    # 3. Removes all columns that are completely empty.
    df = df.dropna(axis=1, how="all")

    # 4. Removes placeholder/index columns if present.
    for col in ["Unnamed: 0", "index"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # 5. Normalizes blank strings / whitespace-only values to NaN.
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # 6. Trims whitespace from string/object columns.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    return df
