import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the input DataFrame by:
    1. Making a copy of the original DataFrame.
    2. Removing duplicate rows.
    3. Dropping columns that are completely empty.
    4. Normalizing blank string to null values (NaN).
    5. Enforcing basic types.
    6. Defines required collumns.
    7. Splits the data into:
        - Cleaned Data with all required fields present.
        - A 'rejects' table containing all rows missing at least one required field

    Returns:
        cleaned_data (pd.DataFrame): DataFrame containing cleaned data with all required fields.
        rejects (pd.DataFrame): DataFrame containing rows with missing required fields.
    """

    # 1. Creates a copy of our DataFrame, allowing us to clean it without modifying the originial.
    df = df.copy()

    # 2. Removes duplicate rows
    df = df.drop_duplicates()

    # 3. Removes all columns that are completely empty, as well as placeholder columns
    df = df.dropna(axis = 1, how = 'all')

    # Removes the placeholder/index columns.
    for col in ["Unnamed: 0", "index"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # 4. Normalizes blank string to null values (NaN)
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    # 5. Enforces basic types
    numeric_columns = ['height', 'weight']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 6. Defines required columns in our dataset
    required_columns = [
        'patient_id', 
        'gender', 
        'height', 
        'weight', 
        'race_list', 
        'person_neoplasm_cancer_status', 
        'vital_status', 
        'tobacco_smoking_history', 
        'alcohol_history_documented', 
        'reflux_history', 
        'barretts_esophagus', 
        'primary_pathology_histological_type',
        ]

    # Keeping only required columns that exist in our DataFrame.
    has_required_columns = [col for col in required_columns if col in df.columns]

    # Treating the case where none of the required columns exist in the DataFrame.
    if not has_required_columns:
        cleaned_data = df
        rejects = pd.DataFrame(columns=list(df.columns) + ['reason'])
        return cleaned_data, rejects
    
    # A mask for the rows that are missing at least one required field.
    missing_required_mask = df[has_required_columns].isna().any(axis=1)

    # If no rows are missing required fields, assume all data is clean.
    if not missing_required_mask.any():
        cleaned_data = df
        rejects = pd.DataFrame(columns=list(df.columns) + ['reason'])
        return cleaned_data, rejects

    # Creates a rejects table for rows with missing required fields
    missing_df = df[has_required_columns].isna()
    reasons = []

    for i, row in missing_df[missing_required_mask].iterrows():
        missing_fields = [col for col, is_missing in row.items() if is_missing]
        if missing_fields:
            reasons.append(f"Missing fields: {', '.join(missing_fields)}")
        else:
            reasons.append("Failed validation")

    # 7. Splits the data into cleaned data and rejects.

    rejects = df[missing_required_mask].copy()
    rejects['reason'] = reasons

    cleaned_data = df[~missing_required_mask].copy()

    return cleaned_data, rejects
