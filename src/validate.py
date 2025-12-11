import pandas as pd
import logging

logger = logging.getLogger("etl.validate")

from rules import (
    REQUIRED_COLUMNS,
    NUMERIC_COLUMNS,
    HEIGHT_MAX, HEIGHT_MIN,
    WEIGHT_MAX, WEIGHT_MIN,
    BMI_NORMAL, BMI_OVERWEIGHT, BMI_UNDERWEIGHT,
    ALCOHOL_RISK_CATEGORIES,
)

def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate the cleaned DataFrame.

    Responsibilities:
    1. Enforce basic column types (numeric columns).
    2. Ensure required fields are not null.
    3. Add a new column for patient BMI.
    4. Optionally apply simple domain rules.
    5. Split into:
       - cleaned_data: rows passing all validation
       - rejects: rows failing validation, with a 'reason' column

    Assumes the input df has already gone through clean.clean().

    Returns:
        cleaned_data (pd.DataFrame)
        rejects (pd.DataFrame)
    """

    df = df.copy()

    # 1. Enforcing numeric types.
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            before_nulls = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            after_nulls = df[col].isna().sum()
            logger.info(f"Numeric cast: {col} - Introduced {after_nulls - before_nulls} nulls")

    # Keeping only required columns that actually exist.
    existing_required = [col for col in REQUIRED_COLUMNS if col in df.columns]

    # If none of the required columns exist, we can't validate meaningfully.
    if not existing_required:
        cleaned_data = df
        rejects = pd.DataFrame(columns=list(df.columns) + ["reason"])
        return cleaned_data, rejects

    # 2. Checking for missing required fields.
    missing_required_mask = df[existing_required].isna().any(axis=1)
    logger.info(f"Rows missing required fields: {missing_required_mask.sum()}")

    # Starting with no domain rule failures.
    domain_fail_mask = pd.Series(False, index=df.index)

    # 3. Adding BMI column if height and weight are present.
    if "height" in df.columns and "weight" in df.columns:
        # Only computing BMI where height > 0 and both height & weight are present
        valid_bmi_mask = (
            df["height"].notna()
            & (df["height"] > 0)
            & df["weight"].notna()
        )

        df["bmi"] = pd.NA  # default
        df.loc[valid_bmi_mask, "bmi"] = (
            df.loc[valid_bmi_mask, "weight"]
            / ((df.loc[valid_bmi_mask, "height"] / 100) ** 2)
        ).round(2)
    else:
        df["bmi"] = pd.NA

    
    # Including a column for BMI categories.
    df["bmi_category"] = pd.NA
    if "bmi" in df.columns:
        valid_bmi = df["bmi"].notna()

        df.loc[valid_bmi & (df["bmi"] < BMI_UNDERWEIGHT), "bmi_category"] = "Underweight"
        df.loc[valid_bmi & (df["bmi"] >= BMI_UNDERWEIGHT) & (df["bmi"] < BMI_NORMAL), "bmi_category"] = "Normal"
        df.loc[valid_bmi & (df["bmi"] >= BMI_NORMAL) & (df["bmi"] < BMI_OVERWEIGHT), "bmi_category"] = "Overweight"
        df.loc[valid_bmi & (df["bmi"] >= BMI_OVERWEIGHT), "bmi_category"] = "Obese"

    # Alcohol consumption derived features.
    if ("frequency_of_alcohol_consumption" in df.columns
        and "amount_of_alcohol_consumption_per_day" in df.columns):

        df["total_drinks_per_week"] = (
            df["frequency_of_alcohol_consumption"] *
            df["amount_of_alcohol_consumption_per_day"]
        )

        # Categorizing alcohol consumption.
        df["alcohol_risk_category"] = pd.NA

        df.loc[df["total_drinks_per_week"] == 0, "alcohol_risk_category"] = "None"
        df.loc[(df["total_drinks_per_week"] > 0) & (df["total_drinks_per_week"] <= 7),
            "alcohol_risk_category"] = "Light"
        df.loc[(df["total_drinks_per_week"] > 7) & (df["total_drinks_per_week"] <= 14),
            "alcohol_risk_category"] = "Moderate"
        df.loc[(df["total_drinks_per_week"] > 14) & (df["total_drinks_per_week"] <= 35),
            "alcohol_risk_category"] = "Heavy"
        df.loc[df["total_drinks_per_week"] > 35, "alcohol_risk_category"] = "Very Heavy"


    # 4. Simple domain rules.
    # Only applied to rows that haven't already failed the missing-required check
    still_candidate_mask = ~missing_required_mask

    if "height" in df.columns:
        invalid_height = (
            still_candidate_mask
            & df["height"].notna()
            & ((df["height"] <= 0) | (df["height"] > 300))
        )
        domain_fail_mask |= invalid_height

    if "weight" in df.columns:
        invalid_weight = (
            still_candidate_mask
            & df["weight"].notna()
            & ((df["weight"] <= 0) | (df["weight"] > 500))
        )
        domain_fail_mask |= invalid_weight

    # Combined mask of all invalid rows.
    invalid_mask = missing_required_mask | domain_fail_mask

    # If everything is valid, just return df and an empty rejects table
    if not invalid_mask.any():
        cleaned_data = df
        rejects = pd.DataFrame(columns=list(df.columns) + ["reason"])
        return cleaned_data, rejects

    # Building detailed reasons for each rejected row.
    reasons = []

    for idx, row in df[invalid_mask].iterrows():
        row_reasons = []

        # Missing required fields for this row
        missing_fields = [col for col in existing_required if pd.isna(row[col])]
        if missing_fields:
            row_reasons.append("Missing fields: " + ", ".join(missing_fields))

        # Domain-specific failures
        if "height" in df.columns and not pd.isna(row.get("height")):
            if row["height"] <= 0 or row["height"] > 300:
                row_reasons.append("Invalid height value")

        if "weight" in df.columns and not pd.isna(row.get("weight")):
            if row["weight"] <= 0 or row["weight"] > 500:
                row_reasons.append("Invalid weight value")

        if not row_reasons:
            row_reasons.append("Failed validation")

        reasons.append("; ".join(row_reasons))

    # Build rejects DataFrame
    rejects = df[invalid_mask].copy()
    rejects["reason"] = reasons

    cleaned_data = df[~invalid_mask].copy()
    rejects.to_json("logs/rejects.json", orient="records", indent=2)

    return cleaned_data, rejects
