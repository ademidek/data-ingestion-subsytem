import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger("etl.validate")

from .rules import (
    REQUIRED_COLUMNS,
    NUMERIC_COLUMNS,
    BMI_NORMAL, BMI_OVERWEIGHT, BMI_UNDERWEIGHT,
)

def cast_numeric(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in numeric_cols:
        if col in df.columns:
            before_nulls = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            after_nulls = df[col].isna().sum()
            logger.info(f"Numeric cast: {col} - Introduced {after_nulls - before_nulls} nulls")
    return df

def add_bmi(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "height" in df.columns and "weight" in df.columns:
        valid_bmi_mask = df["height"].notna() & (df["height"] > 0) & df["weight"].notna()
        df["bmi"] = pd.NA
        df.loc[valid_bmi_mask, "bmi"] = (
            df.loc[valid_bmi_mask, "weight"] / ((df.loc[valid_bmi_mask, "height"] / 100) ** 2)
        ).round(2)
    else:
        df["bmi"] = pd.NA
    return df

def add_bmi_category(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["bmi_category"] = pd.NA
    valid = df["bmi"].notna() if "bmi" in df.columns else pd.Series(False, index=df.index)

    df.loc[valid & (df["bmi"] < BMI_UNDERWEIGHT), "bmi_category"] = "Underweight"
    df.loc[valid & (df["bmi"] >= BMI_UNDERWEIGHT) & (df["bmi"] < BMI_NORMAL), "bmi_category"] = "Normal"
    df.loc[valid & (df["bmi"] >= BMI_NORMAL) & (df["bmi"] < BMI_OVERWEIGHT), "bmi_category"] = "Overweight"
    df.loc[valid & (df["bmi"] >= BMI_OVERWEIGHT), "bmi_category"] = "Obese"
    return df

def add_alcohol_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if ("frequency_of_alcohol_consumption" in df.columns
        and "amount_of_alcohol_consumption_per_day" in df.columns):
        df["total_drinks_per_week"] = (
            df["frequency_of_alcohol_consumption"] * df["amount_of_alcohol_consumption_per_day"]
        )

        df["alcohol_risk_category"] = pd.NA
        df.loc[df["total_drinks_per_week"] == 0, "alcohol_risk_category"] = "None"
        df.loc[(df["total_drinks_per_week"] > 0) & (df["total_drinks_per_week"] <= 7), "alcohol_risk_category"] = "Light"
        df.loc[(df["total_drinks_per_week"] > 7) & (df["total_drinks_per_week"] <= 14), "alcohol_risk_category"] = "Moderate"
        df.loc[(df["total_drinks_per_week"] > 14) & (df["total_drinks_per_week"] <= 35), "alcohol_risk_category"] = "Heavy"
        df.loc[df["total_drinks_per_week"] > 35, "alcohol_risk_category"] = "Very Heavy"
    return df

def build_invalid_mask(df: pd.DataFrame, required_cols: list[str]) -> tuple[pd.Series, list[str]]:
    existing_required = [c for c in required_cols if c in df.columns]
    if not existing_required:
        return pd.Series(False, index=df.index), []

    missing_required_mask = df[existing_required].isna().any(axis=1)

    domain_fail_mask = pd.Series(False, index=df.index)
    still_candidate = ~missing_required_mask

    if "height" in df.columns:
        domain_fail_mask |= (still_candidate & df["height"].notna() & ((df["height"] <= 0) | (df["height"] > 300)))
    if "weight" in df.columns:
        domain_fail_mask |= (still_candidate & df["weight"].notna() & ((df["weight"] <= 0) | (df["weight"] > 500)))

    return (missing_required_mask | domain_fail_mask), existing_required

def build_reject_reasons(df: pd.DataFrame, invalid_mask: pd.Series, existing_required: list[str]) -> list[str]:
    reasons = []
    for _, row in df[invalid_mask].iterrows():
        row_reasons = []
        missing_fields = [c for c in existing_required if pd.isna(row[c])]
        if missing_fields:
            row_reasons.append("Missing fields: " + ", ".join(missing_fields))
        if "height" in df.columns and not pd.isna(row.get("height")) and (row["height"] <= 0 or row["height"] > 300):
            row_reasons.append("Invalid height value")
        if "weight" in df.columns and not pd.isna(row.get("weight")) and (row["weight"] <= 0 or row["weight"] > 500):
            row_reasons.append("Invalid weight value")
        if not row_reasons:
            row_reasons.append("Failed validation")
        reasons.append("; ".join(row_reasons))
    return reasons

def validate(df: pd.DataFrame, *, write_rejects_path: Path | None = Path("logs/rejects.json")):
    df = df.copy()
    df = cast_numeric(df, NUMERIC_COLUMNS)
    df = add_bmi(df)
    df = add_bmi_category(df)
    df = add_alcohol_features(df)

    invalid_mask, existing_required = build_invalid_mask(df, REQUIRED_COLUMNS)

    if not invalid_mask.any():
        return df, pd.DataFrame(columns=list(df.columns) + ["reason"])

    rejects = df[invalid_mask].copy()
    rejects["reason"] = build_reject_reasons(df, invalid_mask, existing_required)
    cleaned = df[~invalid_mask].copy()

    if write_rejects_path is not None:
        write_rejects_path.parent.mkdir(parents=True, exist_ok=True)
        rejects.to_json(write_rejects_path, orient="records", indent=2)

    return cleaned, rejects
