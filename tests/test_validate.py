import pandas as pd
from src import validate

def base_row(**overrides):
    row = {
        "patient_barcode": "p1",
        "gender": "male",
        "height": 180,
        "weight": 81,
        "race_list": "x",
        "primary_pathology_age_at_initial_pathologic_diagnosis": 55,
        "frequency_of_alcohol_consumption": 2,
        "amount_of_alcohol_consumption_per_day": 3,
        "tobacco_smoking_history": "never",
        "reflux_history": "no",
        "barretts_esophagus": "no",
        "primary_pathology_histological_type": "type",
        "person_neoplasm_cancer_status": "no",
        "vital_status": "alive",
    }
    row.update(overrides)
    return row

def test_validate_computes_bmi_and_category():
    df = pd.DataFrame([base_row(height=200, weight=100)])
    cleaned, rejects = validate.validate(df, write_rejects_path=None)
    assert len(rejects) == 0
    bmi = cleaned.loc[cleaned.index[0], "bmi"]
    assert round(float(bmi), 2) == 25.00
    assert cleaned.loc[cleaned.index[0], "bmi_category"] in {"Normal", "Overweight"}

def test_validate_adds_alcohol_features():
    df = pd.DataFrame([base_row(frequency_of_alcohol_consumption=1, amount_of_alcohol_consumption_per_day=8)])
    cleaned, _ = validate.validate(df, write_rejects_path=None)
    assert cleaned.loc[cleaned.index[0], "total_drinks_per_week"] == 8
    assert cleaned.loc[cleaned.index[0], "alcohol_risk_category"] == "Moderate"

def test_validate_rejects_missing_required_fields_with_reason():
    df = pd.DataFrame([base_row(gender=None)])
    cleaned, rejects = validate.validate(df, write_rejects_path=None)
    assert len(cleaned) == 0
    assert len(rejects) == 1
    assert "Missing fields:" in rejects.loc[rejects.index[0], "reason"]

def test_validate_rejects_invalid_height():
    df = pd.DataFrame([base_row(height=0)])
    cleaned, rejects = validate.validate(df, write_rejects_path=None)
    assert len(cleaned) == 0
    assert "Invalid height value" in rejects.loc[rejects.index[0], "reason"]

def test_validate_numeric_cast_coerces_bad_values():
    df = pd.DataFrame([base_row(height="not_a_number")])
    cleaned, rejects = validate.validate(df, write_rejects_path=None)
    assert len(rejects) == 1
    assert "Missing fields:" in rejects.loc[rejects.index[0], "reason"]