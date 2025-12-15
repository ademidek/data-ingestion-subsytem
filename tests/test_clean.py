import pandas as pd
from src import clean
from src.rules import COLUMN_NULL_THRESHOLD

def test_clean_normalizes_columns_and_trims_strings():
    df = pd.DataFrame({
        " Patient_Barcode ": [" A1 ", "A1 "],
        "Gender": [" Male ", "MALE"],
        "Unnamed: 0": [1, 2],
    })
    out = clean.clean(df)
    assert "patient_barcode" in out.columns
    assert out.loc[out.index[0], "patient_barcode"] == "a1"
    assert out.loc[out.index[0], "gender"] == "male"
    assert "unnamed: 0" not in out.columns

def test_clean_drops_duplicates():
    df = pd.DataFrame({"a": [1, 1], "b": ["x", "x"]})
    out = clean.clean(df)
    assert len(out) == 1

def test_clean_drops_all_null_columns():
    df = pd.DataFrame({"a": [1, 2], "empty": [None, None]})
    out = clean.clean(df)
    assert "empty" not in out.columns

def test_clean_drops_high_null_columns():
    df = pd.DataFrame({"keep": [1, 2, 3, 4, 5], "mostly_null": [None, None, None, None, 5]})
    out = clean.clean(df)
    if (4/5)*100 > COLUMN_NULL_THRESHOLD:
        assert "mostly_null" not in out.columns
