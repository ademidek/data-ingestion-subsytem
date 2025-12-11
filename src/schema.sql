-- Dropping exisiting staging tables if they exist
DROP TABLE IF EXISTS stg_esophageal;
DROP TABLE IF EXISTS stg_rejects;

-- Main staging table for valid records
CREATE TABLE IF NOT EXISTS stg_esophageal (
    patient_barcode TEXT PRIMARY KEY,
    gender TEXT,
    height NUMERIC(5,2),
    weight NUMERIC(5,2),
    age_at_diagnosis NUMERIC(10,0),
    race TEXT,
    cancer_status TEXT,
    vital_status TEXT,
    smoking_history TEXT,
    reflux_history TEXT,
    barretts_esophagus TEXT,
    pathology_histological_type TEXT,
    total_drinks_per_week NUMERIC(10,0),
    alcohol_risk_category TEXT,
    bmi NUMERIC(5,2),
    bmi_category TEXT,
    _loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Rejects table for invalid rows
CREATE TABLE IF NOT EXISTS stg_rejects (
    patient_barcode TEXT PRIMARY KEY,
    gender TEXT,
    height NUMERIC(5,2),
    weight NUMERIC(5,2),
    age_at_diagnosis NUMERIC(10,0),
    race TEXT,
    cancer_status TEXT,
    vital_status TEXT,
    smoking_history TEXT,
    reflux_history TEXT,
    barretts_esophagus TEXT,
    pathology_histological_type TEXT,
    total_drinks_per_week NUMERIC(10,0),
    alcohol_risk_category TEXT,
    bmi NUMERIC(5,2),
    bmi_category TEXT,
    reason TEXT,
    _loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);
