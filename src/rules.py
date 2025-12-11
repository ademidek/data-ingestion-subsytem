# Threshold percentage of nulls allowed in a column before it is dropped.
COLUMN_NULL_THRESHOLD = 70.0

PLACEHOLDER_COLUMNS = ["unnamed: 0", "index"]

# Columns that should hold numeric values.
NUMERIC_COLUMNS = [
    'height',
    'weight',
    'total_drinks_per_week',
    'bmi',
]

# Columns that must be present and non-null for a row to be valid.
# Several columns were dropped for clarity and abstracted domain relevance.
REQUIRED_COLUMNS = [
    'patient_barcode',
    'gender',
    'height',
    'weight',
    'race_list',
    'primary_pathology_age_at_initial_pathologic_diagnosis',
    'frequency_of_alcohol_consumption',
    'amount_of_alcohol_consumption_per_day',
    'tobacco_smoking_history',
    'reflux_history',
    'barretts_esophagus',
    'primary_pathology_histological_type',
    'person_neoplasm_cancer_status',
    'vital_status',
]

# Height and Weight constraints.
HEIGHT_MIN, HEIGHT_MAX = 0, 300 # height in cm
WEIGHT_MIN, WEIGHT_MAX = 0, 500 # weight in kg

#BMI category thresholds.
BMI_UNDERWEIGHT = 18.0
BMI_NORMAL = 25.0
BMI_OVERWEIGHT = 30.0

# Alcohol risk categories based on weekly consumption.
ALCOHOL_RISK_CATEGORIES = {
    (0, 0): "None",
    (1, 7): "Low",
    (7, 14): "Moderate",
    (14, 35): "High",
    (15, float('inf')): "Excessive",
}

# Outlining the expected columns in the Esophageal dataset, so as not to upsert unexpected columns.
ESOPHAGEAL_COLUMNS = [
    'patient_barcode',
    'gender',
    'height',
    'weight',
    'bmi',
    'bmi_category',
    'race',
    'total_drinks_per_week',
    'alcohol_risk_category',
    'smoking_history',
    'reflux_history',
    'barretts_esophagus',
    'age_at_diagnosis',
    'cancer_status',
    'vital_status',
    'pathology_histological_type',
]

REJECT_COLUMNS = ESOPHAGEAL_COLUMNS + ['reason']