import pandas as pd

from .readers import csv_reader # Extraction logic
from . import clean # Cleaning logic
from . import validate # Validation logic
from . import load # Loading logic
from .rules import ESOPHAGEAL_COLUMNS, REJECT_COLUMNS
from . import schema_init
from .logging_config import setup_logging

logger = setup_logging()

def main():
    '''
    ETL Pipeline for Esophageal Dataset

    Steps:
    1. Extract data from CSV file.
    2. Clean the extracted data (generic cleaning).
    3. Validate the cleaned data (types, required fields, domain rules).
    4. Load the cleaned data into Postgres.
    5. Log summary statistics at each step.
    6. Save cleaned data and rejects to CSV files.
    7. Print progress and summary information to console.

    '''
    # Creating the schema
    schema_init.run_schema()

    # Step 1: Extracting the data
    data = csv_reader.extract("data/Esophageal_Dataset.csv")
    #logger.info("Raw Data:")
    #logger.info(data.head())
    logger.info(f"Raw Shape: {data.shape}")

    # Step 2: Cleaning the extracted data (generic cleaning)
    cleaned_raw = clean.clean(data)
    logger.info("\nAfter generic cleaning:")
    #logger.info(cleaned_raw.head())
    logger.info(f"Cleaned Raw Shape: {cleaned_raw.shape}")

    # Step 3: Validating the cleaned data (types, required fields, domain rules)
    cleaned_data, rejects = validate.validate(cleaned_raw)
    logger.info("\nValidated Cleaned Data:")
    #logger.info(cleaned_data.head())
    logger.info(f"Validated Cleaned Shape: {cleaned_data.shape}")

    logger.info("\nRejected Data:")
    logger.info(rejects.head())
    logger.info(f"Rejects Shape: {rejects.shape}")

    # Renaming columns for final database schema
    column_mapping = {
        "race_list": "race",
        "person_neoplasm_cancer_status": "cancer_status",
        "tobacco_smoking_history": "smoking_history",
        "primary_pathology_histological_type": "pathology_histological_type",
        "primary_pathology_age_at_initial_pathologic_diagnosis": "age_at_diagnosis",
    }
    cleaned_data = cleaned_data.rename(columns=column_mapping)
    rejects = rejects.rename(columns=column_mapping)

    logger.info("Columns after validate + rename: %s", list(cleaned_data.columns))

    # Ensuring that the only columns used from our DataFrame are those that exist in our table.
    cleaned_filtered = cleaned_data[ESOPHAGEAL_COLUMNS].copy()
    rejects_filtered = rejects[REJECT_COLUMNS].copy()

    logger.info("Columns going into Postgres: %s", list(cleaned_filtered.columns))
    logger.info("Number of rows going into Postgres: %d", len(cleaned_filtered))

    # Save results to CSV for comparison/audit purposes.
    cleaned_filtered.to_csv("data/cleaned_esophageal_data.csv", index=False)
    logger.info("Cleaned data saved to 'data/cleaned_esophageal_data.csv'")
    rejects_filtered.to_csv("data/rejected_esophageal_data.csv", index=False)
    logger.info("Rejected data saved to 'data/rejected_esophageal_data.csv'")

    # Step 4: Loading the cleaned data into Postgres.
    load.upsert_dataframe(
        cleaned_filtered,
        table_name="stg_esophageal",
        pk_columns=["patient_barcode"]
    )
    logger.info("Cleaned data loaded into 'stg_esophageal' table.")

    load.upsert_dataframe(
        rejects_filtered,
        table_name="stg_rejects",
        pk_columns=["patient_barcode"]
    )
    logger.info("Rejected data loaded into 'stg_rejects' table.")

    logger.info("Rows loaded: %d", len(cleaned_filtered))
    logger.info("Rows rejected: %d", len(rejects))

    logger.info("\nSuccessfully completed the ETL process.")

if __name__ == "__main__":
    main()
