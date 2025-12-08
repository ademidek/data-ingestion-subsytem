import pandas as pd
from readers import csv_reader # Extraction logic
import clean # Cleaning logic
import validate # Validation logic

def main():
    # Step 1: Extracting the data
    data = csv_reader.extract("data/Esophageal_Dataset.csv")
    print("Raw Data:")
    print(data.head())
    print("Raw Shape:", data.shape)

    # Step 2: Cleaning the extracted data (generic cleaning)
    cleaned_raw = clean.clean(data)
    print("\nAfter generic cleaning:")
    print(cleaned_raw.head())
    print("Cleaned Raw Shape:", cleaned_raw.shape)

    # Step 3: Validating the cleaned data (types, required fields, domain rules)
    cleaned_data, rejects = validate.validate(cleaned_raw)
    print("\nValidated Cleaned Data:")
    print(cleaned_data.head())
    print("Validated Cleaned Shape:", cleaned_data.shape)

    print("\nRejected Data:")
    print(rejects.head())
    print("Rejects Shape:", rejects.shape)

    # For now, save results to CSV
    cleaned_data.to_csv("data/cleaned_esophageal_data.csv", index=False)
    rejects.to_csv("data/rejected_esophageal_data.csv", index=False)

    print("\nCleaned data saved to 'data/cleaned_esophageal_data.csv'")
    print("Rejected data saved to 'data/rejected_esophageal_data.csv'")

if __name__ == "__main__":
    main()
