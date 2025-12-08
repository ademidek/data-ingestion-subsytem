import pandas as pd
from readers import csv_reader # Extract logic
import clean # Transform logic
import load # Load logic

def main():
    # ETL Pipeline Processes

    # Step 1: Extracting the Data
    data = csv_reader.extract('data/Esophageal_Dataset.csv')
    print("Raw Data:")
    print(data.head())
    print("Raw Shape:", data.shape)
    
    # Step 2: Transforming the extracted data
    cleaned_data, rejects = clean.transform(data)
    print("Cleaned Data:")
    print(cleaned_data.head())
    print("Cleaned Shape:", cleaned_data.shape)

    print("\nRejected Data:")
    print(rejects.head())
    print("Rejects Shape:", rejects.shape)
    
    cleaned_data.to_csv('data/cleaned_esophageal_data.csv', index=False)
    rejects.to_csv('data/rejected_esophageal_data.csv', index=False)

    print("\nCleaned data saved to 'data/cleaned_esophageal_data.csv'")
    print("Rejected data saved to 'data/rejected_esophageal_data.csv'")

if __name__ == "__main__":
    main()