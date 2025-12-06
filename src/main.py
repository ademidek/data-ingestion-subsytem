import pandas as pd
from readers import csv_reader # Extract logic
import clean # Transform logic
import load # Load logic

def main():
    # ETL Pipeline Processes

    # Step 1: Extracting the Data
    data = csv_reader.extract('data/Esophageal_Dataset.csv')
    print(data.head())
    
    # Step 2: Transforming the extracted data
    
    # Step 3: Loading the transformed data into the database

if __name__ == "__main__":
    main()