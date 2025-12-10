# The Data Ingestion Subsystem 🔌

## Application Overview 📝
This application is a **Data Ingestion Subsystem**, designed to collect and organize data from different sources into a unified, structured environment. It is a foundational component of modern data engineering pipelines, responsible for ensuring data is **accurate, clean, and reliable** before downstream analytics or machine learning use.

This subsystem uses **Python** and **PostgreSQL** to read (extract), validate, clean (transform), and load data for later use in analytics or warehousing, similar to other ETL pipelines.

The goal of this application is to analyze and visualize potential correlations between esophageal cancer and several key risk factors, including smoking history, alcohol intake, and body mass index (BMI). By building a structured ETL pipeline, the project transforms raw medical datasets into clean, validated, and analysis-ready information.

In addition to lifestyle factors, the application also explores the clinical progression often associated with esophageal cancer. This includes examining the relationship between acid reflux, Barrett’s Esophagus, and other intermediary conditions that may increase the likelihood of developing esophageal cancer. Through data cleaning, validation, and intuitive visualizations, the system aims to highlight meaningful patterns that could support risk assessment, research, or early-warning insights.

---

## Application Goals 🥅

This project was designed with two goals in mind:

1. Educational — To demonstrate how medical datasets can be ingested, validated, cleaned, and transformed through a modular ETL pipeline. This includes applying data quality rules, building reusable components, and producing structured outputs suitable for analysis.

2. Analytical — To investigate how behavioral and clinical factors—such as smoking history, alcohol consumption, BMI, reflux, and Barrett’s Esophagus—may correlate with the development of esophageal cancer. The project helps surface meaningful trends and relationships through exploratory analysis and visualization.

## Project Structure
```
data_ingestion_pipeline/
├── src/
│   ├── config.py
│   ├── readers/
│   │   ├── csv_reader.py
│   │   ├── json_reader.py
│   │   └── api_reader.py
│   ├── validate.py
│   ├── clean.py
│   ├── load.py
│   ├── rules.py
│   └── main.py
│
├── config/
│   └── sources.yml
│
├── data/
│   ├── Esophageal_Dataset.csv
│
├── tests/
│   ├── test_validate.py
│   └── test_load.py
│
├── requirements.txt
└── README.md
```

Personal notes before finishing:

-list the technologies im using (the tech stack), mention logging
-add some steps to so someone can locally run and test the program, whatever necessary commands
-try and clone the project and see whatever steps you would need to run
-try and test the validation and cleaning logic, anything transformation
-enriching the data is part of transformation, try and refactor the code to enrich the data separate from the validation
-one test could be testing to see if the trigger for adding a column works by computing BMI where height > 0 and both height & weight are present
-modularize my code more, break things down into functions so they can be tested.
-shorten the validation code logic and break it up into smaller functions.
-create oop sort of method you have your class and your methods