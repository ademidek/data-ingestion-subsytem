# The Data Ingestion Subsystem 🔌

## Application Overview 📝
This application is a **Data Ingestion Subsystem**, designed to collect and organize data from different sources into a unified, structured environment. It is a foundational component of modern data engineering pipelines, responsible for ensuring data is **accurate, clean, and reliable** before downstream analytics or machine learning use.

This subsystem uses **Python** and **PostgreSQL** to read (extract), validate, clean (transform), and load data for later use in analytics or warehousing, similar to other ETL pipelines.

The idea of this application is to analyze and visualize potential correlations between esophageal cancer and several key risk factors, including smoking history, alcohol intake, and body mass index (BMI). By building a structured ETL pipeline, the project transforms raw medical datasets into clean, validated, and analysis-ready information. Furthermore, the healthset data will be simplified, made easier to understand for the average person.

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
│   │   ├── __init__.py
│   │   ├── csv_reader.py
│   │   ├── json_reader.py
│   │   └── api_reader.py
│   ├── validate.py
│   ├── clean.py
│   ├── load.py
│   ├── rules.py
│   ├── repo.py
│   ├── schema.sql
│   ├── schema_init.py
│   ├── logging_config.py
│   ├── .env
│   └── main.py
│
├── config/
│   └── sources.yml
│
├── data/
│   ├── Esophageal_Dataset.csv
│
├── logs/
│   ├── log files
│
├── tests/
│   ├── test_validate.py
│   └── test_clean.py
│   └── test_load.py
│
├── __init__.py
├── requirements.txt
└── README.md
```

## Tech Stack📚

* Python 3.13
* pandas
* SQL
* PostgreSQL
* psycopg2
* python-dotenv
* pytest

To run the script, enter the command "python -m src.main" in the project's root.
