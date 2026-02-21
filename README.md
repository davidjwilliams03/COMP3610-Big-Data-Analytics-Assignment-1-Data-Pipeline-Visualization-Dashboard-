# NYC Yellow Taxi Data Analytics Pipeline – January 2024

## Overview

This project implements a complete end-to-end data analytics pipeline using the January 2024 NYC Yellow Taxi dataset.

The pipeline includes:

- Programmatic data ingestion
- Strict validation
- Data cleaning
- Feature engineering
- SQL analytics using DuckDB
- Interactive dashboard using Streamlit
- Public deployment

The objective is to demonstrate robust data engineering practices, analytical reasoning, and scalable dashboard development.

---

## Dataset

Source:
NYC Taxi & Limousine Commission (TLC) – January 2024 Yellow Taxi Data

Files used:
- yellow_tripdata_2024_01.parquet
- taxi_zone_lookup.csv

Records processed:
~2.96 million rows

---

## Project Structure

project/
│
├── assignment1.ipynb # Main notebook (Parts 1–3)
├── app.py # Streamlit dashboard
├── data/
│ └── raw/
│ ├── yellow_tripdata_2024_01.parquet
│ └── taxi_zone_lookup.csv
├── requirements.txt
├── README.md
└── .gitignore

---

## Part 1 – Data Ingestion & Validation

- Programmatic download using requests
- Schema validation
- Datetime validation
- Column existence checks
- Row count confirmation

All validation raises exceptions if integrity fails.

---

## Part 2 – Data Cleaning & Feature Engineering

Cleaning includes:

- Removal of null critical fields
- Removal of invalid trip distances
- Removal of unrealistic fare values
- Removal of negative trip durations

Feature engineering (exactly 4 features):

1. trip_duration_minutes  
2. trip_speed_mph  
3. pickup_hour  
4. pickup_day_of_week  

Row counts before and after cleaning are explicitly reported.

---

## Part 3 – SQL Analysis

Five SQL queries were executed using DuckDB:

1. Top 10 pickup zones
2. Average fare by hour
3. Payment type distribution
4. Average tip percentage by day (credit card only)
5. Top 5 pickup–dropoff zone pairs

---

## Dashboard (Streamlit)

The dashboard includes:

- Cached lazy data loading
- Sidebar filters:
  - Date range
  - Hour range
  - Payment type
- 5 KPI metrics
- 5 required visualisations
- Analytical interpretation text

Filtering is performed in Polars before converting to Pandas for performance optimisation.

---

## Installation & Running Locally

Create environment:
pip install -r requirements.txt

Run dashboard:
streamlit run app.py 
or
python -m streamlit run app.py

---

## Deployment

The dashboard can be deployed via:
- Streamlit Community Cloud
- Render
- Railway
- Heroku

---

## AI Usage Disclosure

See section at bottom of notebook for full declaration.

---

## Author

David Williams  
