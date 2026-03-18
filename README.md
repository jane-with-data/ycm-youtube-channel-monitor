# YCM - Youtube Channel Monitor // ycm-youtube-channel-monitor

## 1. Overview
This project ETL data from Youtube API V3 to monitor channel performance.
This project apply Medallion Architecture Lakehouse with 3 layer: bronze -> silver -> gold.

**KEY**: YCM

## 2. Tech Stack
- Data Storage: .jsonl, .parquet
- ETL: Pandas
- Orchestration: Airflow
- BI: Power BI

## 3. Schema overview
- bronze.*  — raw data from source (append)
- silver.*  — cleaned, conformed (up-to-date data/3NF)
- gold.*    — dim/fact/agg cho BI (dimensional modeling)

## 4. Project structure
```
ycm-youtube-channel-monitor/
├── data/
│   ├── bronze/                # Load data to JSON file
│   ├── silver/                # Load data to .parquet file
│   ├── gold/                  # Load data to .parquet file
|
├── src/
│   ├── bronze/                 # Ingest data source code // Code for pipeline in bronze layer
│   ├── silver/                 # Transfrom source code // Code for pipeline in silver layer
│   └── gold/                   # Denormalize data, dim/fact/aggrerate table // Code for pipeline in gold layer
├── main.py                 
├── .env
├── .env.example
├── dags/                       # Airflow DAGs (Pipeline Orchestration)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/
│   ├── README.md               # Project overview, set-up guideline
│   └── pipeline_management.md  # Pipeline information (SLA, Schedule, data dictionary,...)
└── .
```

## 5. Set-up and Run
1. Clone this repository: `git clone https://github.com/jane-with-data/ycm-youtube-channel-monitor.git`

2. Create and fill out .env from template `.env.example`
(Could create password for filling via script `src/utils/generate_password.py`)

## 6. Contact
- Data lead: ngannk // ngan.nk.data@gmail.com

## 7. Link
- Git: https://github.com/jane-with-data/ycm-youtube-channel-monitor/tree/main
- Google Drive: https://drive.google.com/drive/folders/1-UHBAZPY_F0CAVYzbQQxKvmUBVg-RJHn