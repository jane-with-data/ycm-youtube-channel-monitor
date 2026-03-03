# YCM - Youtube Channel Monitor // ycm-youtube-channel-monitor

## 1. Overview
Là project thu thập daily thông tin kênh Youtube, playlist thuộc kênh, video thuộc kênh, bình luận thuộc video để lưu trữ, theo dõi, phân tích.

**KEY**: YCM

## 2. Tech Stack
- Database: PostgreSQL
- ETL: PySpark
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
|
init/
├── 01_create_schemas.sql   -- create bronze/silver/gold schemas beforehand
├── 02_create_roles.sql     -- create roles/users
└── 03_seed_data.sql        -- insert data mẫu để dev/test
|
├── src/
│   ├── bronze/                 # Extract/Ingest data source code
│   ├── silver/                 # Transfrom source code
│   └── gold/                   # Load, save file source code
├── main.py                 
├── docker-compose.yml
├── .env
├── .env.example
├── dags/                       # Airflow DAGs (ETL pipelines)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/
│   ├── README.md               # Project overview, set-up guideline
│   ├── data_dictionary.md      # Catalog, table, column, business glossary description
│   ├── change_log.md           # Related-Data changing history (Schemas/Tables,...)
│   ├── pipeline_inventory.md   # Pipeline information (SLA, Schedule,...)
│   └── runbook.md              # Error tracking,...
└── Makefile                    # Docker shortcut commands
```

## 5. Set-up and Run
1. Clone this repository: `git clone https://github.com/jane-with-data/ycm-youtube-channel-monitor.git`

2. Create and fill out .env from template `.env.example`
(Could create password for filling via script `src/utils/generate_password.py`)

3.1. Start Docker on PC and run: `docker-compose up -d`
(Need install Docker PC beforeheadhttps://www.docker.com/products/docker-desktop`)

3.2. Check if Docker start succesfully: `docker-compose ps`
Expected ouput:
```
NAME                      STATUS
ycm_postgres              running
ycm_airflow_init          exited (0)   ← exit 0 means success
ycm_airflow_webserver     running
ycm_airflow_scheduler     running
```

4. Check if schemas init sucessfull
`docker exec -it dwh_postgres psql -U dwh_admin -d dwh -c "\dn"`
Expected output:
```
List of schemas
  Name   |  Owner
---------+-----------
 bronze  | ycm_admin
 silver  | ycm_admin
 gold    | ycm_admin
 audit   | ycm_admin
 config  | ycm_admin
```

5. Access if Airflow UI sucessfull
Access: `http://localhost:8080`, login with admin / password in .env

6. Daily Docker command
```
docker-compose up -d        # Start Docker
docker-compose down         # Off Docker (still remain data)
docker-compose logs -f      # Check logs when occur errors
docker-compose ps           # Check Docker status
```

## 6. Contact
- Data lead: ngannk // ngan.nk.data@gmail.com

## 7. Link
- Git: https://github.com/jane-with-data/ycm-youtube-channel-monitor/tree/main
- Google Drive: https://drive.google.com/drive/folders/1-UHBAZPY_F0CAVYzbQQxKvmUBVg-RJHn