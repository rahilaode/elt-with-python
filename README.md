# ELT With Python & SQL

Learning purposes only

## Prerequisites
- First, make sure you already installed :
    - Python
        - Python is used for running the ELT Pipeline
    - Docker
        - Docker is used for running the Data Source & Data Warehouse
    - Dbeaver
        - Dbeaver is used to make it easier to access the database using a GUI

- Install python requirements libraries :
  ```
  pip install -r requirements.txt
  ```

- Create env file in project root directory :
  ```
  sudo nano .env
  ```
  - Fill this into .env file :
    ```
    # Source
    SRC_POSTGRES_DB=...
    SRC_POSTGRES_HOST=...
    SRC_POSTGRES_USER=...
    SRC_POSTGRES_PASSWORD=...
    SRC_POSTGRES_PORT=...

    # DWH
    DWH_POSTGRES_DB=...
    DWH_POSTGRES_HOST=...
    DWH_POSTGRES_USER=...
    DWH_POSTGRES_PASSWORD=...
    DWH_POSTGRES_PORT=...
    ```

- Running data source & data warehouse services :
  ```
  docker compose up -d
  ```
  
- Define Schema & Relation for Data warehouse (Staging & Final Area)
  ```
  python3 ./helper/dwh_init.py
  ```

## Running ELT Pipeline
### Extract & Load
- Command :
  ```
  python3 ./helper/utils/extract_load.py
  ```

### Transform
- Command :
  ```
  python3 ./helper/utils/transform.py
  ```