#  E-commerce Sales ETL (PySpark + Airflow + Postgres) 
A step-by-step **data engineering project** that ingests raw e-commerce sales 
data, cleans it with PySpark, builds daily sales aggregates, and loads into a Postgres 
warehouse and it is orchestrated with **Apache Airflow**. 

## Dataset 
I use the **Online Retail II dataset** from the [UCI Machine Learning 
Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii).   
It contains all the transactions occurring between **01/12/2009 and 09/12/2011** for a 
UK-based online retail.
 
**Columns include**:   
- `Invoice` – Invoice number   
- `StockCode` – Product code   
- `Description` – Product description   
- `Quantity` – Units purchased   
- `InvoiceDate` – Timestamp   
- `UnitPrice` – Price per unit   
- `CustomerID` – Customer identifier   
- `Country` – Customer country   

--- 
## Setup 
1. **Clone repo**
 
```bash 
git clone https://github.com/akka000/ecommerce-etl.git 
cd ecommerce-etl
```
2. **Create virtualenv & install requirements**
```bash
python3.11 -m venv .venv 
source .venv/bin/activate        
(Windows: .venv\Scripts\activate)
pip install -r requirements.txt 
```
3. **Prepare dataset** 
- Download `online_retail_II.xlsx` into `data/raw/`. 
- Convert to Parquet:
```bash
python3.11 scripts/prepare_data.py
```
- Output: `data/raw/online_retail_II.parquet`
## ETL Pipeline 
1. Bronze → Silver
 
Cleans raw data, filters invalid rows, standardizes column names, computes `line_total`.
```bash 
spark-submit etl/bronze_to_silver.py 
```
2. Silver → Gold
 
Aggregates by invoice_date: 
- total sales 
- total quantity 
- avg price 
- unique customers 
```bash
spark-submit etl/silver_to_gold.py 
```
3. Data Quality Checks

Validates no negative sales, no zero quantities, sane values. 
```bash
spark-submit etl/data_quality.py
```
4. Load into Postgres 

Upserts dimensions (`dim_date`, `dim_customer`) and inserts into `sales_fact`. 
```bash
spark-submit etl/load_postgres.py 
```
Warehouse Schema (Postgres) 

Run this once in your Postgres DB: 
```bash
CREATE DATABASE ecommerce; 
\c ecommerce 
CREATE EXTENSION IF NOT EXISTS pgcrypto; 
-- Run the schema script 
\i warehouse/create_tables.sql 
```
Schema:

- `dim_date`: unique dates (UUID PK). 
- `dim_customer`: customers (country). 
- `sales_fact`: fact table with UUID PK, references dims. 

Airflow Orchestration 
```bash
Init Airflow 
export AIRFLOW_HOME=~/airflow 
airflow db init 
airflow users create \
 --username admin --password admin \
 --firstname Arman --lastname Khaxar \
 --role Admin --email admin@example.com 
```
1. Run services 
```bash
airflow standalone
```
2. Enable DAG 

- Copy `dags/ecommerce_pipeline.py` into `$AIRFLOW_HOME/dags/`. 
- Open http://localhost:8080, enable the `ecommerce_pipeline` DAG, and trigger 
manually. 

Smoke Test

For quick validation:
```bash
spark-submit etl/bronze_to_silver.py 
spark-submit etl/silver_to_gold.py 
spark-submit etl/data_quality.py 
spark-submit etl/load_postgres.py 
```
Check Postgres: 

    SELECT * FROM sales_fact LIMIT 10;