import os 
import psycopg2 
from pyspark.sql import SparkSession 
from etl.config_utils import load_config 
from etl.logging_utils import get_logger 
 
logger = get_logger("load_postgres") 
cfg = load_config() 
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) 
 
gold_path = os.path.join(BASE_DIR, cfg["local"]["gold_path"], "sales_gold.parquet") 
silver_path = os.path.join(BASE_DIR, cfg["local"]["silver_path"], "sales_silver.parquet") 
 
spark = SparkSession.builder.appName("load_postgres").getOrCreate() 
 
gold_df = spark.read.parquet(gold_path) 
silver_df = spark.read.parquet(silver_path) 
 
customers = silver_df.select("customer_id", "country").dropna().dropDuplicates() 
 
conn = psycopg2.connect( 
dbname="ecommerce", 
user="postgres", 
password="postgres", 
host="localhost", 
port="5432" 
) 
cur = conn.cursor() 
 
for row in customers.collect(): 
    cur.execute(""" 
        INSERT INTO dim_customer (customer_id, country) 
        VALUES (%s, %s) 
        ON CONFLICT (customer_id) DO NOTHING; 
    """, (int(row["customer_id"]), row["country"])) 
 
for row in gold_df.collect(): 
    cur.execute(""" 
        INSERT INTO dim_date (invoice_date) 
        VALUES (%s) 
        ON CONFLICT (invoice_date) DO NOTHING; 
    """, (row["invoice_date"],)) 
     
    cur.execute(""" 
        INSERT INTO sales_fact (invoice_date, total_sales, total_quantity, avg_price, 
unique_customers) 
        VALUES (%s, %s, %s, %s, %s); 
    """, ( 
        row["invoice_date"], 
        float(row["total_sales"]), 
        int(row["total_quantity"]), 
        float(row["avg_price"]), 
        int(row["unique_customers"]) 
    )) 
 
conn.commit() 
cur.close() 
conn.close() 
spark.stop() 
logger.info(" Data loaded into Postgres successfully!") 