import os
from etl.spark_utils import build_spark
from etl.logging_utils import get_logger
from etl.config_utils import load_config
from pyspark.sql.functions import col, to_timestamp

logger = get_logger(log_file="logs/bronze_to_silver.log")
cfg = load_config()
spark = build_spark("bronze_to_silver")

raw_path = os.path.abspath(cfg["local"]["raw_parquet"])
silver_out = os.path.abspath(os.path.join(cfg["local"]["silver_path"], "sales_silver.parquet"))
logger.info("Reading raw parquet: %s", raw_path)

df = spark.read.parquet(raw_path)
df = (
    df.withColumnRenamed("InvoiceDate", "invoice_date")
      .withColumnRenamed("Customer ID", "customer_id")
      .withColumnRenamed("Price", "price")
      .withColumnRenamed("Quantity", "quantity")
      .withColumnRenamed("Country", "country")
)
df = df.filter((col("quantity").isNotNull()) & (col("quantity") > 0))
df = df.withColumn("line_total", col("quantity") * col("price"))
df = df.withColumn("invoice_ts", to_timestamp(col("invoice_date")))

logger.info("Writing silver parquet to %s", silver_out)
df.write.mode("overwrite").parquet(silver_out)
spark.stop()
logger.info("bronze_to_silver finished")