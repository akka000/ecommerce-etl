import os
from etl.spark_utils import build_spark
from etl.logging_utils import get_logger
from etl.config_utils import load_config
from pyspark.sql.functions import to_date, col, sum as sum_, avg, countDistinct

logger = get_logger(log_file="logs/silver_to_gold.log")
cfg = load_config()
spark = build_spark("silver_to_gold")

silver_in = os.path.abspath(os.path.join(cfg["local"]["silver_path"], "sales_silver.parquet"))
gold_out = os.path.abspath(os.path.join(cfg["local"]["gold_path"], "sales_gold.parquet"))
logger.info("Reading silver: %s", silver_in)

df = spark.read.parquet(silver_in)
df = df.filter(col("invoice_ts").isNotNull())
df = df.withColumn("invoice_date", to_date(col("invoice_ts")))
agg = (
    df.groupBy("invoice_date")
      .agg(
          sum_("line_total").alias("total_sales"),
          sum_("quantity").alias("total_quantity"),
          avg("price").alias("avg_price"),
          countDistinct("customer_id").alias("unique_customers")
      )
)

logger.info("Writing gold partitioned by invoice_date: %s", gold_out)
agg.write.mode("overwrite").partitionBy("invoice_date").parquet(gold_out)
spark.stop()
logger.info("silver_to_gold finished")