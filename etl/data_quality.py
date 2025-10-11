import os 
from etl.config_utils import load_config 
from etl.logging_utils import get_logger 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col

logger = get_logger() 
cfg = load_config() 
spark = SparkSession.builder.appName("data_quality").\
    config("spark.sql.shuffle.partitions", "8").getOrCreate()


gold_in = os.path.abspath(os.path.join(cfg["local"]["gold_path"], "sales_gold.parquet")) 
df = spark.read.parquet(gold_in)

errors = [] 
if df.filter(col("total_sales") <= 0).count() > 0:
    errors.append("Non-positive total_sales present") 
if df.filter(col("total_quantity") <= 0).count() > 0:
    errors.append("Non-positive total_quantity present") 
if errors:
    logger.error("Data Quality failed: %s", errors) 
    raise Exception("Data Quality Failed: " + "; ".join(errors))
 
logger.info("Data Quality passed") 
spark.stop() 