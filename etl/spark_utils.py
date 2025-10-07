from pyspark.sql import SparkSession

def build_spark(app_name="ecom-etl"): 
    return (SparkSession.builder 
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "8") 
            .getOrCreate())