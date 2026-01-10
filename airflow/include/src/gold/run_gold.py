from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from include.src.utils.variables import load_env_vars
from include.src.gold.data_modeling import data_modeling

if __name__ == "__main__":
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    spark = (
        SparkSession.builder
        .appName("silver_normalization")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.698"
        )
        .config("spark.hadoop.fs.s3a.endpoint", bucket_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    data_modeling(
        spark=spark,
        bucket_name=bucket_name,
        today_str=today_str
    )