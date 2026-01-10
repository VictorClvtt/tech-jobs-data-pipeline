from datetime import datetime
from pyspark.sql import SparkSession

from include.src.utils.variables import load_env_vars
from include.src.silver.run_silver_parser import run_silver_parser
from include.src.silver.run_silver_normalizer import run_silver_normalizer

def main(
    spark: SparkSession,
    bucket_endpoint: str,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    today_str: str
):
    run_silver_parser(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        today_str=today_str
    )

    run_silver_normalizer(
        spark=spark,
        bucket_name=bucket_name,
        today_str=today_str
    )

    spark.stop()

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

    main(
        spark=spark,
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        today_str=today_str
    )