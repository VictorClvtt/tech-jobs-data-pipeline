from datetime import datetime
from pyspark.sql import SparkSession

from include.src.utils.variables import load_env_vars

from include.src.silver.normalizer import normalizer

def run_silver_normalizer(
    spark: SparkSession,
    bucket_name: str,
    today_str: str,
    source: str,
):
    df = normalizer(
        spark.read.json(
            f"s3a://{bucket_name}/silver/parsed/{source}/{source}-jobs-{today_str}.json"
        )
    )

    df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/silver/normalized/{source}/dt={today_str}/"
    )

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)

    args = parser.parse_args()

    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    spark = (
        SparkSession.builder
        .appName(f"silver_normalization_{args.source}")
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

    run_silver_normalizer(
        spark=spark,
        bucket_name=bucket_name,
        today_str=today_str,
        source=args.source,
    )

    spark.stop()
