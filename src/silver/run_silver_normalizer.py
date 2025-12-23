from datetime import datetime
from pyspark.sql import SparkSession

from src.utils.variables import load_env_vars

from src.silver.normalizer import normalizer

def run_silver_normalizer(
    spark: SparkSession,
    bucket_name: str,
    today_str: str
):
    ###############
    # NORMALIZING #
    ###############

    # Programathor
    df_programathor = normalizer(
        spark.read.json(
            f"s3a://{bucket_name}/silver/parsed/programathor/programathor-jobs-{today_str}.json"
        )
    )

    df_programathor.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/silver/normalized/programathor/dt={today_str}/"
    )

    # Vagas.com
    df_vagascom = normalizer(
        spark.read.json(
            f"s3a://{bucket_name}/silver/parsed/vagascom/vagascom-jobs-{today_str}.json"
        )
    )

    df_vagascom.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/silver/normalized/vagascom/dt={today_str}/"
    )

    # Nerdin
    df_nerdin = normalizer(
        spark.read.json(
            f"s3a://{bucket_name}/silver/parsed/nerdin/nerdin-jobs-{today_str}.json"
        )
    )

    df_nerdin.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/silver/normalized/nerdin/dt={today_str}/"
    )

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

    run_silver_normalizer(
        spark=spark,
        bucket_name=bucket_name,
        today_str=today_str
    )

    spark.stop()