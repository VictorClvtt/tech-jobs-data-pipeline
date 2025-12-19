import json
from datetime import datetime
from pyspark.sql import SparkSession

from src.utils.variables import load_env_vars
from src.utils.bucket import read_file_from_bucket
from src.silver.parsers.programathor_parser import programathor_parser
from src.silver.parsers.vagascom_parser import vagascom_parser
from src.silver.parsers.nerdin_parser import nerdin_parser

from src.silver.normalizer import normalizer

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
}

def main():
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    spark = SparkSession.builder \
        .appName("bronze_to_silver") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.698") \
        .config("spark.hadoop.fs.s3a.endpoint", bucket_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Programathor
    pages_programathor = read_file_from_bucket(
        bucket_endpoint,
        access_key,
        secret_key,
        bucket_name,
        f"/bronze/json/programathor/programathor-pages-{today_str}.json"
    )

    pages_programathor = json.loads(pages_programathor.decode("utf-8"))

    all_jobs_programathor = [
        job
        for page in pages_programathor
        for job in programathor_parser(
            page["html"],
            page["collecting_date"]
        )
    ]

    df_programathor = normalizer(
        all_jobs_programathor,
        spark
    )

    df_programathor.write.mode("overwrite").parquet(f"s3a://{bucket_name}/silver/programathor/dt={today_str}/")

    # Vagas.com
    pages_vagascom = read_file_from_bucket(
        bucket_endpoint,
        access_key,
        secret_key,
        bucket_name,
        f"/bronze/json/vagascom/vagascom-pages-{today_str}.json"
    )

    pages_vagascom = json.loads(pages_vagascom.decode("utf-8"))
    
    all_jobs_vagascom = [
        job
        for page in pages_vagascom
        for job in vagascom_parser(
            page["html"],
            page["collecting_date"]
        )
    ]

    df_vagascom = normalizer(
        all_jobs_vagascom,
        spark
    )

    df_vagascom.write.mode("overwrite").parquet(f"s3a://{bucket_name}/silver/vagascom/dt={today_str}/")

    # Nerdin
    page_nerdin = read_file_from_bucket(
        bucket_endpoint,
        access_key,
        secret_key,
        bucket_name,
        f"/bronze/json/nerdin/nerdin-pages-{today_str}.json"
    )

    page_nerdin = json.loads(
        page_nerdin.decode("utf-8")
    )

    all_jobs_nerdin = nerdin_parser(
        page_nerdin["html"],
        page_nerdin["collecting_date"]
    )

    df_nerdin = normalizer(
        all_jobs_nerdin,
        spark
    )

    df_nerdin.write.mode("overwrite").parquet(f"s3a://{bucket_name}/silver/nerdin/dt={today_str}/")

    df_programathor.show()
    df_vagascom.show()
    df_nerdin.show()

    spark.stop()


if __name__ == "__main__":
    main()
