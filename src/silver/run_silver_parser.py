import json
from datetime import datetime
from pyspark.sql import SparkSession

from src.utils.variables import load_env_vars
from src.silver.parsers.nerdin_parser import nerdin_parser
from src.silver.parsers.programathor_parser import programathor_parser
from src.silver.parsers.vagascom_parser import vagascom_parser
from src.silver.parsers.run_parser import run_parser

def run_silver_parser(
    access_key: str,
    secret_key: str,
    bucket_name: str,
    bucket_endpoint: str,
    today_str: str
):
    run_parser(
        parser_func=nerdin_parser,
        source_name="nerdin",
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        bucket_endpoint=bucket_endpoint,
        today_str=today_str
    )

    run_parser(
        parser_func=programathor_parser,
        source_name="programathor",
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        bucket_endpoint=bucket_endpoint,
        today_str=today_str
    )

    run_parser(
        parser_func=vagascom_parser,
        source_name="vagascom",
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        bucket_endpoint=bucket_endpoint,
        today_str=today_str
    )

if __name__ == "__main__":
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    run_silver_parser(
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        bucket_endpoint=bucket_endpoint,
        today_str=today_str
    )