import json
from src.utils.bucket import read_file_from_bucket, file_to_bucket


def run_parser(
    parser_func,
    source_name: str,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    bucket_endpoint: str,
    today_str: str
):
    bronze_path = (
        f"/bronze/json/{source_name}/"
        f"{source_name}-pages-{today_str}.json"
    )

    raw = read_file_from_bucket(
        bucket_endpoint,
        access_key,
        secret_key,
        bucket_name,
        bronze_path
    )

    data = json.loads(raw.decode("utf-8"))

    # 🔑 NORMALIZAÇÃO AQUI
    if isinstance(data, dict):
        pages = [data]
    else:
        pages = data

    jobs = [
        job
        for page in pages
        for job in parser_func(
            page["html"],
            page["collecting_date"]
        )
    ]

    silver_path = (
        f"silver/parsed/{source_name}/"
        f"{source_name}-jobs-{today_str}.json"
    )

    file_to_bucket(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        path=silver_path,
        data=jobs,
        format="jsonl"
    )
