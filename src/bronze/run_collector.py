from src.utils.bucket import file_to_bucket
from src.bronze.collector import collector
from config.http_headers import HEADERS

def run_collector(
    source: dict,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    bucket_endpoint: str,
    today_str: str
):
    source_name = source["name"].lower().replace(".", "")

    data = collector(
        max_pages=source["max_pages"],
        base_url=source["url"],
        source_name=source["name"],
        headers=HEADERS
    )

    file_to_bucket(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        path=f"bronze/json/{source_name}/{source_name}-pages-{today_str}.json",
        data=data
    )
