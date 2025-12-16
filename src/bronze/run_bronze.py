from datetime import datetime

from src.utils.variables import load_env_vars

from src.bronze.collectors.programathor_collector import programathor_collector

MAX_PAGES = 5
PROGRAMATHOR_URL = "https://programathor.com.br/jobs"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
}

access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()

today_str = datetime.today().strftime("%Y-%m-%d")


if __name__ == "__main__":
    programathor_collector(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        today_str=today_str,
        max_pages=MAX_PAGES,
        base_url=PROGRAMATHOR_URL,
        headers=HEADERS
    )