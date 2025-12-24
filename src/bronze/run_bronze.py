from datetime import datetime

from src.utils.variables import load_env_vars
from config.sources import sources
from src.bronze.run_collector import run_collector


def main(
    access_key: str,
    secret_key: str,
    bucket_name: str,
    bucket_endpoint: str,
    today_str: str
):
    for source in sources:
        run_collector(
            source=source,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name,
            bucket_endpoint=bucket_endpoint,
            today_str=today_str
        )


if __name__ == "__main__":
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    main(
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        bucket_endpoint=bucket_endpoint,
        today_str=today_str
    )
