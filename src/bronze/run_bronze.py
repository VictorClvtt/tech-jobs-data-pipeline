from datetime import datetime
from src.utils.variables import load_env_vars

from src.bronze.collectors.programathor_collector import programathor_collector
from src.bronze.collectors.vagascom_collector import vagascom_collector
from src.bronze.collectors.nerdin_collector import nerdin_collector

MAX_PAGES = 5

PROGRAMATHOR_URL = "https://programathor.com.br/jobs/page/"
VAGASCOM_URL = "https://www.vagas.com.br/vagas-de-tecnologia?&pagina="
NERDIN_URL = "https://www.nerdin.com.br/vagas"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
}

def main():
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

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

    print("\n")

    vagascom_collector(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        today_str=today_str,
        max_pages=MAX_PAGES,
        base_url=VAGASCOM_URL,
        headers=HEADERS
    )

    print("\n")

    nerdin_collector(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        today_str=today_str,
        max_pages=None,
        base_url=NERDIN_URL,
        headers=HEADERS
    )

if __name__ == "__main__":
    main()
