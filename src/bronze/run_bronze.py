from datetime import datetime

from src.utils.variables import load_env_vars
from src.utils.bucket import file_to_bucket
from src.bronze.collector import collector

sources = [
    {
        "name": "Programathor",
        "url": "https://programathor.com.br/jobs/page/",
        "max_pages": 5
    },
    {
        "name": "Vagas.com",
        "url": "https://www.vagas.com.br/vagas-de-tecnologia?&pagina=",
        "max_pages": 5
    },
    {
        "name": "Nerdin",
        "url": "https://www.nerdin.com.br/vagas",
        "max_pages": None
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
}

def main():
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    for source in sources:
        source_name = source["name"].lower().replace(".", "")

        file_to_bucket(
            bucket_endpoint=bucket_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name,
            path=f"bronze/json/{source_name}/{source_name}-pages-{today_str}.json",
            data=collector(
                max_pages=source["max_pages"],
                base_url=source["url"],
                source_name=source["name"],
                headers=HEADERS
            )
        )

        print("\n")

if __name__ == "__main__":
    main()
