from src.utils.web_scraping import collect_pages
from src.utils.bucket import file_to_bucket

def vagascom_collector(
    bucket_endpoint,
    access_key,
    secret_key,
    bucket_name,
    today_str,
    max_pages,
    base_url,
    headers
):
    print("[INFO] Starting Vagas.com Bronze ingestion\n")

    file_to_bucket(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        path=f"bronze/JSON/vagascom/vagascom-pages-{today_str}.json",

        data=collect_pages(
            max_pages=max_pages,
            base_url=base_url,
            headers=headers
        )
    )

    print("\n[INFO] Vagas.com Bronze ingestion finished successfully")