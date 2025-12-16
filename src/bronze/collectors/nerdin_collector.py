from src.utils.web_scraping import collect_page
from src.utils.bucket import file_to_bucket

def nerdin_collector(
    bucket_endpoint,
    access_key,
    secret_key,
    bucket_name,
    today_str,
    base_url,
    headers
):
    print("[INFO] Starting Nerdin Bronze ingestion\n")

    file_to_bucket(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        path=f"bronze/JSON/nerdin/nerdin-pages-{today_str}.json",

        data=collect_page(
            base_url=base_url,
            headers=headers
        )
    )

    print("\n[INFO] Nerdin Bronze ingestion finished successfully")