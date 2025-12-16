import requests
from datetime import datetime, timezone

def collect_pages(max_pages: int, base_url: str, headers: dict) -> list:
    pages_data = []

    for page_number in range(1, max_pages + 1):

        response = requests.get(
            f"{base_url}/page/{page_number}",
            headers=headers,
            timeout=10
        )

        if response.status_code != 200:
            print(f"[ERROR] Failed to collect page {page_number} | Status: {response.status_code}")
            break

        page_data = {
            "source": "programathor",
            "page": page_number,
            "url": response.url,
            "status_code": response.status_code,
            "html": response.text,
            "scraping_date": datetime.now(timezone.utc).isoformat()
        }

        pages_data.append(page_data)

        print(f"[INFO] Successfully collected page {page_number} | Status: {response.status_code}")

    return pages_data