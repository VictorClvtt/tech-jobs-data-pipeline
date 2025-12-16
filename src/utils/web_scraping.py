import requests
from datetime import datetime, timezone
from typing import Optional

def _fetch_page(url: str, headers: dict, page_number: int | None = None) -> Optional[dict]:
    response = requests.get(url, headers=headers, timeout=10)

    if response.status_code != 200:
        print(f"[ERROR] Failed to collect page {page_number or ''} | Status: {response.status_code}")
        return None

    print(f"[INFO] Successfully collected page {page_number or ''} | Status: {response.status_code}")

    return {
        "source": "programathor",
        "page": page_number,
        "url": response.url,
        "status_code": response.status_code,
        "html": response.text,
        "scraping_date": datetime.now(timezone.utc).isoformat()
    }

def collect_page(base_url: str, headers: dict) -> Optional[dict]:
    return _fetch_page(base_url, headers)

def collect_pages(max_pages: int, base_url: str, headers: dict) -> list[dict]:
    pages_data = []

    for page_number in range(1, max_pages + 1):
        page_data = _fetch_page(
            url=f"{base_url}{page_number}",
            headers=headers,
            page_number=page_number
        )

        if not page_data:
            break

        pages_data.append(page_data)

    return pages_data
