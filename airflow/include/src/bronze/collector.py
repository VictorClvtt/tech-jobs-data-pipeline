from include.src.utils.web_scraping import collect_page, collect_pages

def collector(
    base_url,
    headers,
    source_name,
    max_pages=None   
):
    print(f"[INFO] Starting {source_name} page collecting\n")

    if max_pages:
        data = collect_pages(
            max_pages=max_pages,
            base_url=base_url,
            headers=headers
        )
    else:
        data = collect_page(
            base_url=base_url,
            headers=headers
        )

    print(f"\n[INFO] {source_name} page collecting finished successfully")

    return data