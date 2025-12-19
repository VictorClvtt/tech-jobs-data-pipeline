from bs4 import BeautifulSoup

BASE_URL = "https://programathor.com.br"

def programathor_parser(html: str, collecting_date: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    cards = soup.select("div.cell-list")

    for card in cards:
        title_el = card.select_one("h3")
        title = title_el.get_text(" ", strip=True) if title_el else None

        link_el = card.select_one("a")
        job_url = (
            BASE_URL + link_el["href"]
            if link_el and link_el.has_attr("href")
            else None
        )

        icons = card.select("div.cell-list-content-icon span")

        company = location = salary = seniority = employment_type = None

        for span in icons:
            span_html = str(span)
            text = span.get_text(strip=True)

            if "fa-briefcase" in span_html:
                company = text
            elif "fa-map-marker-alt" in span_html:
                location = text
            elif "fa-money-bill-alt" in span_html:
                salary = text
            elif "fa-chart-bar" in span_html:
                seniority = text
            elif "fa-file-alt" in span_html:
                employment_type = text

        techs = [
            tech.get_text(strip=True)
            for tech in card.select("span.tag-list")
        ]

        jobs.append({
            "title_raw": title,
            "company_raw": company,
            "location_raw": location,
            "salary_raw": salary,
            "seniority_raw": seniority,
            "employment_type_raw": employment_type,
            "tech_stack_raw": techs,
            "job_url_raw": job_url,
            "collecting_date_raw": collecting_date,
            "source": "Programathor"
        })

    return jobs