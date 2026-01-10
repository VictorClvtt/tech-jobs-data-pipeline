from bs4 import BeautifulSoup

BASE_URL = "https://vagas.com.br"

def vagascom_parser(html: str, collecting_date: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # seleciona todos os cards de vaga
    cards = soup.select("li.vaga")

    for card in cards:
        # Título e URL
        title_el = card.select_one("h2.cargo a.link-detalhes-vaga")
        title = title_el.get_text(" ", strip=True) if title_el else None

        job_url = (
            BASE_URL + title_el["href"]
            if title_el and title_el.has_attr("href")
            else None
        )

        # Empresa
        company_el = card.select_one("span.emprVaga")
        company = company_el.get_text(strip=True) if company_el else None

        # Senioridade
        seniority_el = card.select_one("span.nivelVaga")
        seniority = seniority_el.get_text(strip=True) if seniority_el else None

        # Localização (span ou div)
        location_el = card.select_one(".vaga-local")
        location = location_el.get_text(" ", strip=True) if location_el else None

        jobs.append({
            "title_raw": title,
            "company_raw": company,
            "location_raw": location,
            "salary_raw": None,
            "seniority_raw": seniority,
            "employment_type_raw": None,
            "tech_stack_raw": [],
            "job_url_raw": job_url,
            "collecting_date_raw": collecting_date,
            "source": "Vagas.com"
        })

    return jobs