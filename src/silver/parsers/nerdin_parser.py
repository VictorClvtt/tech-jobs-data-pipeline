from bs4 import BeautifulSoup

BASE_URL = "https://nerdin.com.br/"

def nerdin_parser(html: str, collecting_date: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    cards = soup.select("div.vaga-card")

    for card in cards:
        # -------- TÍTULO --------
        title_el = card.select_one("h3.vaga-titulo")
        title = title_el.get_text(" ", strip=True) if title_el else None

        # -------- LINK --------
        job_url = None
        onclick = card.get("onclick")
        if onclick and "window.location.href" in onclick:
            relative_url = onclick.split("'")[1]
            job_url = BASE_URL + relative_url

        # -------- EMPRESA --------
        company_el = card.select_one("div.vaga-empresa")
        company = (
            company_el.get_text(" ", strip=True)
            .replace("", "")
            if company_el else None
        )

        # -------- LOCALIZAÇÃO --------
        location_el = card.select_one("div.vaga-local")
        location = (
            location_el.get_text(" ", strip=True)
            .replace("", "")
            if location_el else None
        )

        # -------- TIPO DE CONTRATO --------
        employment_type = None
        contract_icon = card.select_one("i.fa-file-contract")
        if contract_icon:
            employment_type = contract_icon.get("title")

        # -------- SENIORIDADE (não existe nesse card) --------
        seniority = None

        # -------- SALÁRIO (não existe nesse card) --------
        salary_el = card.select_one("div.vaga-salario")
        salary = (
            salary_el.get_text(" ", strip=True)
            if salary_el else None
        )

        # -------- TECH STACK / TAGS --------
        techs = [
            tag.get_text(strip=True).replace("#", "")
            for tag in card.select("div.vaga-hashtags a.hashtag")
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
            "source": "Nerdin"
        })

    return jobs
