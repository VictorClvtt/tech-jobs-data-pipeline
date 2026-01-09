import streamlit as st
import pandas as pd
import duckdb
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_DIR))

from src.utils.variables import load_env_vars

# ======================================================
# App config
# ======================================================
st.set_page_config(
    page_title="Tech Jobs Explorer",
    layout="wide"
)

st.title("📀 Tech Jobs Explorer")

# ======================================================
# Data source config
# ======================================================
USE_PERSISTENT_DB = True
DUCKDB_PATH = "./src/app/catalog.duckdb"

# "minio" | "local"
DATA_SOURCE = "local"

# ======================================================
# DuckDB connection
# ======================================================
@st.cache_resource
def init_connection(persistent: bool, db_path: str):
    if persistent:
        con = duckdb.connect(database=db_path)
    else:
        con = duckdb.connect(database=":memory:")

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    if DATA_SOURCE == "minio":
        access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()

        bucket_endpoint = (
            bucket_endpoint
            .replace("http://", "")
            .replace("https://", "")
            .rstrip("/")
        )

        con.execute(f"SET s3_endpoint='{bucket_endpoint}';")
        con.execute("SET s3_region='us-east-1';")
        con.execute(f"SET s3_access_key_id='{access_key}';")
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")
    else:
        bucket_name = None

    return con, bucket_name


def load_table(con, table_name, parquet_path):
    if DATA_SOURCE == "minio":
        df = con.execute(
            "SELECT * FROM read_parquet(?)",
            [parquet_path]
        ).df()

        con.register("df_tmp", df)
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_tmp"
        )
        con.unregister("df_tmp")

        return df

    elif DATA_SOURCE == "local":
        return con.execute(f"SELECT * FROM {table_name}").df()

    else:
        raise ValueError("DATA_SOURCE inválido")


con, bucket_name = init_connection(
    persistent=USE_PERSISTENT_DB,
    db_path=DUCKDB_PATH
)

# ======================================================
# GOLD tables
# ======================================================
gold_tables = {
    "job": f"s3://{bucket_name}/gold/3fn/job/*/*.parquet",
    "company": f"s3://{bucket_name}/gold/3fn/company/*/*.parquet",
    "city": f"s3://{bucket_name}/gold/3fn/city/*/*.parquet",
    "technology": f"s3://{bucket_name}/gold/3fn/technology/*/*.parquet",
    "job_technology": f"s3://{bucket_name}/gold/3fn/job_technology/*/*.parquet"
}

# ======================================================
# Load tables
# ======================================================
@st.cache_data
def load_all_tables():
    job = load_table(con, "job", gold_tables["job"])
    company = load_table(con, "company", gold_tables["company"])
    city = load_table(con, "city", gold_tables["city"])
    technology = load_table(con, "technology", gold_tables["technology"])
    job_technology = load_table(con, "job_technology", gold_tables["job_technology"])

    return job, company, city, technology, job_technology


job_df, company_df, city_df, tech_df, job_tech_df = load_all_tables()

# ======================================================
# JOIN with pandas
# ======================================================
jobs = (
    job_df
    .merge(company_df[["company_id", "company_name"]], on="company_id", how="left")
    .merge(city_df[["city_id", "city_name"]], on="city_id", how="left")
)

job_tech = job_tech_df.merge(
    tech_df[["technology_id", "technology_name"]],
    on="technology_id",
    how="left"
)

tech_stack = (
    job_tech
    .groupby("job_id")["technology_name"]
    .apply(list)
    .reset_index(name="tech_stack")
)

df = jobs.merge(tech_stack, on="job_id", how="left")
df["tech_stack"] = df["tech_stack"].apply(lambda x: x if isinstance(x, list) else [])

# ======================================================
# Date normalization (IMPORTANTE)
# ======================================================
df["collecting_date"] = pd.to_datetime(df["collecting_date"])

# ======================================================
# Filters UI
# ======================================================
st.subheader("🔎 Pesquisa de Vagas")

with st.sidebar:
    st.header("Filtros")

    search_text = st.text_input("Buscar por título")

    company_filter = st.multiselect(
        "Empresa",
        sorted(df["company_name"].dropna().unique())
    )

    city_filter = st.multiselect(
        "Cidade",
        sorted(df["city_name"].dropna().unique())
    )

    source_filter = st.multiselect(
        "Fonte",
        sorted(df["source"].dropna().unique())
    )

    tech_options = sorted({tech for stack in df["tech_stack"] for tech in stack})
    tech_filter = st.multiselect("Tecnologias", tech_options)

    # 🔹 Filtro por data de coleta
    min_date = df["collecting_date"].min().date()
    max_date = df["collecting_date"].max().date()

    date_range = st.date_input(
        "Data de coleta",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

# ======================================================
# Apply filters
# ======================================================
filtered = df.copy()

if search_text:
    filtered = filtered[
        filtered["title"].str.contains(search_text, case=False, na=False)
    ]

if company_filter:
    filtered = filtered[filtered["company_name"].isin(company_filter)]

if city_filter:
    filtered = filtered[filtered["city_name"].isin(city_filter)]

if source_filter:
    filtered = filtered[filtered["source"].isin(source_filter)]

if tech_filter:
    filtered = filtered[
        filtered["tech_stack"].apply(
            lambda stack: any(t in stack for t in tech_filter)
        )
    ]

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["collecting_date"].dt.date >= start_date) &
        (filtered["collecting_date"].dt.date <= end_date)
    ]

filtered = filtered.sort_values("collecting_date", ascending=False)

# ======================================================
# Results
# ======================================================
st.markdown(f"## 📄 {len(filtered)} vagas encontradas")
st.caption(
    f"Período: {date_range[0].strftime('%d/%m/%Y')} → {date_range[1].strftime('%d/%m/%Y')}"
)
st.divider()

for _, row in filtered.iterrows():
    with st.container():
        st.markdown(f"### {row['title']}")

        salary = row["salary_rs"]
        salary_text = (
            f"R$ {salary:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            if pd.notna(salary)
            else "Não informado"
        )

        st.write(
            f"🏢 **{row['company_name'] or 'Não informado'}**  "
            f" — 📍 *{row['city_name'] or 'Não informado'}*  "
            f" — 💼 {row['employment_type'] or 'Não informado'}  "
            f" — 💰 {salary_text}"
        )

        if row["tech_stack"]:
            st.write(f"🧠 **Techs:** {', '.join(row['tech_stack'])}")

        st.caption(
            f"📅 Coletado em {row['collecting_date'].strftime('%d/%m/%Y')} · Fonte: {row['source']}"
        )

        st.link_button("🔗 Ver vaga", row["job_url"])
        st.divider()
