from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.variables import load_env_vars

def main(
    spark: SparkSession,
    bucket_name: str,
    today_str: str
):
    df_programathor = spark.read.parquet(
        f"s3a://{bucket_name}/silver/normalized/programathor/dt={today_str}"
    )
    df_vagascom = spark.read.parquet(
        f"s3a://{bucket_name}/silver/normalized/vagascom/dt={today_str}"
    )
    df_nerdin = spark.read.parquet(
        f"s3a://{bucket_name}/silver/normalized/nerdin/dt={today_str}"
    )

    df = (
        df_programathor
        .unionByName(df_vagascom, allowMissingColumns=True)
        .unionByName(df_nerdin, allowMissingColumns=True)
    )

    df = (
        df
        .filter(F.col("title").isNotNull())
        .filter(F.col("company").isNotNull())
        .dropDuplicates(["title", "company", "city"])
    )

    df = df.withColumn(
        "job_text",
        F.concat_ws(
            "\n",
            F.concat(F.lit("Vaga: "), F.col("title")),
            F.concat(F.lit("Empresa: "), F.col("company")),
            F.concat(F.lit("Cidade: "), F.col("city")),
            F.concat(F.lit("Tipo: "), F.col("employment_type")),
            F.concat(F.lit("Senioridade: "), F.col("seniority")),
            F.concat(F.lit("Tecnologias: "), F.concat_ws(", ", F.col("tech_stack")))
        )
    )

    df = df.withColumn(
        "tech_stack_normalized",
        F.array_distinct(
            F.transform(F.col("tech_stack"), lambda x: F.lower(x))
        )
    )

    df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/gold/jobs/{today_str}.json"
    )

    df.select("job_id", "job_text").write.mode("overwrite").json(
        f"s3a://{bucket_name}/gold/jobs_llm/dt={today_str}"
    )

if __name__ == "__main__":
    access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
    today_str = datetime.today().strftime("%Y-%m-%d")

    spark = (
        SparkSession.builder
        .appName("silver_normalization")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.698"
        )
        .config("spark.hadoop.fs.s3a.endpoint", bucket_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    main(
        spark=spark,
        bucket_name=bucket_name,
        today_str=today_str
    )