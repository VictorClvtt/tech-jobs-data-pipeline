from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def data_modeling(
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
        "tech_stack_normalized",
        F.array_distinct(
            F.transform(F.col("tech_stack"), lambda x: F.lower(x))
        )
    )

    # Modeling
    job_df = (
        df
        .withColumn(
            "company_id",
            F.sha2(F.col("company"), 256)
        )
        .withColumn(
            "city_id",
            F.sha2(F.col("city"), 256)
        )
        .select(
            "job_id",
            "company_id",
            "city_id",
            "employment_type",
            "job_url",
            "source",
            "salary_rs",
            "title",
            "modality",
            "collecting_date",
            "seniority"
        )
    )

    company_df = (
    df
        .select(
            F.col("company").alias("company_name")
        )
        .distinct()
        .withColumn(
            "company_id",
            F.sha2(F.col("company_name"), 256)
        )
    )

    city_df = (
    df
        .select(
            F.col("city").alias("city_name")
        )
        .distinct()
        .withColumn(
            "city_id",
            F.sha2(F.col("city_name"), 256)
        )
    )

    job_tech_exploded = (
        df
        .select(
            "job_id",
            F.explode("tech_stack_normalized").alias("technology_name")
        )
        .filter(F.col("technology_name").isNotNull())
    )

    technology_df = (
        job_tech_exploded
        .select("technology_name")
        .distinct()
        .withColumn(
            "technology_id",
            F.sha2(F.col("technology_name"), 256)
        )
    )

    job_technology_df = (
        job_tech_exploded
        .join(
            technology_df,
            on="technology_name",
            how="inner"
        )
        .select(
            "job_id",
            "technology_id"
        )
        .dropDuplicates()
    )

    # Writing data
    job_df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/gold/3fn/job/dt={today_str}"
    )

    company_df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/gold/3fn/company/dt={today_str}"
    )

    city_df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/gold/3fn/city/dt={today_str}"
    )

    technology_df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/gold/3fn/technology/dt={today_str}"
    )

    job_technology_df.write.mode("overwrite").parquet(
        f"s3a://{bucket_name}/gold/3fn/job_technology/dt={today_str}"
    )

