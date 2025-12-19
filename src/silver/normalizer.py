from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def normalizer(raw_jobs: list[dict], spark: SparkSession):
    job_raw_schema = StructType([
        StructField("title_raw", StringType(), True),
        StructField("company_raw", StringType(), True),
        StructField("location_raw", StringType(), True),
        StructField("salary_raw", StringType(), True),
        StructField("seniority_raw", StringType(), True),
        StructField("employment_type_raw", StringType(), True),
        StructField("job_url_raw", StringType(), True),
        StructField("tech_stack_raw", ArrayType(StringType()), True),
        StructField("collecting_date_raw", StringType(), True),
        StructField("source", StringType(), True)
    ])

    df = spark.createDataFrame(raw_jobs, schema=job_raw_schema)

    df = (
        df.withColumn(
            "tech_stack",
            F.when(
                F.col("tech_stack_raw").isNull() | (F.size("tech_stack_raw") == 0),
                F.lit(None)
            ).otherwise(F.col("tech_stack_raw"))
        )
        .withColumn(
            "salary_rs",
            F.coalesce(
                F.when(
                    F.col("salary_raw").isNotNull() & (F.trim(F.col("salary_raw")) != ""),
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_extract(
                                F.col("salary_raw"),
                                r"R\$?\s*([\d\.,]+)",
                                1
                            ),
                            r"\.", ""
                        ),
                        r",", "."
                    ).cast("double")
                ),
                F.lit(0.0)
            )
        )
        .withColumn(
            "title",
            F.trim(F.regexp_replace("title_raw", r"\s+", " "))
        )
        .withColumn(
            "city",
            F.when(
                F.col("location_raw").isNull(),
                F.lit("Não especificada")
            )
            .when(
                F.lower(F.col("location_raw"))
                .rlike("remoto|remote|home\\s*office|anywhere|100%\\s*remote"),
                F.lit("Não especificada")
            )
            .otherwise(
                F.trim(
                    F.regexp_replace(
                        # agora corta em (, -, | ou /
                        F.regexp_extract(
                            F.col("location_raw"),
                            r"^([^(\-\|\/]+)",
                            1
                        ),
                        r"\s{2,}",
                        " "
                    )
                )
            )
        )
        .withColumn(
            "modality",
            F.when(
                F.lower(F.col("location_raw"))
                .rlike("remoto|remote|home\\s*office|anywhere|100%\\s*remote"),
                "remoto"
            )
            .when(
                F.lower(F.col("location_raw"))
                .rlike("h[ií]brido|hybrid|flex"),
                "hibrido"
            )
            .when(
                F.lower(F.col("location_raw"))
                .rlike("presencial|on[-\\s]?site|in[-\\s]?office"),
                "presencial"
            )
            .otherwise("presencial")
        )
        .withColumn(
            "collecting_date",
            F.to_date("collecting_date_raw")
        )
        .withColumn(
            "employment_type",
            F.when(
                F.col("employment_type_raw").isNull(),
                F.lit("Não especificado")
            ).otherwise(F.col("employment_type_raw"))
        )
        .withColumn(
            "seniority",
            F.when(
                F.lower(F.col("seniority_raw")).rlike(r"\b(jr|júnior|junior)\b"),
                "Junior"
            )
            .when(
                F.lower(F.col("seniority_raw")).rlike(r"\b(pl|pleno|mid|middle)\b"),
                "Pleno"
            )
            .when(
                F.lower(F.col("seniority_raw")).rlike(r"\b(sr|sênior|senior)\b"),
                "Senior"
            )
            .when(
                F.lower(F.col("title"))
                .rlike(r"\b(jr|júnior|junior)\b"),
                "Junior"
            )
            .when(
                F.lower(F.col("title"))
                .rlike(r"\b(pl|pleno|mid|middle)\b"),
                "Pleno"
            )
            .when(
                F.lower(F.col("title"))
                .rlike(r"\b(sr|sênior|senior)\b"),
                "Senior"
            )
            .otherwise("Não especificado")
        )
        .withColumn("tech", F.explode_outer("tech_stack"))
        .withColumn(
            "tech",
            F.when(
                F.col("tech").isNull(),
                F.lit("Não especificado")
            ).otherwise(F.col("tech"))
        )
        .withColumnRenamed("company_raw", "company")
        .withColumnRenamed("job_url_raw", "job_url")
        .withColumn(
            "job_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("title"),
                    F.col("company"),
                    F.col("city"),
                    F.col("job_url")
                ),
                256
            )
        )
        .drop("tech_stack_raw", "salary_raw", "title_raw", "location_raw", "collecting_date_raw", "tech_stack", "employment_type_raw", "seniority_raw")
        .dropna()
        .drop_duplicates()
    )

    return df