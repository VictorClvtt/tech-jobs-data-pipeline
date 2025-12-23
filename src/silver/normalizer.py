from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def normalizer(df: DataFrame) -> DataFrame:
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
                None
            )
            .when(
                F.lower(F.col("location_raw"))
                .rlike("remoto|remote|home\\s*office|anywhere|100%\\s*remote"),
                None
            )
            .otherwise(
                F.trim(
                    F.regexp_replace(
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
                F.col("location_raw").isNull(),
                None
            )
            .when(
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
            .otherwise(F.lit("presencial"))
        )
        .withColumn(
            "collecting_date",
            F.to_date("collecting_date_raw")
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
            .otherwise(None)
        )
        .withColumnRenamed("company_raw", "company")
        .withColumnRenamed("employment_type_raw", "employment_type")
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
        .drop("tech_stack_raw", "salary_raw", "title_raw", "location_raw", "collecting_date_raw", "employment_type_raw", "seniority_raw")
        .dropna(subset=["job_id"])
        .drop_duplicates(subset=["job_id"])
    )

    return df