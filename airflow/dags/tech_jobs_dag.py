from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from include.src.bronze.run_collector import run_collector
from include.src.silver.parsers import nerdin_parser, programathor_parser, vagascom_parser, run_parser

from include.src.utils.variables import load_env_vars
from include.config.sources import sources

access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()
today_str = datetime.today().strftime("%Y-%m-%d")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="tech_jobs_dag",
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["spark"]
)
def tech_jobs_dag():
    collector_tasks = []

    for source in sources:
        task = PythonOperator(
            task_id=f"collect_{source['name'].lower().replace('.', '')}",
            python_callable=run_collector,
            op_kwargs={
                "source": source,
                "access_key": access_key,
                "secret_key": secret_key,
                "bucket_name": bucket_name,
                "bucket_endpoint": bucket_endpoint,
                "today_str": today_str,
            },
        )

        collector_tasks.append(task)

    parse_nerdin = PythonOperator(
        task_id="parse_nerdin",
        python_callable=run_parser,
        op_kwargs={
            "parser_func": nerdin_parser,
            "source_name": "nerdin",
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket_name,
            "bucket_endpoint": bucket_endpoint,
            "today_str": today_str,
        },
    )

    parse_programathor = PythonOperator(
        task_id="parse_programathor",
        python_callable=run_parser,
        op_kwargs={
            "parser_func": programathor_parser,
            "source_name": "programathor",
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket_name,
            "bucket_endpoint": bucket_endpoint,
            "today_str": today_str,
        },
    )

    parse_vagascom = PythonOperator(
        task_id="parse_vagascom",
        python_callable=run_parser,
        op_kwargs={
            "parser_func": vagascom_parser,
            "source_name": "vagascom",
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket_name,
            "bucket_endpoint": bucket_endpoint,
            "today_str": today_str,
        },
    )

    normalize_programathor = SparkSubmitOperator(
        task_id="normalize_programathor",
        conn_id="my_spark_conn",
        application="include/src/silver/run_silver_normalizer.py",
        application_args=["--source", "programathor"],
        conf={
            "spark.hadoop.fs.s3a.endpoint": bucket_endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.671",
        verbose=True,
    )

    normalize_nerdin = SparkSubmitOperator(
        task_id="normalize_nerdin",
        conn_id="my_spark_conn",
        application="include/src/silver/run_silver_normalizer.py",
        application_args=["--source", "nerdin"],
        conf={
            "spark.hadoop.fs.s3a.endpoint": bucket_endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.671",
        verbose=True,
    )

    normalize_vagascom = SparkSubmitOperator(
        task_id="normalize_vagascom",
        conn_id="my_spark_conn",
        application="include/src/silver/run_silver_normalizer.py",
        application_args=["--source", "vagascom"],
        conf={
            "spark.hadoop.fs.s3a.endpoint": bucket_endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.671",
        verbose=True,
    )

    unify_and_model = SparkSubmitOperator(
        task_id="unify_and_model",
        conn_id="my_spark_conn",
        application="include/src/gold/run_gold.py",
        conf={
            "spark.hadoop.fs.s3a.endpoint": bucket_endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.671",
        verbose=True,
    )

    collector_tasks[0] >> parse_programathor >> normalize_programathor
    collector_tasks[1] >> parse_vagascom >> normalize_vagascom
    collector_tasks[2] >> parse_nerdin >> normalize_nerdin

    [normalize_programathor, normalize_vagascom, normalize_nerdin] >> unify_and_model


tech_jobs_dag()