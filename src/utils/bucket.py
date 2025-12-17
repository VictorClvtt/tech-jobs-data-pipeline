import json
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.utils import AnalysisException


def create_bucket_if_not_exists(bucket_endpoint, access_key, secret_key, bucket_name):
    s3 = boto3.client(
        "s3",
        endpoint_url=bucket_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3.head_bucket(Bucket=bucket_name)

    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            print(f"[INFO] Bucket '{bucket_name}' not found. Creating bucket...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"[INFO] Bucket '{bucket_name}' successfully created.")
        else:
            print(f"[ERROR] Failed to check bucket '{bucket_name}'.")
            raise e


def file_to_bucket(bucket_endpoint, access_key, secret_key, bucket_name, path, data):

    create_bucket_if_not_exists(bucket_endpoint, access_key, secret_key, bucket_name)

    s3 = boto3.client(
        "s3",
        endpoint_url=bucket_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    if isinstance(data, bytes):
        body = data
    else:
        body = json.dumps(data, indent=2).encode("utf-8")

    s3.put_object(
        Bucket=bucket_name,
        Key=path,
        Body=body
    )

    print(f"[INFO] File successfully saved to s3://{bucket_name}/{path}")

def read_file_from_bucket(bucket_endpoint, access_key, secret_key, bucket_name, path):
    s3 = boto3.client(
        "s3",
        endpoint_url=bucket_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        response = s3.get_object(Bucket=bucket_name, Key=path)
        body = response["Body"].read()
        return body

    except ClientError as e:
        print(f"[ERROR] Failed to read file s3://{bucket_name}/{path}")
        raise e

def df_to_bucket(df, path: str, partition_by: str = None, mode: str = "append"):
    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.parquet(path)

    print(f"[INFO] DataFrame written to {path} | Mode: {mode}")


def save_or_update_table(df_new, path, dedup_cols, format="parquet", mode="overwrite"):
    spark = df_new.sparkSession

    print(f"[INFO] Checking if table exists at: {path}")

    try:
        df_existing = spark.read.format(format).load(path)
        table_exists = True
        print("[INFO] Existing table found. Merging and deduplicating data.")
    except AnalysisException:
        df_existing = None
        table_exists = False
        print("[INFO] Table not found. Creating a new one.")

    if table_exists:
        df_final = (
            df_existing
            .unionByName(df_new, allowMissingColumns=True)
            .dropDuplicates(dedup_cols)
        )
    else:
        df_final = df_new

    df_final.write.format(format).mode(mode).save(path)

    print(f"[INFO] Table successfully saved/updated at: {path}")
