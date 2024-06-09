import os
from io import BytesIO

import boto3
import polars as pl
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")

DB_USER = os.getenv("LOCALIZA_POSTGRES_USER")
DB_PASSWORD = os.getenv("LOCALIZA_POSTGRES_PASSWORD")
DB_NAME = os.getenv("LOCALIZA_POSTGRES_DB")
DB_HOST = os.getenv("LOCALIZA_POSTGRES_HOST", "localhost")

DB_CONN_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_PASSWORD}"
TABLE_NAME = "tmp_fraud_credit"


def load_data():
    bucket_name = "bronze"
    file_key = "df_fraud_credit.csv"
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=None,
    )
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    return csv_string


def format_data(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(pl.from_epoch(pl.col("timestamp"), time_unit="s"))
    df = df.filter(pl.col("location_region") != "0")
    df = df.filter(pl.col("amount").is_not_null())
    df = df.filter(pl.col("risk_score").is_not_null())
    return df


csv_data = BytesIO(load_data().encode("utf-8"))

df = pl.read_csv(csv_data, null_values=["none"]).pipe(format_data)

df.write_database(
    table_name=TABLE_NAME,
    if_table_exists="replace",
    connection=DB_CONN_STRING,
    engine="adbc",
)
