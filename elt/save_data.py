import logging
import os

import boto3
from dotenv import load_dotenv

# Configuração do logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localhost:9000")


def init_s3_resource():
    logging.info("Inicializando o recurso S3 do boto3.")
    return boto3.resource(
        "s3",
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=None,
    )


def upload_file(s3_resource, bucket_name, file_path):
    file_key = os.path.basename(file_path)
    logging.info(
        f"Iniciando upload do arquivo {file_path} para o bucket {bucket_name} com a chave {file_key}."
    )
    try:
        s3_resource.Bucket(bucket_name).upload_file(file_path, file_key)
        logging.info(
            f"Arquivo {file_key} enviado com sucesso para o bucket {bucket_name}."
        )
    except Exception as e:
        logging.error(f"Erro ao enviar o arquivo: {e}")


if __name__ == "__main__":
    s3_resource = init_s3_resource()
    bucket_name = "bronze"
    file_path = "data/df_fraud_credit.csv"
    upload_file(s3_resource, bucket_name, file_path)
