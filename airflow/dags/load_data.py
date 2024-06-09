import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator

insert_new_data = """
WITH max_timestamp AS (
    SELECT MAX("timestamp") AS max_ts
    FROM public.fraud_credit
)
INSERT INTO public.fraud_credit (
    "timestamp",
    sending_address,
    receiving_address,
    amount,
    transaction_type,
    location_region,
    ip_prefix,
    login_frequency,
    session_duration,
    purchase_pattern,
    age_group,
    risk_score,
    anomaly
)
select
    t."timestamp",
    t.sending_address,
    t.receiving_address,
    t.amount,
    t.transaction_type,
    t.location_region,
    t.ip_prefix,
    t.login_frequency,
    t.session_duration,
    t.purchase_pattern,
    t.age_group,
    t.risk_score,
    t.anomaly
FROM
    public.tmp_fraud_credit t
WHERE
    t."timestamp" > COALESCE((SELECT max_ts FROM max_timestamp), '1800-01-01 00:00:00'::timestamp);
"""

with DAG(
    dag_id="load_data",
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 6, tz="UTC"),
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )
    end = EmptyOperator(
        task_id="end",
    )

    save_data_s3 = DockerOperator(
        task_id="save_data_s3",
        api_version="auto",
        auto_remove=True,
        docker_url="TCP://docker-socket-proxy:2375",
        image="elt_localiza",
        tty=True,
        mount_tmp_dir=False,
        xcom_all=False,
        network_mode="container:minio",
        command="save_data.py",
        environment={
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "p@ssw0rd",
            "AWS_ENDPOINT": "http://minio:9000",
        },
    )
    load_tmp_data = DockerOperator(
        task_id="load_tmp_data",
        api_version="auto",
        auto_remove=True,
        docker_url="TCP://docker-socket-proxy:2375",
        image="elt_localiza",
        tty=True,
        mount_tmp_dir=False,
        xcom_all=False,
        network_mode="container:minio",
        command="load_data.py",
        environment={
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "p@ssw0rd",
            "AWS_ENDPOINT": "http://minio:9000",
            "LOCALIZA_POSTGRES_USER": "localiza",
            "LOCALIZA_POSTGRES_PASSWORD": "localiza",
            "LOCALIZA_POSTGRES_DB": "localiza",
            "LOCALIZA_POSTGRES_HOST": "localiza_db",
        },
    )

    update_data = SQLExecuteQueryOperator(
        task_id="update_fraud_credit_table",
        sql=insert_new_data,
    )

    start >> save_data_s3 >> load_tmp_data >> update_data >> end
