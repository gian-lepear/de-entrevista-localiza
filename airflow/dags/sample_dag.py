import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_dag",
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 6, tz="UTC"),
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    test_task = BashOperator(
        task_id="test_task",
        bash_command="echo https://airflow.apache.org/",
    )
    end = EmptyOperator(
        task_id="end",
    )

    start >> test_task >> end
