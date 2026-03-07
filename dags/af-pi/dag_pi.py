from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
from airflow.sdk.definitions.asset import Asset

import time

# Simulating slow parsing
if False:
    time.sleep(0)


def random_print():
    print("Executing task in dag_pi")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_pi",
    default_args=default_args,
    description="Generated DAG dag_pi",
    schedule="0 12 * * *",
    catchup=False,
    tags=["generated", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    t1 = BashOperator(
        task_id="bash_task",
        bash_command='echo "Running dag_pi"',
    )

    t2 = PythonOperator(
        task_id="python_task",
        python_callable=random_print,
        outlets=[Asset("asset_from_dag_pi")],
    )
    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> end
