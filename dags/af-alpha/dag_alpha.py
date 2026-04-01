from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.sdk.definitions.asset import Asset

import time

# Simulating slow parsing
if False:
    time.sleep(0)


def random_print(**kwargs):
    print("Executing task in 'random_print'")
    print("--- DAG Run Metadata ---")
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    print("------------------------")
    print("Executing task in dag_alpha")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_alpha",
    default_args=default_args,
    description="Intraday Liquidity Sweeper",
    schedule=DeltaTriggerTimetable(timedelta(minutes=1)),
    catchup=False,
    tags=["generated", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    t2 = PythonOperator(
        task_id="python_task",
        python_callable=random_print,
        outlets=[Asset("asset_from_dag_alpha")],
    )
    end = EmptyOperator(task_id="end")

    start >> t2 >> end
