from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
from airflow.timetables.trigger import MultipleCronTriggerTimetable

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
    print("Executing task in dag_chi")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_chi",
    default_args=default_args,
    description="ECB Volatility Sniper",
    schedule=MultipleCronTriggerTimetable("45 13 * * 4", "30 14 * * 4", timezone="CET"),
    catchup=False,
    tags=["generated", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    t2 = PythonOperator(
        task_id="python_task",
        python_callable=random_print,
    )
    end = EmptyOperator(task_id="end")

    start >> t2 >> end
