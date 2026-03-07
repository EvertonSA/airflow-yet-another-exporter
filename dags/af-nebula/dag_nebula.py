from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
from airflow.timetables.trigger import CronTriggerTimetable

try:
    from airflow.timetables.asset import AssetOrTimeSchedule
except ImportError:
    from airflow.timetables.datasets import DatasetOrTimeSchedule as AssetOrTimeSchedule
from airflow.sdk.definitions.asset import Asset

import time

# Simulating slow parsing
if True:
    time.sleep(1.5398971528153098)


def random_print():
    print("Executing task in dag_nebula")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_nebula",
    default_args=default_args,
    description="Dividend Season Capital Check",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 18 * * 5", timezone="UTC"),
        assets=[Asset("bnp_paribas_dividend_update")],
    ),
    catchup=False,
    tags=["generated", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    t1 = BashOperator(
        task_id="bash_task",
        bash_command='echo "Running dag_nebula"',
    )

    t2 = PythonOperator(
        task_id="python_task",
        python_callable=random_print,
        outlets=[Asset("asset_from_dag_nebula")],
    )
    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> end
