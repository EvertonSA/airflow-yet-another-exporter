
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import time
import random

# Simulating slow parsing
if False:
    time.sleep(0)

def random_print():
    print(f"Executing task in dag_omega")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_omega',
    default_args=default_args,
    description='Generated DAG dag_omega',
    schedule_interval=timedelta(hours=random.randint(1, 24)),
    catchup=False,
    tags=['generated', 'test'],
) as dag:

    start = EmptyOperator(task_id='start')

    t1 = BashOperator(
        task_id='bash_task',
        bash_command='echo "Running dag_omega"',
    )

    t2 = PythonOperator(
        task_id='python_task',
        python_callable=random_print,
    )

    t3 = BashOperator(
        task_id='potentially_fail',
        bash_command='if [ $((RANDOM % 10)) -gt 7 ]; then exit 1; else exit 0; fi',
    )

    end = EmptyOperator(task_id='end')

    start >> t1 >> t2 >> t3 >> end
