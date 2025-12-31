from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import time

def random_failure():
    time.sleep(random.randint(1, 5))
    if random.random() < 0.3:
        raise Exception("Random failure for metrics testing!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'example_metrics_dag',
    default_args=default_args,
    description='A DAG to generate metrics for the exporter',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    tags=['example', 'metrics'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )
    
    t3 = PythonOperator(
        task_id='random_fail',
        python_callable=random_failure,
    )

    t1 >> [t2, t3]
