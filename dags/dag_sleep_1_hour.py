"""
DAG that runs a Python method sleeping every 10 seconds and printing 'hello' for 1 hour.
"""

import time
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="sleep_1_hour_dag",
    schedule=None,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["test", "long_running"],
)
def sleep_1_hour_dag():
    
    @task
    def sleep_and_print():
        # 1 hour = 3600 seconds = 360 iterations of 10s
        steps = 360
        for i in range(steps):
            print(f"[{i+1}/{steps}] hello")
            time.sleep(10)
            
    sleep_and_print()

sleep_1_hour_dag()
