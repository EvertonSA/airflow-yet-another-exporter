from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import random
import time

# Default arguments for all dynamic DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dag(dag_id, schedule_hours, default_args):
    """
    Function to generate a DAG definition using TaskFlow API.
    """
    
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Dynamically generated DAG {dag_id}',
        schedule=timedelta(hours=schedule_hours),
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['generated', 'valkyrie', 'dynamic'],
    )
    def generated_dag():
        
        @task(task_id='python_task')
        def random_print():
            print(f"Executing task in {dag_id}")

        @task.branch(task_id='potentially_fail')
        def potentially_fail():
            # Simulate a condition that might fail or lead to a specific path
            if random.randint(1, 100) > 80:
                raise Exception(f"Task in {dag_id} failed randomly!")
            return 'end'

        t1 = BashOperator(
            task_id='bash_task',
            bash_command=f'echo "Running {dag_id}"',
        )
        
        # Define workflow
        t1 >> random_print() >> potentially_fail()

    return generated_dag()

# Dynamically register 100 DAGs
for i in range(100):
    dag_id = f'valkyrie_{i}'
    # Assign the DAG object to a global variable so Airflow picks it up
    globals()[dag_id] = create_dag(
        dag_id=dag_id,
        schedule_hours=random.randint(1, 24),
        default_args=default_args
    )
