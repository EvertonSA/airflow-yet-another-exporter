import os
import random
import string
import time

DAGS_DIR = "dags"
NUM_FOLDERS = 20

# List of random names for variety
NAMES = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
    "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho",
    "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega", "zenith",
    "quasars", "pulsar", "nebula", "galaxy", "supernova"
]

def generate_random_string(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def create_dag_content(dag_id, slow_parse=False):
    sleep_time = 0
    if slow_parse:
        sleep_time = random.uniform(0.5, 2.0)

    content = f"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import time
import random

# Simulating slow parsing
if {slow_parse}:
    time.sleep({sleep_time})

def random_print():
    print(f"Executing task in {dag_id}")

default_args = {{
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

with DAG(
    '{dag_id}',
    default_args=default_args,
    description='Generated DAG {dag_id}',
    schedule_interval=timedelta(hours=random.randint(1, 24)),
    catchup=False,
    tags=['generated', 'test'],
) as dag:

    start = EmptyOperator(task_id='start')

    t1 = BashOperator(
        task_id='bash_task',
        bash_command='echo "Running {dag_id}"',
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
"""
    return content

def main():
    if not os.path.exists(DAGS_DIR):
        os.makedirs(DAGS_DIR)

    # Shuffle names to pick random 20 unique ones easily if list is long enough, 
    # or just pick randomly.
    selected_names = random.sample(NAMES, NUM_FOLDERS) if len(NAMES) >= NUM_FOLDERS else \
                     [f"{random.choice(NAMES)}_{generate_random_string()}" for _ in range(NUM_FOLDERS)]

    for name in selected_names:
        folder_name = f"af-{name}"
        folder_path = os.path.join(DAGS_DIR, folder_name)
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"Created {folder_path}")

        # Decide if this DAG file is slow to parse (20% chance)
        is_slow = random.random() < 0.2
        
        dag_id = f"dag_{name.replace('-', '_')}"
        file_content = create_dag_content(dag_id, slow_parse=is_slow)
        
        file_path = os.path.join(folder_path, f"{dag_id}.py")
        with open(file_path, "w") as f:
            f.write(file_content)
            print(f"  Generated DAG {file_path} (Slow Parse: {is_slow})")

if __name__ == "__main__":
    main()
