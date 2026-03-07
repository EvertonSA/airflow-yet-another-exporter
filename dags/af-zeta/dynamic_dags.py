from airflow.decorators import dag, task  # pylint: disable=no-name-in-module
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
from airflow.sdk.definitions.asset import Asset

import time

# Default arguments for all dynamic DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

time.sleep(1.5)


def create_dag(dag_id, default_args):
    """
    Function to generate a DAG definition using TaskFlow API.
    """

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Dynamically generated DAG {dag_id}",
        schedule="@daily",
        start_date=datetime(2025, 12, 1, tzinfo=pendulum.timezone("Europe/Paris")),
        catchup=False,
        tags=["generated", "valkyrie", "dynamic"],
    )
    def generated_dag():

        @task(task_id="python_task", outlets=[Asset(f"dynamic_asset_{dag_id}")])
        def random_print():
            print(f"Executing task in {dag_id}")

        t1 = BashOperator(
            task_id="bash_task",
            bash_command=f'echo "Running {dag_id}"',
        )

        # Define workflow
        t1 >> random_print()

    return generated_dag()


# Dynamically register 100 DAGs
for i in range(100):
    dag_id = f"valkyrie_{i}"
    # Assign the DAG object to a global variable so Airflow picks it up
    globals()[dag_id] = create_dag(dag_id=dag_id, default_args=default_args)
