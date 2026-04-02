from airflow.decorators import dag, task  # pylint: disable=no-name-in-module
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
        def random_print(**kwargs):
            print(f"Executing task in '{dag_id}'")
            print("--- DAG Run Metadata ---")
            for key, value in kwargs.items():
                print(f"{key}: {value}")
            print("------------------------")

        # Define workflow
        random_print()

    return generated_dag()


# Dynamically register 100 DAGs
for i in range(1):
    dag_id = f"valkyrie_{i}"
    # Assign the DAG object to a global variable so Airflow picks it up
    globals()[dag_id] = create_dag(dag_id=dag_id, default_args=default_args)
