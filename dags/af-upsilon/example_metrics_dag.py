from airflow.decorators import dag  # pylint: disable=no-name-in-module
from datetime import datetime, timedelta
import pendulum

# @task
# def random_failure():
#     time.sleep(random.randint(1, 5))
#     if random.random() < 0.3:
#         raise Exception("Random failure for metrics testing!")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


@dag(
    "example_metrics_dag",
    default_args=default_args,
    description="A DAG to generate metrics for the exporter",
    schedule="0 8 * * *",  # generic catchall,
    start_date=datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    catchup=False,
    tags=["example", "metrics"],
)
def example_metrics_dag(**kwargs):
    print("Executing task in 'example_metrics_dag'")
    print("--- DAG Run Metadata ---")
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    print("------------------------")

    # t3 = random_failure()


example_metrics_dag()
