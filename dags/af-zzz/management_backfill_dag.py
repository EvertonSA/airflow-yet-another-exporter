from datetime import datetime, timedelta
import pendulum
import subprocess
import logging
import os

from airflow.decorators import dag, task  # pylint: disable=no-name-in-module
from airflow.models import DagBag

# Logger for the DAG
logger = logging.getLogger("airflow.task")


@task
def get_all_dag_ids(**context) -> list[str]:
    """
    Retrieves all DAG IDs from the Airflow DagBag.
    Returns a list of DAG IDs to be used for dynamic task mapping.
    """
    logger.info("Instantiating DagBag to fetch all DAG IDs...")
    dagbag = DagBag()
    all_dag_ids = list(dagbag.dag_ids)

    # Remove this management DAG from the list to prevent recursive backfills
    current_dag_id = context["dag"].dag_id
    if current_dag_id in all_dag_ids:
        all_dag_ids.remove(current_dag_id)

    logger.info(f"Found {len(all_dag_ids)} DAGs to backfill.")
    return all_dag_ids


@task
def trigger_backfill(dag_id: str):
    """
    Triggers a backfill for a single DAG for the last 30 days using the Airflow CLI.
    This task will run in parallel for every DAG ID passed to it.
    """
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting backfill for DAG: {dag_id}")
    logger.info(f"Backfill range: {start_date} to {end_date}")

    # In Airflow 3, the CLI command is `airflow backfill create`
    cmd = [
        "airflow",
        "backfill",
        "create",
        "--dag-id",
        dag_id,
        "--from-date",
        start_date,
        "--to-date",
        end_date,
    ]

    env = os.environ.copy()
    # Airflow 3 workers don't have DB access by default, but the CLI needs it
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
        "postgresql+psycopg2://airflow:airflow_password@postgres/airflow"
    )

    try:
        # Popen or subprocess.run natively pauses the Airflow Task until it completes.
        # This gives us the "wait until finished" behavior inherently.
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True, env=env
        )
        logger.info(f"Successfully finished backfill queueing for {dag_id}.")
        logger.debug(f"Output: {result.stdout}")
        return f"Success: {dag_id}"
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to queue backfill for {dag_id}. Error: {e.stderr}")
        # Raising the exception intentionally marks this specific mapped task as failed in the UI
        raise RuntimeError(f"Backfill failed for {dag_id}. See logs for details.")


@dag(
    dag_id="management_trigger_all_backfills",
    description="Dynamically maps and triggers 30-day parallel backfills for all DAGs",
    schedule=None,  # Trigger manually only
    start_date=datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    catchup=False,
    tags=["management", "backfill", "parallel"],
    # Ensure our Celery workers aren't completely overwhelmed (optional limit)
    max_active_tasks=20,
)
def management_backfill_dag(**kwargs):
    print("Executing task in 'management_backfill_dag'")
    print("--- DAG Run Metadata ---")
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    print("------------------------")

    # 1. Fetch the list of all DAG IDs
    dag_ids = get_all_dag_ids()

    # 2. Dynamically map the backfill task to run in parallel for every DAG ID
    trigger_backfill.expand(dag_id=dag_ids)


# Instantiate the DAG
management_backfill_dag_instance = management_backfill_dag()
