from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
import time
from airflow.sdk.definitions.asset import Asset
from airflow.timetables.base import DataInterval, TimeRestriction, DagRunInfo
from airflow.timetables.interval import CronDataIntervalTimetable
from typing import Optional


def get_dutch_holidays(year: int) -> set:
    """Returns a set of dates (YYYY-MM-DD string) of Dutch holidays for a given year."""
    import holidays

    nl_holidays = holidays.NL(years=year)  # pylint: disable=no-member
    return set(d.strftime("%Y-%m-%d") for d in nl_holidays.keys())


class HolidayTimetable(CronDataIntervalTimetable):
    """Custom Timetable skipping Dutch official holidays."""

    def __init__(self, cron: str, timezone: str):
        super().__init__(cron, timezone)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        info = super().next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )
        while info is not None:
            # Skip if the logical date (data_interval start) falls on a holiday
            run_date = info.data_interval.start.in_timezone(self._timezone).date()
            dutch_holidays = get_dutch_holidays(run_date.year)

            if run_date.strftime("%Y-%m-%d") not in dutch_holidays:
                break

            info = super().next_dagrun_info(
                last_automated_data_interval=info.data_interval,
                restriction=restriction,
            )

        return info


# Simulating slow parsing
if True:
    time.sleep(1.2)  # Parsing latency


def random_print():
    print("Executing task in dag_phi")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_phi",
    default_args=default_args,
    description="Generated DAG dag_phi",
    schedule=HolidayTimetable("0 9 * * *", "America/New_York"),
    catchup=False,
    tags=["generated", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    t1 = BashOperator(
        task_id="bash_task",
        bash_command='echo "Running dag_phi"',
    )

    t2 = PythonOperator(
        task_id="python_task",
        python_callable=random_print,
        outlets=[Asset("asset_from_dag_phi")],
    )
    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> end
