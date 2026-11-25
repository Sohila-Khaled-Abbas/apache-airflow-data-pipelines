"""
Celery & Distributed Parallelism Demonstration DAG
Demonstrates fan-out concurrent execution across worker processes or distributed
Celery queues, converging back to a downstream aggregation step.

This DAG is designed to work with the CeleryExecutor configuration in
infra/celery/docker-compose.yaml. To run with Celery:

    # Using the included Celery stack
    cd infra/celery
    docker-compose up -d

    # Or with Airflow directly
    export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
    airflow celery worker
"""

from datetime import timedelta
from time import sleep
from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["executors", "celery", "concurrency", "demo"],
    description="Demonstrates parallel fan-out task execution for Celery/Local executor benchmarking",
)
def celery_parallelism_demo():

    @task()
    def prepare_batch() -> str:
        """Prepare initial work unit before parallel fan-out."""
        print("Preparing batch for parallel execution...")
        return "batch_ready"

    @task()
    def worker_alpha() -> str:
        """Simulate independent parallel worker task A."""
        print("Running worker_alpha")
        sleep(5)
        return "alpha_done"

    @task()
    def worker_beta() -> str:
        """Simulate independent parallel worker task B."""
        print("Running worker_beta")
        sleep(5)
        return "beta_done"

    @task()
    def worker_gamma() -> str:
        """Simulate independent parallel worker task C."""
        print("Running worker_gamma")
        sleep(5)
        return "gamma_done"

    @task()
    def aggregate_results(results: list[str]) -> None:
        """Aggregate completion statuses after parallel tasks finish."""
        print(f"All parallel branches finished: {results}")

    start = prepare_batch()
    branches = [worker_alpha(), worker_beta(), worker_gamma()]
    start >> branches >> aggregate_results(branches)


celery_parallelism_demo()
