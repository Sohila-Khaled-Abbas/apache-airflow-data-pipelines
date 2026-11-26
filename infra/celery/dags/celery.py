"""
Celery Multi-Queue Task Routing DAG (Airflow 2.10.5 Compose Stack)

Demonstrates routing tasks to specialized Celery worker queues:
  - Tasks ``a``, ``c``, and ``d`` run on the ``cpu`` queue (``airflow-worker-2``).
  - Task ``b`` runs on the ``gpu`` queue (``airflow-worker``).

Note: Uses ``airflow.decorators`` for compatibility with the ``apache/airflow:2.10.5``
container image configured in ``infra/celery/docker-compose.yaml``.
"""

from datetime import timedelta
from time import sleep

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["executors", "celery", "queues", "demo"],
    description="Demonstrates multi-queue Celery task routing across cpu and gpu workers",
)
def celery():
    """Orchestrate fan-out tasks routed across dedicated cpu and gpu Celery queues."""

    @task(queue="cpu")
    def a() -> None:
        """Initial CPU-bound setup step."""
        print("A")
        sleep(15)

    @task(queue="gpu")
    def b() -> None:
        """Parallel task routed to the GPU worker queue."""
        print("B")
        sleep(15)

    @task(queue="cpu")
    def c() -> None:
        """Parallel task routed to the CPU worker queue."""
        print("C")
        sleep(15)

    @task(queue="cpu")
    def d() -> None:
        """Parallel task routed to the CPU worker queue."""
        print("D")
        sleep(15)

    a() >> [b(), c(), d()]


celery()
