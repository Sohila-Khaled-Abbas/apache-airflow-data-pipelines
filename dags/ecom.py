"""
E-Commerce Downstream Event-Driven DAG
Demonstrates data-aware scheduling triggered automatically whenever
Dataset_cocktail is updated by an upstream producer DAG.
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from pendulum import datetime, duration

from include.datasets import Dataset_cocktail


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[Dataset_cocktail],
    catchup=False,
    description="Processes ecommerce data downstream of Dataset_cocktail updates",
    tags=["team_a", "ecom", "etl"],
    default_args={"retries": 2},
    dagrun_timeout=duration(minutes=20),
    max_consecutive_failed_dag_runs=2,
)
def ecom():
    EmptyOperator(task_id="task_a")


ecom()
