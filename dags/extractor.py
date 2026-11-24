"""
Cocktail Data Extraction Pipeline
Extracts random cocktail records from TheCocktailDB API, validates JSON structure
and payload size, dynamically branches based on alcohol classification, and emits
Dataset_cocktail to trigger downstream consumer DAGs.
"""

from datetime import timedelta
import json
import os
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task, task_group
from airflow.task.trigger_rule import TriggerRule
from pendulum import datetime

from include.datasets import Dataset_cocktail
from include.extractor.callbacks import (
    _handle_failed_dag_run,
    _handle_success_dag_run,
    handle_empty_size,
)
from include.tasks import _check_json_data, _check_size, _get_cocktail


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=2),
    },
    tags=["team_a", "extractor", "etl"],
    on_success_callback=_handle_success_dag_run,
    on_failure_callback=_handle_failed_dag_run,
    description="Extracts cocktail data from external API and produces Dataset_cocktail",
)
def extractor():

    @task_group(
        default_args={
            "retries": 2,
            "retry_delay": timedelta(seconds=2),
        }
    )
    def check_data():
        check_json_data = PythonOperator(
            task_id="check_json_data",
            python_callable=_check_json_data,
            inlets=[Dataset_cocktail],
        )

        check_size = PythonOperator(
            task_id="check_size",
            python_callable=_check_size,
            inlets=[Dataset_cocktail],
            on_execute_callback=handle_empty_size,
        )

        @task.branch()
        def branch_cocktail_type() -> str:
            """Branch based on whether the extracted cocktail is alcoholic."""
            if not os.path.exists(Dataset_cocktail.uri):
                raise FileNotFoundError(f"Dataset file {Dataset_cocktail.uri} does not exist")

            with open(Dataset_cocktail.uri, "r", encoding="utf-8") as f:
                data = json.load(f)

            drinks = data.get("drinks", [])
            if not drinks:
                raise ValueError("No drink data found in dataset JSON")

            if drinks[0].get("strAlcoholic") == "Alcoholic":
                return "check_data.alcoholic_drink"
            return "check_data.non_alcoholic_drink"

        @task()
        def alcoholic_drink() -> None:
            print("Identified drink classification: Alcoholic")

        @task()
        def non_alcoholic_drink() -> None:
            print("Identified drink classification: Non-alcoholic")

        @task(
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            templates_dict={
                "the_current_date": "{{ ds }}",
                "my_api": "{{ var.value.get('api', 'https://www.thecocktaildb.com/api/json/v1/1/random.php') }}",
            },
        )
        def clean_data(templates_dict: dict[str, str]) -> None:
            """Verify extraction artifact remains intact for asset-triggered downstream DAGs."""
            path = Dataset_cocktail.uri
            if os.path.exists(path):
                file_size = os.path.getsize(path)
                print(
                    f"Dataset verified and retained at {path} ({file_size} bytes) "
                    "for downstream consumer DAGs."
                )
            else:
                print(f"Notice: dataset file {path} not present.")

            print(f"Extraction pipeline finalized for execution date: {templates_dict['the_current_date']}")
            print(f"API endpoint used: {templates_dict['my_api']}")

        alc = alcoholic_drink()
        non_alc = non_alcoholic_drink()
        branch = branch_cocktail_type()
        cleanup = clean_data()

        check_json_data >> check_size >> branch >> [alc, non_alc] >> cleanup

    get_cocktail = PythonOperator(
        task_id="get_cocktail",
        python_callable=_get_cocktail,
        outlets=[Dataset_cocktail],
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=15),
    )

    get_cocktail >> check_data()


my_extractor = extractor()

if __name__ == "__main__":
    my_extractor.test()
