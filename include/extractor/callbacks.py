"""Lifecycle and monitoring callbacks for the cocktail extraction DAG."""

from typing import Any


def _handle_failed_dag_run(context: dict[str, Any]) -> None:
    """Log failure details when a DAG run fails."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")
    print(
        f"DAG run failed at task '{task_id}' "
        f"for the data interval between {start} and {end}."
    )


def _handle_success_dag_run(context: dict[str, Any]) -> None:
    """Log completion details when a DAG run succeeds."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")
    print(
        f"DAG run succeeded at task '{task_id}' "
        f"for the data interval between {start} and {end}."
    )


def handle_empty_size(context: dict[str, Any]) -> None:
    """Log notification when size validation starts or payload is empty."""
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")
    print(
        "Validating cocktail payload size "
        f"for the data interval between {start} and {end}."
    )
