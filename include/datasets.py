"""Shared Airflow Assets (Datasets) for event-driven DAG scheduling."""

from airflow.sdk import Asset

Dataset_cocktail = Asset("/tmp/cocktail.json")
