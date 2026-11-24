"""
Stock Market End-to-End Capstone Data Pipeline
Orchestrates:
  1. API Availability Verification (Custom TaskFlow Sensor with PokeReturnValue)
  2. Stock Price Extraction from Yahoo Finance (PythonOperator)
  3. Staging Raw JSON in MinIO Object Storage (S3 API)
  4. Distributed PySpark ETL on Docker (DockerOperator)
  5. Discovery of Formatted CSV in Object Storage (PythonOperator)
  6. Data Lake to Postgres Data Warehouse Loading (Native PostgresHook + MinIO)
"""

import csv
from datetime import datetime, timedelta
import io
import logging
from airflow.exceptions import AirflowFailException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import BaseHook
from airflow.sdk import PokeReturnValue, dag, task
import requests

from include.stock_market.tasks import (
    BUCKET_NAME,
    _get_formatted_csv,
    _get_minio_client,
    _get_stock_prices,
    _store_prices,
)

logger = logging.getLogger(__name__)
SYMBOL = "NVDA"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["stock_market", "capstone", "spark", "minio", "data_warehouse"],
    description="End-to-end stock market ETL pipeline using MinIO, Spark, Docker, and PostgreSQL",
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        """Poke Yahoo Finance endpoint to verify connectivity and API health."""
        try:
            api = BaseHook.get_connection("stock_api")
            endpoint = api.extra_dejson.get("endpoint", "/v8/finance/chart/")
            headers = api.extra_dejson.get("headers", {"User-Agent": "Mozilla/5.0"})
            url = f"{api.host.rstrip('/')}{endpoint}"
        except Exception:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/"
            headers = {"User-Agent": "Mozilla/5.0"}

        test_url = f"{url}{SYMBOL}"
        try:
            response = requests.get(test_url, headers=headers, timeout=15)
            payload = response.json()
            is_valid = payload.get("chart", {}).get("error") is None
            return PokeReturnValue(is_done=is_valid, xcom_value=url)
        except Exception as err:
            logger.warning("Stock API probe returned error: %s", err)
            return PokeReturnValue(is_done=False, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={
            "url": '{{ ti.xcom_pull(task_ids="is_api_available") }}',
            "symbol": SYMBOL,
        },
    )

    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={
            "stock": '{{ ti.xcom_pull(task_ids="get_stock_prices") }}',
        },
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image="airflow/stock-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove="success",
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": '{{ ti.xcom_pull(task_ids="store_prices") }}',
        },
    )

    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={
            "path": '{{ ti.xcom_pull(task_ids="store_prices") }}',
        },
    )

    @task()
    def load_to_dw(csv_object_name: str) -> int:
        """Stream Spark-formatted CSV from MinIO and load into PostgreSQL data warehouse."""
        client = _get_minio_client()
        response = client.get_object(BUCKET_NAME, csv_object_name)
        try:
            csv_text = response.read().decode("utf-8")
        finally:
            response.close()
            response.release_conn()

        reader = csv.DictReader(io.StringIO(csv_text))
        rows = [
            (
                int(row["timestamp"]),
                float(row["close"]) if row["close"] else None,
                float(row["high"]) if row["high"] else None,
                float(row["low"]) if row["low"] else None,
                float(row["open"]) if row["open"] else None,
                int(float(row["volume"])) if row["volume"] else None,
                row["date"],
            )
            for row in reader
            if row.get("timestamp")
        ]

        if not rows:
            raise AirflowFailException(f"No rows parsed from s3://{BUCKET_NAME}/{csv_object_name}")

        hook = PostgresHook(postgres_conn_id="postgres")
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS stock_market (
                timestamp BIGINT PRIMARY KEY,
                close DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                open DOUBLE PRECISION,
                volume BIGINT,
                date DATE
            );
            """
        )

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO stock_market (timestamp, close, high, low, open, volume, date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO UPDATE SET
                        close = EXCLUDED.close,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        open = EXCLUDED.open,
                        volume = EXCLUDED.volume,
                        date = EXCLUDED.date;
                    """,
                    rows,
                )
            conn.commit()
        finally:
            conn.close()

        logger.info("Loaded %d rows into public.stock_market", len(rows))
        return len(rows)

    (
        is_api_available()
        >> get_stock_prices
        >> store_prices
        >> format_prices
        >> get_formatted_csv
        >> load_to_dw(get_formatted_csv.output)
    )


stock_market()
