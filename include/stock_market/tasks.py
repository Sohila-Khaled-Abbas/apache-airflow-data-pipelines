"""
Tasks and helper functions for the Stock Market Pipeline.
Handles Yahoo Finance API extraction, MinIO object storage, and transformed CSV discovery.
"""

from io import BytesIO
import json
import logging
from airflow.exceptions import AirflowFailException, AirflowNotFoundException
from airflow.sdk import BaseHook
from minio import Minio
import requests

logger = logging.getLogger(__name__)

BUCKET_NAME = "stock-market"
DEFAULT_MINIO_ENDPOINT = "minio:9000"
DEFAULT_MINIO_USER = "minio"
DEFAULT_MINIO_PASSWORD = "minio123"


def _get_minio_client() -> Minio:
    """Instantiate and return an authenticated MinIO S3 client."""
    try:
        minio_conn = BaseHook.get_connection("minio")
        endpoint_url = minio_conn.extra_dejson.get("endpoint_url", f"http://{DEFAULT_MINIO_ENDPOINT}")
        endpoint = endpoint_url.split("//")[-1]
        access_key = minio_conn.login or DEFAULT_MINIO_USER
        secret_key = minio_conn.password or DEFAULT_MINIO_PASSWORD
    except Exception as err:
        logger.warning("Could not fetch 'minio' connection from Airflow, falling back to defaults: %s", err)
        endpoint = DEFAULT_MINIO_ENDPOINT
        access_key = DEFAULT_MINIO_USER
        secret_key = DEFAULT_MINIO_PASSWORD

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def _get_stock_prices(url: str, symbol: str) -> str:
    """Fetch 1-year daily stock price history from Yahoo Finance API."""
    query_url = f"{url.rstrip('/')}/{symbol}?metrics=high?&interval=1d&range=1y"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        api_conn = BaseHook.get_connection("stock_api")
        if api_conn.extra_dejson and "headers" in api_conn.extra_dejson:
            headers.update(api_conn.extra_dejson["headers"])
    except Exception as err:
        logger.info("Using default headers for stock API query: %s", err)

    try:
        response = requests.get(query_url, headers=headers, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except Exception as err:
        raise AirflowFailException(f"Failed to query stock prices from {query_url}: {err}") from err

    results = payload.get("chart", {}).get("result")
    if not results:
        raise AirflowFailException(f"No result found in stock API response for {symbol}: {payload}")

    return json.dumps(results[0])


def _store_prices(stock: str) -> str:
    """Store raw stock prices JSON into MinIO S3 storage."""
    client = _get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        logger.info("Created MinIO bucket: %s", BUCKET_NAME)

    stock_data = json.loads(stock)
    symbol = stock_data.get("meta", {}).get("symbol", "UNKNOWN")
    data_bytes = json.dumps(stock_data, ensure_ascii=False).encode("utf-8")

    object_name = f"{symbol}/prices.json"
    obj = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json",
    )
    logger.info("Stored stock data to %s/%s", obj.bucket_name, object_name)
    return f"{obj.bucket_name}/{symbol}"


def _get_formatted_csv(path: str) -> str:
    """Locate the Spark-transformed CSV file within the MinIO bucket."""
    client = _get_minio_client()
    symbol = path.split("/")[1]
    prefix_name = f"{symbol}/formatted_prices/"

    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            logger.info("Located transformed CSV: %s", obj.object_name)
            return obj.object_name

    raise AirflowNotFoundException(
        f"No transformed CSV file found in bucket '{BUCKET_NAME}' with prefix '{prefix_name}'"
    )
