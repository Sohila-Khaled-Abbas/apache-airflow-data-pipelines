"""Task callables for fetching and validating cocktail API payloads."""

import json
import os
import requests
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from include.datasets import Dataset_cocktail

DEFAULT_API = "https://www.thecocktaildb.com/api/json/v1/1/random.php"


def _get_cocktail(ti=None) -> int:
    """Fetch random cocktail JSON from API and persist to the shared asset path."""
    api = Variable.get("api", default=DEFAULT_API)
    try:
        response = requests.get(api, timeout=15)
        response.raise_for_status()
    except Exception as err:
        raise AirflowFailException(f"Failed to fetch cocktail data from {api}: {err}") from err

    target_path = Dataset_cocktail.uri
    os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)

    with open(target_path, "wb") as f:
        f.write(response.content)

    content_len = len(response.content)
    print(f"Successfully fetched cocktail data ({content_len} bytes) from {api}")

    if ti:
        ti.xcom_push(key="request_size", value=content_len)

    return content_len


def _check_size(ti=None) -> int:
    """Verify downloaded content size is positive."""
    size = None
    if ti:
        size = ti.xcom_pull(key="request_size", task_ids="get_cocktail")

    print(f"The size of the request is: {size}")
    if not size:
        raise AirflowFailException("Request size is empty or zero")

    return size


def _check_json_data() -> int:
    """Verify downloaded file exists, is valid JSON, and contains expected data."""
    path = Dataset_cocktail.uri
    if not os.path.exists(path):
        raise AirflowFailException(f"JSON file not found at {path}")

    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as err:
            raise AirflowFailException(f"Invalid JSON at {path}: {err}") from err

    drinks = data.get("drinks")
    if not drinks or not isinstance(drinks, list):
        raise AirflowFailException(f"JSON at {path} does not contain a valid 'drinks' list: {data}")

    print(f"JSON data is valid — top-level keys: {list(data.keys())}")
    print(f"Found {len(drinks)} drink record(s). File size: {os.path.getsize(path)} bytes")
    return len(drinks)
