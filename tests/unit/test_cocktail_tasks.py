"""Unit tests for cocktail API extraction and validation helpers in ``include/tasks.py``."""

import json
from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowFailException

import include.tasks as cocktail_tasks


def test_check_size_passes_for_positive():
    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = 42

    assert cocktail_tasks._check_size(ti=mock_ti) == 42
    mock_ti.xcom_pull.assert_called_once_with(key="request_size", task_ids="get_cocktail")


@pytest.mark.parametrize("size", [0, None])
def test_check_size_raises_for_empty_or_missing(size):
    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = size

    with pytest.raises(AirflowFailException):
        cocktail_tasks._check_size(ti=mock_ti)


def test_check_json_data_validates_payload(tmp_path, monkeypatch):
    dataset_file = tmp_path / "cocktail.json"
    dataset_file.write_text(
        json.dumps({"drinks": [{"idDrink": "11007", "strDrink": "Margarita"}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(cocktail_tasks, "Dataset_cocktail", Mock(uri=str(dataset_file)))

    assert cocktail_tasks._check_json_data() == 1


def test_check_json_data_raises_on_missing_file(tmp_path, monkeypatch):
    missing_file = tmp_path / "missing.json"
    monkeypatch.setattr(cocktail_tasks, "Dataset_cocktail", Mock(uri=str(missing_file)))

    with pytest.raises(AirflowFailException, match="not found"):
        cocktail_tasks._check_json_data()


def test_check_json_data_raises_on_invalid_json(tmp_path, monkeypatch):
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{not-valid-json", encoding="utf-8")
    monkeypatch.setattr(cocktail_tasks, "Dataset_cocktail", Mock(uri=str(bad_file)))

    with pytest.raises(AirflowFailException, match="Invalid JSON"):
        cocktail_tasks._check_json_data()


def test_get_cocktail_writes_payload_and_pushes_xcom(tmp_path, monkeypatch):
    output_file = tmp_path / "cocktail.json"
    payload = b'{"drinks": [{"idDrink": "11007"}]}'
    mock_response = Mock(content=payload)
    mock_response.raise_for_status = Mock()

    monkeypatch.setattr(cocktail_tasks, "Dataset_cocktail", Mock(uri=str(output_file)))
    monkeypatch.setenv("COCKTAIL_API", "https://example.test/cocktail")
    mock_ti = Mock()

    with patch("include.tasks.requests.get", return_value=mock_response) as mock_get:
        size = cocktail_tasks._get_cocktail(ti=mock_ti)

    mock_get.assert_called_once_with("https://example.test/cocktail", timeout=15)
    assert size == len(payload)
    assert output_file.read_bytes() == payload
    mock_ti.xcom_push.assert_called_once_with(key="request_size", value=len(payload))
