# DAG Authoring & Engineering Conventions

This guide defines the Airflow 3 coding standards, Asset patterns, unit/integrity testing rules, and CI/CD guardrails enforced across [`dags/`](../dags/) and [`include/`](../include/).

For Git workflow, branching, and PR submission instructions, see [`CONTRIBUTING.md`](../CONTRIBUTING.md).

---

## 1. DAG Conventions

Every DAG in [`dags/`](../dags/) must satisfy these rules (validated in CI):

| Rule | Requirement | Enforced By |
|---|---|---|
| Tags | Non-empty `tags` list | [`test_dag_tags_and_descriptions`](../tests/dags/test_dag_integrity.py) |
| Description | Meaningful `description` string | [`test_dag_tags_and_descriptions`](../tests/dags/test_dag_integrity.py) |
| Retries | `default_args["retries"] >= 2` | [`test_dag_retries_configured`](../tests/dags/test_dag_integrity.py) |
| Import health | Zero import or syntax errors | [`test_no_import_errors`](../tests/dags/test_dag_integrity.py) |
| Acyclic graph | No dependency cycles | [`test_no_cyclic_dependencies`](../tests/dags/test_dag_integrity.py) |
| No top-level DB calls | No `Variable.get()` or `BaseHook.get_connection()` at module scope | [`test_no_top_level_variable_or_hook_calls`](../tests/dags/test_dag_integrity.py) |

### Example Airflow 3 DAG Skeleton

```python
from datetime import timedelta

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["example", "pipeline"],
    description="A well-formed Airflow 3 example DAG",
)
def example_dag():
    @task()
    def step_one() -> str:
        return "done"

    step_one()


example_dag()
```

---

## 2. Assets & Data-Aware Scheduling

Shared Airflow Assets are declared in [`include/datasets.py`](../include/datasets.py):

```python
from airflow.sdk import Asset

Dataset_cocktail = Asset("/tmp/cocktail.json")
```

- **Producers** attach the Asset via `outlets=[Dataset_cocktail]`.
- **Consumers** subscribe via `schedule=[Dataset_cocktail]`.
- **No cross-DAG file races**: Never delete a local dataset file inside an upstream DAG while downstream Asset-triggered DAGs depend on reading it.

---

## 3. Adding a New DAG

1. Create `dags/<your_dag>.py` following the conventions above.
2. Add the new DAG ID to `EXPECTED_DAG_IDS` in [`tests/dags/test_dag_integrity.py`](../tests/dags/test_dag_integrity.py).
3. Add mocked unit tests for any new helper logic under [`tests/unit/`](../tests/unit/).
4. Add the DAG to the DAG Catalog table in [`README.md`](../README.md) and update [`docs/architecture.md`](architecture.md).
5. Run local checks and submit a PR using [`.github/pull_request_template.md`](../.github/pull_request_template.md).

---

## 4. CI/CD Guardrails

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push and PR to `main` or `master`:

1. **Flake8 linting** — syntax errors (`E9,F63,F7,F82`) and complexity/style warnings (`max-complexity=10`, `max-line-length=120`).
2. **Import & syntax verification** — imports all `include/` modules, all root `dags/` modules, and syntax-checks [`infra/celery/dags/celery.py`](../infra/celery/dags/celery.py) and [`spark/notebooks/stock_transform/stock_transform.py`](../spark/notebooks/stock_transform/stock_transform.py).
3. **DAG integrity, unit & AST tests** — runs `airflow db migrate` followed by `pytest tests/ -v --tb=short`.

---

## 5. Documentation Sync Matrix

When modifying code or infrastructure, keep the corresponding docs current:

| Change | Documentation to Update |
|---|---|
| New or updated root DAG | [`README.md`](../README.md) + [`docs/architecture.md`](architecture.md) |
| New helper unit tests | [`docs/learning_journal.md`](learning_journal.md) |
| Executor or infra config change | [`docs/executors_guide.md`](executors_guide.md) + [`infra/README.md`](../infra/README.md) |
| Celery DAG pattern or queue update | [`docs/celery_dag_pattern.md`](celery_dag_pattern.md) + [`docs/diagrams/`](diagrams/) |
| Bug fix or reliability case study | [`docs/troubleshooting_guide.md`](troubleshooting_guide.md) |
