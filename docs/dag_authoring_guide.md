# Contribution Guidelines for This Repository

This guide explains how to contribute to the Apache Airflow portfolio repository, including DAG conventions, testing requirements, and the CI/CD guardrails.

---

## 1. DAG Conventions

Every DAG in `dags/` must follow these rules:

| Rule | Requirement | Enforced By |
|------|-------------|-------------|
| Tags | At least one meaningful `tags` list | CI test `test_dag_tags_and_descriptions` |
| Description | A `description` string summarizing the pipeline | CI test `test_dag_tags_and_descriptions` |
| Retries | `default_args["retries"] >= 2` | CI test `test_dag_retries_configured` |
| Import health | Zero import/syntax errors | CI test `test_no_import_errors` |
| Acyclic | No dependency cycles | CI test `test_no_cyclic_dependencies` |
| No top-level DB calls | No `Variable.get()` / `BaseHook.get_connection()` at module scope | CI test `test_no_top_level_variable_or_hook_calls` |

### Example DAG Skeleton

```python
from datetime import datetime, timedelta
from pendulum import datetime as pendulum_datetime
from airflow.sdk import dag, task

@dag(
    start_date=pendulum_datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["example", "pipeline"],
    description="A well-formed example DAG",
)
def example_dag():
    @task()
    def step_one():
        return "done"

    step_one()


example_dag()
```

---

## 2. Assets & Data-Aware Scheduling

Shared Airflow Assets are defined in [`include/datasets.py`](include/datasets.py):

```python
from airflow.sdk import Asset

Dataset_cocktail = Asset("/tmp/cocktail.json")
```

- **Producers** declare the Asset in `outlets=[...]`
- **Consumers** schedule on the Asset via `schedule=[Dataset_cocktail]`
- Avoid file-based race conditions: do not delete a dataset file that downstream DAGs need

---

## 3. Adding a New DAG

1. Create `dags/your_dag.py` following the conventions above.
2. Add the DAG ID to `EXPECTED_DAG_IDS` in [`tests/dags/test_dag_integrity.py`](tests/dags/test_dag_integrity.py).
3. Add a row to the DAG Catalog table in [`README.md`](README.md).
4. Run `pytest tests/ -v` locally to verify.
5. Open a Pull Request using [.github/pull_request_template.md](.github/pull_request_template.md).

---

## 4. CI/CD Guardrails

The [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push and PR:

1. **Flake8 linting** — syntax and complexity checks
2. **DagBag import** — all DAGs parse cleanly
3. **Cycle & best-practice checks** — tags, descriptions, retries >= 2
4. **AST anti-pattern guardrail** — blocks top-level `Variable.get` and `BaseHook.get_connection`

---

## 5. Documentation

When changing code, update the corresponding docs:

| Change | Docs to Update |
|--------|---------------|
| New DAG | `README.md` DAG Catalog + `docs/architecture.md` |
| New executor config | `docs/executors_guide.md` + `infra/` |
| New Spark job | `spark/notebooks/` + `README.md` |
| Bug fix / regression | `docs/troubleshooting_guide.md` |

Use Mermaid diagrams for architecture visualizations.

---

## 6. Questions

Open a [discussion](https://github.com/Sohila-Khaled-Abbas/apache-airflow-data-pipelines/discussions) or an issue using the templates in `.github/ISSUE_TEMPLATE/`.