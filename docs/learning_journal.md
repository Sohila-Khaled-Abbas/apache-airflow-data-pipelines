# Data Engineering Competency Matrix

This document maps the core Apache Airflow engineering patterns and production practices implemented across this repository.

---

## Competency Matrix

| Domain | Key Concepts | Implementation in Repository |
|---|---|---|
| **Airflow 3 SDK & TaskFlow** | `@dag`, `@task`, `@task_group`, `@task.branch`, `@task.sensor` | [`dags/extractor.py`](../dags/extractor.py), [`dags/cocktail_loader.py`](../dags/cocktail_loader.py), [`dags/stock_market.py`](../dags/stock_market.py), [`dags/celery_parallelism_demo.py`](../dags/celery_parallelism_demo.py) |
| **Operators & Hooks** | `PythonOperator`, `EmptyOperator`, `DockerOperator`, `PostgresHook`, `BaseHook` | [`dags/stock_market.py`](../dags/stock_market.py), [`dags/cocktail_loader.py`](../dags/cocktail_loader.py), [`include/stock_market/tasks.py`](../include/stock_market/tasks.py) |
| **Data-Aware Scheduling** | Airflow Assets (`Asset`), `inlets`, `outlets`, multi-DAG event triggers | [`include/datasets.py`](../include/datasets.py) connecting `extractor` -> `cocktail_loader` & `ecom` |
| **Routing & Resilience** | Dynamic branching, `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`, exponential backoff, callbacks | [`dags/extractor.py`](../dags/extractor.py), [`include/extractor/callbacks.py`](../include/extractor/callbacks.py) |
| **Distributed Compute & Storage** | MinIO S3-compatible object storage, containerized Spark Master/Worker cluster | [`spark/`](../spark/), [`docker-compose.override.yml`](../docker-compose.override.yml), [`include/stock_market/tasks.py`](../include/stock_market/tasks.py) |
| **Data Warehouse Loading** | Idempotent SQL DDL + transactional `ON CONFLICT` upserts in PostgreSQL | [`dags/cocktail_loader.py`](../dags/cocktail_loader.py), [`dags/stock_market.py`](../dags/stock_market.py) |
| **Executors & Multi-Queue Celery** | `LocalExecutor` tuning, `CeleryExecutor` + Redis compose (`cpu`/`gpu` queues), `KubernetesExecutor` Helm values | [`.env.example`](../.env.example), [`infra/celery/docker-compose.yaml`](../infra/celery/docker-compose.yaml), [`infra/celery/dags/celery.py`](../infra/celery/dags/celery.py), [`infra/kubernetes/values.yaml`](../infra/kubernetes/values.yaml), [`docs/executors_guide.md`](executors_guide.md), [`docs/celery_dag_pattern.md`](celery_dag_pattern.md) |
| **Testing, CI/CD & Quality Guardrails** | Automated Flake8 linting, Pytest DAG integrity, mocked unit tests, cycle detection, AST anti-pattern checks | [`.github/workflows/ci.yml`](../.github/workflows/ci.yml), [`tests/dags/test_dag_integrity.py`](../tests/dags/test_dag_integrity.py), [`tests/unit/test_cocktail_tasks.py`](../tests/unit/test_cocktail_tasks.py) |
