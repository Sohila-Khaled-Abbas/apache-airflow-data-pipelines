# Documentation Index

Welcome to the engineering documentation for **Apache Airflow Data Pipelines & Lakehouse Architecture**.

## Guides

| Document | Description |
|---|---|
| [Architecture & Pipeline Design](architecture.md) | Component topology, Airflow 3 & Celery stacks, and end-to-end pipeline data flows |
| [DAG Authoring Guide](dag_authoring_guide.md) | Airflow 3 SDK conventions, Asset scheduling, testing standards, and CI guardrails |
| [Executors & Scaling Guide](executors_guide.md) | Comparison of Sequential, Local, Celery (multi-queue), and Kubernetes executors |
| [Celery DAG Pattern Guide](celery_dag_pattern.md) | Decorator syntax guide, Airflow 2 vs. 3 imports, and `cpu`/`gpu` queue routing |
| [Troubleshooting & Case Study](troubleshooting_guide.md) | Root-cause analysis for scheduler stalls, AST guardrails, dataset races, and Airflow 3 migration |
| [Engineering Competency Matrix](learning_journal.md) | Mapping of Airflow competencies to concrete repository implementations |
| [Course Materials Guide](course_materials_guide.md) | Guide to the preserved Udemy *Apache Airflow: The Hands-On Guide* section snapshots |

## Diagrams

Standalone Mermaid source files live in [`diagrams/`](diagrams/):

- [`diagrams/celery_dag_flow.mmd`](diagrams/celery_dag_flow.mmd) — Task fan-out and `cpu`/`gpu` queue routing in `infra/celery/dags/celery.py`
- [`diagrams/celery_decorator_fix.mmd`](diagrams/celery_decorator_fix.mmd) — Before/after fix for the `@dag` / `@task` decorator syntax error

## Repository-Level Community Docs

- [Main README](../README.md)
- [Contributing Guidelines](../CONTRIBUTING.md)
- [Code of Conduct](../CODE_OF_CONDUCT.md)
- [Security Policy](../SECURITY.md)
- [Support](../SUPPORT.md)
