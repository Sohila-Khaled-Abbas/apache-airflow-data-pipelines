# Infrastructure Templates (`infra/`)

This directory contains reference infrastructure definitions for running Apache Airflow under distributed executors beyond the local Astro Runtime `LocalExecutor` setup.

## Contents

| Directory | Executor | Description | Documentation |
|---|---|---|---|
| [`celery/`](celery/) | `CeleryExecutor` | Multi-container Docker Compose stack (Airflow `2.10.5`, Redis, PostgreSQL, Flower, specialized `cpu` and `gpu` workers) | [Celery Stack README](celery/README.md) |
| [`kubernetes/`](kubernetes/) | `KubernetesExecutor` | Official Helm chart `values.yaml` template configured for per-task Pod execution (`defaultAirflowTag: "2.10.5"`) | [Executors Guide](../docs/executors_guide.md) |

> **Note**: These configurations are intended for local experimentation and reference architecture demonstrations. Harden secrets, Fernet keys, network policies, and resource limits before deploying to production.
