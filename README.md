# Apache Airflow Enterprise Data Pipelines & Lakehouse Architecture

<div align="center">

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1.8-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Distributed%20ETL-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Object%20Storage-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20%26%20Astro-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![CI](https://img.shields.io/github/actions/workflow/status/Sohila-Khaled-Abbas/apache-airflow-data-pipelines/ci.yml?label=CI)](https://github.com/Sohila-Khaled-Abbas/apache-airflow-data-pipelines/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

</div>

A production-structured **Apache Airflow 3** portfolio repository showcasing end-to-end ETL/ELT pipelines, event-driven Asset scheduling, distributed **PySpark** transformations over **MinIO (S3)**, **PostgreSQL** data warehousing, multi-executor infrastructure templates (`Local`, `Celery`, `Kubernetes`), and automated **CI/CD DAG integrity guardrails**.

---

> **🎓 Learning Evidence** — This repository is published as evidence of learning for the **Apache Airflow: The Hands-On Guide** course. All course section materials are preserved under [`course-materials/udemy-hands-on-guide/`](./course-materials/udemy-hands-on-guide/) alongside the capstone portfolio implementation.

---

## Table of Contents

- [Learning Outcomes](#learning-outcomes)
- [Architecture Overview](#architecture-overview)
- [DAG Catalog](#dag-catalog)
- [Featured Pipelines](#featured-pipelines)
  - [1. Stock Market Data Lakehouse](#1-stock-market-data-lakehouse)
  - [2. Asset-Driven Cocktail ETL](#2-asset-driven-cocktail-etl)
- [Performance & Reliability Case Study](#performance--reliability-case-study)
- [Course Materials (Udemy)](#course-materials-udemy)
- [Repository Structure](#repository-structure)
- [Quickstart Guide](#quickstart-guide)
- [Testing & CI/CD Guardrails](#testing--cicd-guardrails)
- [Documentation Index](#documentation-index)
- [Contributing & Support](#contributing--support)

---

## Learning Outcomes

| # | Competency | Evidence in Repo |
|---|-----------|------------------|
| 1 | **Airflow 3 SDK & TaskFlow API** | `@dag`, `@task`, `@task_group`, `@task.branch`, `@task.sensor` across `dags/` |
| 2 | **Data-Aware Scheduling (Assets)** | `include/datasets.py` — producer `extractor` → consumers `cocktail_loader`, `ecom` |
| 3 | **Distributed Compute & Storage** | MinIO S3 lake + containerized Spark Master/Worker cluster in `spark/` |
| 4 | **Data Warehouse Loading** | Idempotent `ON CONFLICT` upserts into PostgreSQL (`stock_market`, `cocktails`) |
| 5 | **Executors & Scaling** | `Local`, `Celery` (+Redis), `KubernetesExecutor` Helm values in `infra/` |
| 6 | **CI/CD & Quality Guardrails** | `.github/workflows/ci.yml` + `tests/dags/test_dag_integrity.py` (AST anti-pattern, cycle, retry checks) |
| 7 | **Production Troubleshooting** | `docs/troubleshooting_guide.md` — scheduler heartbeat, module-scope DB thrashing, file race conditions |
| 8 | **Infrastructure as Code** | Docker Compose profiles, Kubernetes `values.yaml`, `.env` tuning, `airflow_settings.yaml` |

---

## Architecture Overview

```mermaid
graph LR
    subgraph External Sources
        YF[Yahoo Finance API]
        CDB[TheCocktailDB API]
    end

    subgraph Orchestration -- Apache Airflow 3.1.8
        SM[stock_market DAG]
        EX[extractor DAG]
        CL[cocktail_loader DAG]
        EC[ecom DAG]
        ASSET((Airflow Asset))
    end

    subgraph Compute & Storage
        MINIO[(MinIO S3 Lake)]
        SPARK[PySpark Cluster]
        PG[(PostgreSQL DW)]
        MB[Metabase BI]
    end

    YF --> SM
    SM --> MINIO
    MINIO <--> SPARK
    SM --> PG
    PG --> MB

    CDB --> EX
    EX -->|outlets| ASSET
    ASSET -.->|schedule| CL
    ASSET -.->|schedule| EC
    CL --> PG
```

---

## DAG Catalog

| DAG ID | Schedule | Pattern Highlight | Target Sink | File |
|---|---|---|---|---|
| `stock_market` | `@daily` | TaskFlow Sensor (`PokeReturnValue`), `DockerOperator` + PySpark, S3-to-Postgres loader | MinIO & PostgreSQL | [stock_market.py](dags/stock_market.py) |
| `extractor` | `@daily` | TaskGroups, `@task.branch`, `TriggerRule`, callbacks, Asset producer | Local Asset (`Dataset_cocktail`) | [extractor.py](dags/extractor.py) |
| `cocktail_loader` | `[Dataset_cocktail]` | Data-aware Asset consumer, transactional SQL upserts (`ON CONFLICT`) | PostgreSQL (`cocktails`) | [cocktail_loader.py](dags/cocktail_loader.py) |
| `ecom` | `[Dataset_cocktail]` | Lightweight downstream Asset subscriber with timeout & failure limits | Downstream workflow | [ecom.py](dags/ecom.py) |
| `celery_parallelism_demo` | `Manual` | Parallel fan-out task orchestration for `Local`/`Celery` benchmarking | Stdout / XCom | [celery_parallelism_demo.py](dags/celery_parallelism_demo.py) |

---

## Featured Pipelines

### 1. Stock Market Data Lakehouse

An automated market data lakehouse pipeline (`dags/stock_market.py`):

1. **Probe (`is_api_available`)**: Custom `@task.sensor` returning `PokeReturnValue` checks Yahoo Finance API health before running compute tasks.
2. **Extract (`get_stock_prices`)**: Queries 1-year daily OHLCV market history for `NVDA`.
3. **Stage (`store_prices`)**: Persists raw JSON into MinIO S3 bucket `stock-market`.
4. **Transform (`format_prices`)**: Launches an ephemeral PySpark application container (`spark/notebooks/stock_transform/stock_transform.py`) connected to `spark-master` via `DockerOperator` to normalize nested JSON arrays into clean CSV partitions.
5. **Discover & Load (`get_formatted_csv` -> `load_to_dw`)**: Locates the Spark output partition in MinIO and streams rows idempotently into `public.stock_market` in PostgreSQL.

### 2. Asset-Driven Cocktail ETL

An event-driven multi-DAG workflow demonstrating modern Airflow 3 **Assets**:

- **`extractor`** fetches cocktail records, validates JSON schema and byte size inside a `@task_group`, dynamically routes execution via `@task.branch` (`alcoholic_drink` vs. `non_alcoholic_drink`), executes cleanup with `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`, and updates `Dataset_cocktail`.
- **`cocktail_loader`** and **`ecom`** activate automatically upon Asset updates without fragile cron polling.

---

## Performance & Reliability Case Study

During development on a constrained local Docker Desktop WSL2 environment, three production-style issues were diagnosed and resolved (full write-up in [Troubleshooting Guide](docs/troubleshooting_guide.md)):

1. **Scheduler Heartbeat Starvation (`634s` stalls)**: Heavy background containers starved the shared WSL2 engine, causing Docker API `500` errors and missed Airflow scheduler heartbeats. Stopping competing containers, resetting the WSL2 engine, tuning `.env` (`AIRFLOW__CORE__PARALLELISM=6`), and isolating heavy Spark/Metabase containers behind a Docker Compose `stock-market` profile restored sub-second responsiveness.
2. **Module-Scope DB Thrashing**: Eliminated top-level `Variable.get()` calls from `include/tasks.py` and enforced a static AST CI test (`test_no_top_level_variable_or_hook_calls`) so scheduler parse loops never hammer the metadata DB.
3. **Cross-DAG File Race Condition**: Resolved intermittent `cocktail_loader` failures caused by `extractor` deleting `/tmp/cocktail.json` while the Asset-triggered downstream DAG attempted to ingest it.
4. **Airflow 3 Compatibility**: Replaced legacy `astro-sdk-python` (`core/enable_xcom_pickling` error on Airflow 3) with a native Airflow 3 `Minio` + `PostgresHook` loader.

---

## Course Materials (Udemy)

All **course section snapshots** from *Apache Airflow: The Hands-On Guide* are preserved under [`course-materials/udemy-hands-on-guide/`](./course-materials/udemy-hands-on-guide/) for reference and comparison against the capstone implementation.

| Section | Contents | Location |
|---------|----------|----------|
| `course_sec_4_2` | DAGs, include, plugins, tests, Astro config | [`course-materials/udemy-hands-on-guide/course_sec_4_2/`](./course-materials/udemy-hands-on-guide/course_sec_4_2/) |
| `course_sec_4_6` | Airflow project scaffolding | [`course-materials/udemy-hands-on-guide/course_sec_4_6/`](./course-materials/udemy-hands-on-guide/course_sec_4_6/) |
| `course_sec_4_9` | DAG integrity tests, Astro runtime config | [`course-materials/udemy-hands-on-guide/course_sec_4_9/`](./course-materials/udemy-hands-on-guide/course_sec_4_9/) |
| `course_sec_4_10` | Course section 4.10 materials | [`course-materials/udemy-hands-on-guide/course_sec_4_10/`](./course-materials/udemy-hands-on-guide/course_sec_4_10/) |
| `course_sec_4_11` | Course section 4.11 materials | [`course-materials/udemy-hands-on-guide/course_sec_4_11/`](./course-materials/udemy-hands-on-guide/course_sec_4_11/) |
| `course_sec_4_12` | Course section 4.12 materials | [`course-materials/udemy-hands-on-guide/course_sec_4_12/`](./course-materials/udemy-hands-on-guide/course_sec_4_12/) |
| `course_sec_4_13` | Course section 4.13 materials | [`course-materials/udemy-hands-on-guide/course_sec_4_13/`](./course-materials/udemy-hands-on-guide/course_sec_4_13/) |

> **Note**: Sensitive/local files (`.env`, `airflow_settings.yaml`, `airflow.db`, `__pycache__`, `__MACOSX`) were excluded from the course materials snapshot for security and cleanliness.

---

## Repository Structure

```text
.
├── .github/
│   ├── ISSUE_TEMPLATE/         # Bug report and feature proposal templates
│   ├── workflows/ci.yml        # Flake8 linting + Airflow 3 pytest CI pipeline
│   └── pull_request_template.md
├── course-materials/
│   └── udemy-hands-on-guide/   # Udemy course section snapshots (reference)
│       ├── course_sec_4_2/
│       ├── course_sec_4_6/
│       ├── course_sec_4_9/
│       ├── course_sec_4_10/
│       ├── course_sec_4_11/
│       ├── course_sec_4_12/
│       └── course_sec_4_13/
├── dags/
│   ├── stock_market.py         # End-to-end API → MinIO → Spark → Postgres pipeline
│   ├── extractor.py            # API extractor with branching & Asset production
│   ├── cocktail_loader.py      # Asset-triggered Postgres loader
│   ├── ecom.py                 # Asset-triggered downstream subscriber
│   └── celery_parallelism_demo.py
├── docs/
│   ├── architecture.md         # Component topology and pipeline diagrams
│   ├── executors_guide.md      # Sequential, Local, Celery, and Kubernetes guide
│   ├── learning_journal.md     # Competency matrix
│   ├── troubleshooting_guide.md # Root-cause analysis & performance tuning report
│   ├── CONTRIBUTING_GUIDE.md   # Contribution guidelines for this repo
│   └── COURSE_MATERIALS_GUIDE.md # How to use the Udemy course materials
├── include/
│   ├── datasets.py             # Shared Airflow Asset definitions
│   ├── tasks.py                # Cocktail extraction and validation helpers
│   ├── extractor/callbacks.py  # Success/failure DAG lifecycle callbacks
│   └── stock_market/tasks.py   # MinIO S3 and Yahoo Finance client utilities
├── infra/
│   ├── celery/docker-compose.yaml # Distributed Celery + Redis reference stack
│   └── kubernetes/values.yaml  # Helm chart configuration for KubernetesExecutor
├── spark/
│   ├── master/                 # Spark Master Dockerfile and entrypoint
│   ├── worker/                 # Spark Worker Dockerfile and entrypoint
│   └── notebooks/stock_transform/ # PySpark ETL job & app Dockerfile
├── tests/dags/
│   ├── test_dag_example.py
│   └── test_dag_integrity.py   # Import, cycle, tag, retry, and AST guardrail tests
├── airflow_settings.example.yaml
├── docker-compose.override.yml # Airflow 3 network + optional stock-market stack
├── Dockerfile
└── requirements.txt
```

---

## Quickstart Guide

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (8 GB+ RAM recommended when running the full Spark/Metabase stack)
- [Astronomer CLI (`astro`)](https://www.astronomer.io/docs/astro/cli/install-cli)

### 1. Configure Local Environment

```bash
cp .env.example .env
cp airflow_settings.example.yaml airflow_settings.yaml
```

### 2. Start Core Airflow Stack (Lightweight Mode)

Starts `api-server`, `scheduler`, `dag-processor`, `triggerer`, and `postgres` with tuned local parallelism:

```bash
astro dev start
```

Access the Airflow UI at `http://localhost:8080`.

### 3. Start Full Stock Market Stack (MinIO + Spark + Metabase)

To also run `stock_market` end-to-end with distributed Spark transformation and BI visualization:

```bash
COMPOSE_PROFILES=stock-market astro dev start
```

| Service | URL | Default Credentials |
|---|---|---|
| **Airflow UI** | `http://localhost:8080` | Local admin (no auth required in dev) |
| **MinIO Console** | `http://localhost:9001` | `minio` / `minio123` |
| **Spark Master UI** | `http://localhost:8082` | — |
| **Metabase BI** | `http://localhost:3001` | Setup wizard on first launch |

---

## Testing & CI/CD Guardrails

Run the full validation suite locally or via Docker:

```bash
pytest tests/ -v
```

Automated checks executed by [CI Workflow](.github/workflows/ci.yml):
1. **Syntax & Complexity Linting**: `flake8` validates Python syntax and undefined symbols.
2. **DagBag Import Verification**: Confirms every DAG parses cleanly with zero import errors.
3. **Cycle & Best-Practice Checks**: Ensures acyclic graphs, tags, descriptions, and `retries >= 2`.
4. **AST Anti-Pattern Guardrail**: Scans module bodies to block top-level `Variable.get` and `BaseHook.get_connection` calls.

---

## Documentation Index

- [Architecture & Pipeline Diagrams](docs/architecture.md)
- [Troubleshooting & Performance Case Study](docs/troubleshooting_guide.md)
- [Airflow Executors & Scaling Guide](docs/executors_guide.md)
- [Competency & Engineering Matrix](docs/learning_journal.md)
- [Contribution Guidelines](docs/CONTRIBUTING_GUIDE.md)
- [Course Materials Guide](docs/COURSE_MATERIALS_GUIDE.md)

---

## Contributing & Support

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to contribute, [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community standards, and [`SECURITY.md`](SECURITY.md) for security disclosure policy.
