# System Architecture & Pipeline Design

This document details the engineering design, component topology, and data flows of the Apache Airflow pipelines in this repository.

---

## 1. High-Level Component Topology

The repository provides two containerized local topologies:

1. **Primary Portfolio Stack (Root — Astro Runtime / Apache Airflow 3.1.8)**: Runs core Airflow services (`api-server`, `dag-processor`, `scheduler`, `triggerer`, `postgres`) with an optional `stock-market` Compose profile for MinIO, Spark, Docker Proxy, and Metabase.
2. **Distributed Celery Reference Stack (`infra/celery/` — Apache Airflow 2.10.5)**: Runs Redis, PostgreSQL, Airflow Webserver/Scheduler/Triggerer, Flower monitoring, and specialized `cpu` and `gpu` Celery worker services.

```mermaid
graph TB
    subgraph AirflowCore["Airflow 3.1.8 Core Services (Astro Runtime)"]
        API["Airflow API Server / UI :8080"]
        DP["Airflow DAG Processor"]
        SCH["Airflow Scheduler"]
        TRG["Airflow Triggerer"]
        DB[("Metadata DB: PostgreSQL")]
    end

    subgraph OptionalStockMarketStack["Stock-Market Stack (profile: stock-market)"]
        MINIO[("MinIO S3 Storage :9000/:9001")]
        SPARK_M["Apache Spark Master :8082"]
        SPARK_W["Apache Spark Worker :8081"]
        DOCKER_P["Docker Proxy Socket :2376"]
        BI["Metabase Analytics UI :3001"]
    end

    SCH --> DB
    API --> DB
    DP --> DB
    TRG --> DB
    SCH --> MINIO
    SCH --> DOCKER_P
    DOCKER_P --> SPARK_M
    SPARK_M --> SPARK_W
    SPARK_W --> MINIO
    DB --> BI
```

> **Note**: Heavy analytics services (`minio`, `spark-master`, `spark-worker`, `metabase`, `docker-proxy`) are scoped under the Docker Compose `stock-market` profile in [`docker-compose.override.yml`](../docker-compose.override.yml) so baseline `astro dev start` stays lightweight and responsive.

---

## 2. Pipeline 1: Stock Market Data Lakehouse (`dags/stock_market.py`)

An end-to-end financial data pipeline that probes market API availability, stages raw JSON payloads in object storage, runs distributed transformations in Spark, and loads analytics rows into PostgreSQL.

```mermaid
flowchart LR
    A["is_api_available<br/>@task.sensor"] -->|Valid Endpoint| B["get_stock_prices<br/>PythonOperator"]
    B -->|Raw JSON Payload| C["store_prices<br/>MinIO S3 Storage"]
    C -->|Bucket Object URI| D["format_prices<br/>DockerOperator PySpark"]
    D -->|Distributed ETL| E["get_formatted_csv<br/>MinIO File Locator"]
    E -->|Transformed CSV URI| F["load_to_dw<br/>@task PostgresHook"]
    F -->|Upserted Table| G[("PostgreSQL DW: stock_market")]
```

### Stage Details

1. **`is_api_available` (`@task.sensor`)**: Uses `PokeReturnValue` to probe the Yahoo Finance endpoint (`BaseHook.get_connection("stock_api")` with public fallback) before downstream execution.
2. **`get_stock_prices` (`PythonOperator`)**: Pulls 1-year daily OHLCV history for `NVDA` via [`include/stock_market/tasks.py`](../include/stock_market/tasks.py).
3. **`store_prices` (`PythonOperator`)**: Creates the `stock-market` bucket if missing and writes immutable raw JSON (`NVDA/prices.json`) into MinIO.
4. **`format_prices` (`DockerOperator` + PySpark)**: Launches an ephemeral `airflow/stock-app` container running [`spark/notebooks/stock_transform/stock_transform.py`](../spark/notebooks/stock_transform/stock_transform.py) attached to `spark-master` to flatten nested JSON arrays and write CSV partitions back to MinIO.
5. **`get_formatted_csv` (`PythonOperator`)**: Locates the generated `.csv` output object under `<SYMBOL>/formatted_prices/` in MinIO.
6. **`load_to_dw` (`@task`)**: Streams the CSV directly from MinIO and performs idempotent `ON CONFLICT (timestamp) DO UPDATE` upserts into `stock_market` in PostgreSQL (`postgres` connection).

---

## 3. Pipeline 2: Event-Driven Cocktail ETL (`dags/extractor.py`, `dags/cocktail_loader.py`, `dags/ecom.py`)

A multi-DAG pipeline demonstrating Airflow 3 **Asset-driven scheduling**, TaskGroups, runtime branching, lifecycle callbacks, and custom trigger rules.

```mermaid
flowchart TD
    subgraph UpstreamDAG["Upstream DAG: extractor"]
        E1["get_cocktail<br/>PythonOperator"] -->|Emits Asset| OUT[("Asset: /tmp/cocktail.json")]
        E1 --> G1["TaskGroup: check_data"]

        subgraph check_data["check_data TaskGroup"]
            C1["check_json_data"] --> C2["check_size"]
            C2 --> B1{"branch_cocktail_type"}
            B1 -->|Alcoholic| T1["alcoholic_drink"]
            B1 -->|Non-Alcoholic| T2["non_alcoholic_drink"]
            T1 --> CL["clean_data<br/>NONE_FAILED_MIN_ONE_SUCCESS"]
            T2 --> CL
        end
    end

    OUT -.->|Asset Trigger| CD1["create_table"]
    OUT -.->|Asset Trigger| EC1["task_a"]

    subgraph LoaderDAG["Downstream DAG: cocktail_loader"]
        CD1 --> CD2["read_cocktail_json"]
        CD2 --> CD3["store_to_database"]
        CD3 --> CD4["verify_storage"]
    end

    subgraph EcomDAG["Downstream DAG: ecom"]
        EC1
    end
```

### Key Engineering Features

- **Asset-Driven Scheduling**: `cocktail_loader` and `ecom` trigger automatically whenever `Dataset_cocktail` ([`include/datasets.py`](../include/datasets.py)) is updated by `extractor`.
- **Safe Configuration Lookup**: `include/tasks.py` reads `COCKTAIL_API` from environment variables at task execution time (with `DEFAULT_API` fallback), while `dags/extractor.py` passes `{{ var.value.get('api', ...) }}` via Jinja `templates_dict` — avoiding module-scope DB calls.
- **Dynamic Branching & Trigger Rules**: `branch_cocktail_type` inspects the extracted payload at runtime and routes to `alcoholic_drink` or `non_alcoholic_drink`; `clean_data` uses `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` and retains `/tmp/cocktail.json` so downstream Asset consumers never hit a file race condition.
- **Transactional Postgres Upserts**: `cocktail_loader` (`cocktail_db` connection) creates the `cocktails` table if needed and guarantees idempotence via `ON CONFLICT (drink_id) DO UPDATE`.

---

## 4. Pipeline 3: Celery Parallelism & Multi-Queue Routing

Two complementary DAGs cover distributed execution:

- **[`dags/celery_parallelism_demo.py`](../dags/celery_parallelism_demo.py)** *(Airflow 3.1.8 SDK)*: Fans out `prepare_batch` across `worker_alpha`, `worker_beta`, and `worker_gamma` before converging at `aggregate_results`.
- **[`infra/celery/dags/celery.py`](../infra/celery/dags/celery.py)** *(Airflow 2.10.5 Compose stack)*: Routes task `b` to `queue="gpu"` (`airflow-worker`) and tasks `a`, `c`, `d` to `queue="cpu"` (`airflow-worker-2`). See [Executors & Scaling Guide](executors_guide.md) and [Celery DAG Pattern Guide](celery_dag_pattern.md).
