# System Architecture & Pipeline Design

This document details the engineering design, component topology, and data flows of the Apache Airflow pipelines in this repository.

---

## 1. High-Level Component Topology

The local development and testing environment runs via containerized services orchestrated by Docker Compose and Astro Runtime (`Apache Airflow 3.1.8`):

```mermaid
graph TB
    subgraph Airflow Core Services
        API[Airflow API Server / UI]
        DP[Airflow DAG Processor]
        SCH[Airflow Scheduler]
        TRG[Airflow Triggerer]
        DB[(Metadata DB: PostgreSQL)]
    end

    subgraph OptionalStockMarketStack["Stock-Market Stack (profile: stock-market)"]
        MINIO[(MinIO Object Storage S3)]
        SPARK_M[Apache Spark Master]
        SPARK_W[Apache Spark Worker]
        DOCKER_P[Docker Proxy Socket]
        BI[Metabase Analytics UI :3001]
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

> **Note**: Heavy analytics services (`minio`, `spark-master`, `spark-worker`, `metabase`, `docker-proxy`) are scoped under the Docker Compose `stock-market` profile so baseline `astro dev start` stays lightweight and responsive.

---

## 2. Pipeline 1: Stock Market Data Lakehouse (`dags/stock_market.py`)

An end-to-end financial data pipeline that probes market API availability, stages raw JSON payloads in object storage, runs distributed transformations in Spark, and loads analytics rows into PostgreSQL.

```mermaid
flowchart LR
    A[is_api_available<br/>TaskFlow Sensor] -->|Valid Endpoint| B[get_stock_prices<br/>PythonOperator]
    B -->|Raw JSON Payload| C[store_prices<br/>MinIO S3 Storage]
    C -->|Bucket Object URI| D[format_prices<br/>DockerOperator PySpark]
    D -->|Distributed ETL| E[get_formatted_csv<br/>MinIO File Locator]
    E -->|Transformed CSV URI| F[load_to_dw<br/>Airflow 3 TaskFlow Loader]
    F -->|Upserted Table| G[(PostgreSQL Warehouse)]
```

### Stage Details

1. **`is_api_available` (`@task.sensor`)**: Uses `PokeReturnValue` to probe external API readiness before downstream execution.
2. **`get_stock_prices` (`PythonOperator`)**: Pulls 1-year daily OHLCV history for `NVDA`.
3. **`store_prices` (`PythonOperator`)**: Writes immutable raw JSON into MinIO bucket `stock-market`.
4. **`format_prices` (`DockerOperator` + PySpark)**: Launches an ephemeral PySpark container against `spark-master` to flatten nested arrays and write CSV partitions back to MinIO.
5. **`get_formatted_csv` (`PythonOperator`)**: Locates the generated `.csv` output object in MinIO.
6. **`load_to_dw` (`@task`)**: Streams the CSV directly from MinIO and performs idempotent `ON CONFLICT` upserts into `public.stock_market` in PostgreSQL.

---

## 3. Pipeline 2: Event-Driven Cocktail ETL (`dags/extractor.py`, `dags/cocktail_loader.py`, `dags/ecom.py`)

A modern multi-DAG pipeline demonstrating Asset/Dataset-driven scheduling, TaskGroups, runtime branching, and custom trigger rules.

```mermaid
flowchart TD
    subgraph Upstream DAG: extractor
        E1[get_cocktail<br/>API Extractor] -->|Produces Asset| OUT[(Asset: /tmp/cocktail.json)]
        E1 --> G1[Task Group: check_data]

        subgraph check_data
            C1[check_json_data] --> C2[check_size]
            C2 --> B1{branch_cocktail_type}
            B1 -->|Alcoholic| T1[alcoholic_drink]
            B1 -->|Non-Alcoholic| T2[non_alcoholic_drink]
            T1 --> CL[clean_data<br/>NONE_FAILED_MIN_ONE_SUCCESS]
            T2 --> CL
        end
    end

    OUT -.->|Asset Trigger| CD1[create_table]
    OUT -.->|Asset Trigger| EC1[task_a]

    subgraph Downstream DAG: cocktail_loader
        CD1 --> CD2[read_cocktail_json]
        CD2 --> CD3[store_to_database]
        CD3 --> CD4[verify_storage]
    end

    subgraph Downstream DAG: ecom
        EC1
    end
```

### Key Engineering Features

- **Asset-Driven Scheduling**: `cocktail_loader` and `ecom` trigger automatically whenever `Dataset_cocktail` is updated by `extractor`.
- **Dynamic Branching**: Inspects extracted JSON at runtime to select downstream execution paths cleanly.
- **Trigger Rules**: `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` ensures final verification runs regardless of branch outcome.
- **Transactional Postgres Upserts**: `cocktail_loader` guarantees idempotence via `ON CONFLICT (drink_id) DO UPDATE`.
