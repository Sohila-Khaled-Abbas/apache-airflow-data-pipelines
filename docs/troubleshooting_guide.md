# Production Troubleshooting Case Study: Diagnosing Airflow Latency & Scheduler Freezes

This document details the real-world performance, concurrency, and compatibility issues encountered during development, the diagnostic root-cause analysis, and the architectural remediations applied.

---

## 1. Problem Statement

During local execution on Docker Desktop (WSL2):

- The Airflow UI exhibited extreme page loading latency and request timeouts.
- DAGs remained indefinitely in `queued` or failed to transition to `running`.
- Docker CLI commands intermittently failed with:
  ```text
  request returned 500 Internal Server Error for API route http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/...
  ```
- Airflow Scheduler logs revealed massive heartbeat lapses:
  ```text
  Heartbeat recovered after 634.85 seconds
  Marked 1 SchedulerJob instances as failed
  Heartbeat recovered after 316.07 seconds
  ```

---

## 2. Root Cause Analysis

### Cause A: Docker Desktop Linux Engine Socket Contention

Inspection of live container resource utilization revealed that external, non-Airflow background services (`qdrant`, multiple `n8n` instances) were consuming over **700% CPU** within the shared WSL2 Linux VM.

This CPU starvation caused:

1. Docker daemon's Windows named pipe (`//./pipe/dockerDesktopLinuxEngine`) to drop connections or return `500` / `502 Bad Gateway`.
2. The Airflow Scheduler to be starved of CPU cycles, causing its main loop to miss the 30-second heartbeat threshold by up to 10 minutes (`634s`).
3. Active `SchedulerJob` instances to be marked as dead by self-monitoring checks, stranding queued tasks.

### Cause B: Top-Level Database & Variable Querying (Anti-Pattern)

In an earlier version of [`include/tasks.py`](../include/tasks.py):

```python
# ANTI-PATTERN: Executed at top-level module scope during every DAG parse
API = Variable.get("api")
```

Every time the Airflow DAG Processor / Scheduler parses DAG files:

1. Python re-imports modules and executes module-level code.
2. `Variable.get("api")` triggered a synchronous metadata database query on every parse cycle.
3. Missing variables crashed DAG parsing with `KeyError`, while present variables created metadata DB connection pool thrashing.

### Cause C: Race Condition in File-Based Datasets

In [`dags/extractor.py`](../dags/extractor.py), `clean_data` originally deleted `/tmp/cocktail.json` immediately upon completion of the upstream DAG. When downstream Asset-triggered DAG [`dags/cocktail_loader.py`](../dags/cocktail_loader.py) launched from the Asset event, the file had already been removed, causing immediate task failure.

### Cause D: Airflow 3 SDK & Decorator Compatibility Issues

1. **Legacy `astro-sdk-python` incompatibility**: `aql.load_file` failed on Airflow 3 due to the removed `core/enable_xcom_pickling` configuration.
2. **Airflow 3 `BaseHook` import path**: Importing `BaseHook` from `airflow.sdk` failed in CI on Airflow 3.1.8; `airflow.hooks.base.BaseHook` is the valid import path.
3. **Celery DAG decorator syntax error**: Placing `@task(executor="celery")` inline after `def celery():` caused a Python parse error (documented in [Celery DAG Pattern Guide](celery_dag_pattern.md)).

---

## 3. Remediations & Architecture Fixes

| Issue | Remediation | Result |
|---|---|---|
| **WSL2 Socket Deadlock** | Stopped orphaned high-CPU containers, rebooted `docker-desktop` WSL subsystem, tuned [`.env.example`](../.env.example), and scoped heavy services under Compose profile `stock-market`. | Docker daemon latency dropped from ~15,000ms to <10ms; baseline `astro dev start` stays lightweight and scheduler heartbeats stabilize. |
| **Top-Level Variable Queries** | Refactored [`include/tasks.py`](../include/tasks.py) to read `os.environ.get("COCKTAIL_API", DEFAULT_API)` inside `_get_cocktail()`, moved Jinja `var.value.get(...)` into runtime `templates_dict` in `extractor.py`, and deferred `BaseHook.get_connection()` inside runtime functions in `stock_market`. | Zero DB overhead during DAG parsing loops; parse times stay sub-second. |
| **Dataset Race Condition** | Updated `clean_data` in [`dags/extractor.py`](../dags/extractor.py) to validate and retain `/tmp/cocktail.json` for downstream Asset consumers. | `cocktail_loader` reliably ingests and upserts records into PostgreSQL. |
| **Airflow 3 Compatibility** | Replaced `astro-sdk-python` with native `Minio` + `PostgresHook` loader in [`dags/stock_market.py`](../dags/stock_market.py) and fixed `airflow.hooks.base.BaseHook` imports. | Clean DAG loading and execution on Apache Airflow 3.1.8. |
| **Automated AST Guardrail** | Added `test_no_top_level_variable_or_hook_calls` in [`tests/dags/test_dag_integrity.py`](../tests/dags/test_dag_integrity.py) and CI checks in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml). | Blocks top-level `Variable.get` / `BaseHook.get_connection` regressions before merge. |
