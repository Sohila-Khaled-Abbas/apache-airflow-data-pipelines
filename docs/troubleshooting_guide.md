# Production Troubleshooting Case Study: Diagnosing Airflow Latency & Scheduler Freezes

This document details the real-world performance issues encountered in this environment, the systematic diagnostic methodology used to identify root causes, and the architectural remediations applied.

---

## 1. Problem Statement

During local execution:
- Airflow UI exhibited extreme page loading latency and request timeouts.
- DAGs remained indefinitely in `queued` or failed to transition to `running`.
- Docker CLI commands intermittently failed with:
  ```text
  request returned 500 Internal Server Error for API route http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/...
  ```
- The Airflow Scheduler logs revealed massive heartbeat lapses:
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
3. Active `SchedulerJob` instances being marked as dead by self-monitoring checks, stranding queued tasks.

### Cause B: Top-Level Database & Variable Querying (Anti-Pattern)
In `include/tasks.py`:
```python
# ANTI-PATTERN: Executed at the top-level module scope
API = Variable.get('api')
```
Every time the Airflow Scheduler parses DAG files (by default every few seconds):
1. The Python interpreter re-imports all modules.
2. `Variable.get('api')` triggered a synchronous database query to the Airflow metadata database.
3. If the variable did not exist, the scheduler crashed the DAG parse with a `KeyError`.
4. Even when present, this constant roundtrip created intense database connection pool thrashing.

### Cause C: Race Condition in File-Based Datasets
In `dags/extractor.py`, `clean_data` deleted `/tmp/cocktail.json` immediately upon completion of the upstream DAG. When the downstream DAG `cocktail_loader` triggered from the dataset event, the file had already been deleted, causing immediate task failure.

---

## 3. Remediations & Architecture Fixes

| Issue | Remediation | Result |
|---|---|---|
| **WSL2 Socket Deadlock** | Stopped orphaned high-CPU background containers; terminated and rebooted the `docker-desktop` WSL subsystem. | Docker daemon latency dropped from 15,000ms to <10ms; scheduler heartbeats stabilized to normal 5s intervals. |
| **Top-Level Variable Queries** | Removed global `Variable.get()` calls; deferred variable lookup to task execution inside functions with fallback defaults. | Zero DB overhead during DAG parsing loops; DAG parse time dropped from seconds to milliseconds. |
| **Dataset Race Condition** | Refactored `clean_data` to preserve the dataset for downstream consumption and log status cleanly. | Downstream `cocktail_loader` reads the file reliably and inserts records into PostgreSQL. |
| **Automated Guardrail** | Added an AST-based test in `tests/dags/test_dag_integrity.py` that fails CI if any top-level `Variable.get` or `BaseHook.get_connection` is introduced. | Prevents performance regressions before code reaches production. |
