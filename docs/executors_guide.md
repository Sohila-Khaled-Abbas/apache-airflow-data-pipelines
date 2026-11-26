# Apache Airflow Executors & Scaling Architecture

This guide compares Airflow executor models and explains the infrastructure templates stored under [`infra/`](../infra/).

---

## 1. Executor Comparison

| Executor | Parallelism | Worker Isolation | Broker Required | Typical Use Case |
|---|---|---|---|---|
| **SequentialExecutor** | Single task at a time | Shared scheduler process | None | Minimal single-file SQLite debugging |
| **LocalExecutor** | Multi-process on one host | Separate local processes | None (uses Metadata DB) | Local development, single-node deployments |
| **CeleryExecutor** | Distributed across worker nodes | Separate worker containers/hosts | Redis / RabbitMQ | Production clusters with steady concurrent load and queue routing |
| **KubernetesExecutor** | Dynamic per-task Pods | Isolated Kubernetes Pod per task | None (Kubernetes API) | Elastic workloads, strict dependency/resource isolation |

---

## 2. Included Infrastructure Templates

### Celery Multi-Queue Cluster (`infra/celery/docker-compose.yaml`)

[`infra/celery/docker-compose.yaml`](../infra/celery/docker-compose.yaml) defines a multi-container **Apache Airflow 2.10.5** cluster backed by PostgreSQL 13, Redis 7.2 broker, Flower monitoring, Airflow Webserver/Scheduler/Triggerer, and **two queue-specialized Celery workers**:

| Service | Command / Role | Queue Consumed |
|---|---|---|
| `airflow-worker` | `celery worker -q gpu` | `gpu` |
| `airflow-worker-2` | `celery worker -q cpu` | `cpu` |
| `flower` *(profile: `flower`)* | `celery flower` | Web monitor on `:5555` |

> **Version Note**: The `infra/celery` compose stack runs `apache/airflow:2.10.5`, so its local demo DAG ([`infra/celery/dags/celery.py`](../infra/celery/dags/celery.py)) uses `from airflow.decorators import dag, task`. Root portfolio DAGs under [`dags/`](../dags/) target **Airflow 3.1.8** (`from airflow.sdk import dag, task`).

#### Quick Start with Docker Compose

```bash
# 1. Navigate to the Celery infra directory
cd infra/celery

# 2. Copy the environment template
cp .env.example .env

# 3. Start the core stack (include --profile flower for Flower UI)
docker compose --profile flower up -d

# 4. Access the services
#   Airflow UI:     http://localhost:8080  (default: airflow / airflow)
#   Flower Monitor: http://localhost:5555
```

#### Key Environment Variables (`infra/celery/.env.example`)

| Variable | Description | Default |
|---|---|---|
| `AIRFLOW_IMAGE_NAME` | Airflow container image | `apache/airflow:2.10.5` |
| `AIRFLOW__CORE__EXECUTOR` | Executor type | `CeleryExecutor` |
| `AIRFLOW__CORE__PARALLELISM` | Max parallel tasks across cluster | `32` |
| `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG` | Max concurrent tasks per DAG | `16` |
| `AIRFLOW__CELERY__WORKER_CONCURRENCY` | Worker process concurrency | `16` |
| `AIRFLOW__CELERY__WORKER_AUTOSCALE` | Autoscale `max,min` processes | `16,4` |

#### Testing Celery Queue Routing & Parallel Fan-Out

1. **Multi-Queue Routing DAG ([`infra/celery/dags/celery.py`](../infra/celery/dags/celery.py))**:
   - Task `a` runs on `queue="cpu"` (`airflow-worker-2`)
   - Fans out in parallel to `b` (`queue="gpu"` on `airflow-worker`) and `c`, `d` (`queue="cpu"` on `airflow-worker-2`)
   - See [Celery DAG Pattern Guide](celery_dag_pattern.md) and [`diagrams/celery_dag_flow.mmd`](diagrams/celery_dag_flow.mmd) for detailed patterns.

2. **Airflow 3 Parallelism Demo ([`dags/celery_parallelism_demo.py`](../dags/celery_parallelism_demo.py))**:
   - `prepare_batch` fans out to `worker_alpha`, `worker_beta`, and `worker_gamma`, then converges on `aggregate_results`.

Trigger from the Airflow UI or CLI:

```bash
airflow dags trigger celery
# or on the Airflow 3 stack:
airflow dags trigger celery_parallelism_demo
```

---

### Kubernetes Helm Configuration (`infra/kubernetes/values.yaml`)

[`infra/kubernetes/values.yaml`](../infra/kubernetes/values.yaml) provides the Helm chart configuration for `KubernetesExecutor` (`executor: "KubernetesExecutor"`, `defaultAirflowTag: "2.10.5"`), including worker pod templates, resource settings, and DAG synchronization options.

---

## 3. Local Tuning (`LocalExecutor` via Astro Runtime)

On resource-constrained local environments (such as a 6 GB Docker Desktop WSL2 VM), Airflow's default `parallelism = 32` spawns up to 32 worker processes inside the scheduler container. Tuning root [`.env.example`](../.env.example) controls concurrency cleanly:

```dotenv
AIRFLOW__CORE__PARALLELISM=6
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=2
AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=30
```
