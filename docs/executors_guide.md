# Apache Airflow Executors & Scaling Architecture

This guide compares Airflow executor models and explains the reference configurations stored under `infra/`.

---

## 1. Executor Comparison

| Executor | Parallelism | Worker Isolation | Broker Required | Typical Use Case |
|---|---|---|---|---|
| **SequentialExecutor** | Single task at a time | Shared scheduler process | None | Minimal single-file SQLite debugging |
| **LocalExecutor** | Multi-process on one host | Separate local processes | None (uses Metadata DB) | Local development, single-node deployments |
| **CeleryExecutor** | Distributed across worker nodes | Separate worker containers/hosts | Redis / RabbitMQ | Production clusters with steady concurrent load |
| **KubernetesExecutor** | Dynamic per-task Pods | Isolated Kubernetes Pod per task | None (Kubernetes API) | Elastic workloads, strict dependency/resource isolation |

---

## 2. Included Infrastructure Templates

### Celery Cluster (`infra/celery/docker-compose.yaml`)

Defines a multi-container cluster backed by PostgreSQL, Redis broker, Celery workers, Flower monitoring, and Airflow Scheduler/Triggerer/Webserver. Tested alongside `dags/celery_parallelism_demo.py` to demonstrate parallel fan-out across distributed workers.

#### Quick Start with Docker Compose

```bash
# 1. Navigate to the Celery infra directory
cd infra/celery

# 2. Copy the environment template
cp .env.example .env

# 3. Start the stack
docker-compose up -d

# 4. Access the services
#   Airflow UI:    http://localhost:8080
#   Flower Monitor: http://localhost:5555
```

#### Environment Variables

See `.env.example` for all configurable options. Key settings:

| Variable | Description | Default |
|----------|-------------|---------|
| `AIRFLOW__CORE__EXECUTOR` | Executor type | `CeleryExecutor` |
| `AIRFLOW__CORE__PARALLELISM` | Max parallel tasks | `32` |
| `AIRFLOW__CELERY__WORKER_CONCURRENCY` | Worker concurrency | `16` |

#### Testing with the Demo DAG

The `dags/celery_parallelism_demo.py` DAG demonstrates parallel fan-out execution:

- `prepare_batch` → fans out to `worker_alpha`, `worker_beta`, `worker_gamma` (parallel)
- All three workers converge to `aggregate_results`

Trigger the DAG manually from the Airflow UI or via CLI:

```bash
airflow dags trigger celery_parallelism_demo
```

Then monitor task distribution in Flower at `http://localhost:5555`.

### Kubernetes Helm Configuration (`infra/kubernetes/values.yaml`)

Configures `KubernetesExecutor` deployment settings, worker pod templates, resource quotas, and DAG synchronization for cluster deployments.

---

## 3. Local Tuning (`LocalExecutor`)

On resource-constrained local environments (such as a 6 GB Docker Desktop WSL2 VM), Airflow's default `parallelism = 32` spawns 32 worker processes inside the scheduler container. Tuning `.env` controls concurrency cleanly:

```dotenv
AIRFLOW__CORE__PARALLELISM=6
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=2
AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=30
```
