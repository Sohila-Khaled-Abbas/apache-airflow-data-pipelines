# CeleryExecutor Multi-Queue Stack (`infra/celery/`)

A standalone Docker Compose setup running **Apache Airflow 2.10.5** with `CeleryExecutor`, Redis message broker, PostgreSQL metadata DB, optional Flower monitoring UI, and two queue-specialized Celery workers (`cpu` and `gpu`).

## Services Overview

| Service | Role | Queue / Port |
|---|---|---|
| `postgres` | Airflow metadata DB & Celery result backend | Internal `5432` |
| `redis` | Celery message broker | Internal `6379` |
| `airflow-webserver` | Airflow 2 Web UI & REST API | `http://localhost:8080` (`airflow` / `airflow`) |
| `airflow-scheduler` | Task scheduling & DAG dispatching | Health check `:8974` |
| `airflow-triggerer` | Deferrable operator triggerer | — |
| `airflow-worker` | Specialized Celery worker (`celery worker -q gpu`) | `gpu` queue |
| `airflow-worker-2` | Specialized Celery worker (`celery worker -q cpu`) | `cpu` queue |
| `flower` *(profile: `flower`)* | Celery task & worker monitoring UI | `http://localhost:5555` |

## Quick Start

```bash
cd infra/celery
cp .env.example .env
docker compose --profile flower up -d
```

To inspect or stop the stack:

```bash
docker compose ps
docker compose --profile flower down
```

## Included Multi-Queue Demo DAG

[`dags/celery.py`](dags/celery.py) defines the `celery` DAG (`a() >> [b(), c(), d()]`):
- `a`, `c`, `d` target `queue="cpu"` (`airflow-worker-2`)
- `b` targets `queue="gpu"` (`airflow-worker`)

See [`docs/celery_dag_pattern.md`](../../docs/celery_dag_pattern.md) and [`docs/executors_guide.md`](../../docs/executors_guide.md) for full architecture diagrams and troubleshooting notes.
