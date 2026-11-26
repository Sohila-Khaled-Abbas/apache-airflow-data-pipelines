# Contributing to Apache Airflow Data Pipelines

Thank you for your interest in contributing! This repository is published as **learning evidence** for the *Apache Airflow: The Hands-On Guide* course, and contributions that improve clarity, correctness, or educational value are welcome.

For Airflow DAG coding standards, Asset conventions, and CI guardrail details, see [`docs/dag_authoring_guide.md`](docs/dag_authoring_guide.md).

## How to Contribute

### 1. Fork & Clone

```bash
git clone https://github.com/<your-username>/apache-airflow-data-pipelines.git
cd apache-airflow-data-pipelines
```

### 2. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 3. Make Your Changes

Follow the repository conventions:
- Root DAGs must include `tags`, `description`, and `default_args` with `retries >= 2`
- No top-level `Variable.get()` or `BaseHook.get_connection()` calls (enforced by CI AST test)
- All Python code should pass `flake8` linting
- Add or update integrity tests under [`tests/dags/`](tests/dags/) and mocked unit tests under [`tests/unit/`](tests/unit/)

### 4. Run Checks Locally

```bash
# Lint (matches CI workflow)
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --exclude=.astro,venv,.venv,__pycache__,spark,course-materials

# Run DAG integrity, AST guardrail, and unit tests (Linux/WSL2/Docker/`astro dev pytest`)
pytest tests/ -v --tb=short

# On native Windows (where Airflow DagBag requires POSIX fcntl), run unit + AST checks:
pytest tests/unit/ -v && pytest tests/dags/test_dag_integrity.py -k test_no_top_level_variable_or_hook_calls -v
```

### 5. Commit & Push

```bash
git add .
git commit -m "feat: describe your change"
git push origin feature/your-feature-name
```

### 6. Open a Pull Request

Use the [pull request template](.github/pull_request_template.md) and ensure all checklist items are addressed.

## Repository Areas

| Area | Purpose | Guidelines |
|---|---|---|
| [`dags/`](dags/) | Production Airflow 3 DAGs | Must pass integrity tests; no top-level DB calls |
| [`include/`](include/) | Shared helpers, Assets, callbacks | Keep imports clean; defer connection lookups into tasks |
| [`docs/`](docs/README.md) | Architecture, troubleshooting, guides, diagrams | Use valid Mermaid diagrams; keep synchronized with code |
| [`infra/`](infra/README.md) | Celery Docker Compose & Kubernetes Helm configs | Reference templates; not for production use as-is |
| [`spark/`](spark/) | PySpark jobs & container definitions | Test against local `stock-market` Spark cluster |
| [`tests/`](tests/) | DAG integrity, AST guardrail, & unit tests | Add tests for new DAGs and task helpers |
| [`course-materials/`](course-materials/) | Udemy course section snapshots | Historical reference only; do not modify |

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

See our [Security Policy](SECURITY.md) for vulnerability disclosure.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
