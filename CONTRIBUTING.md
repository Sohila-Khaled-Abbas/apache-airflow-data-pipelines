# Contributing to Apache Airflow Data Pipelines

Thank you for your interest in contributing! This repository is published as **learning evidence** for the Apache Airflow: The Hands-On Guide course, and contributions that improve clarity, correctness, or educational value are welcome.

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
- DAGs must include `tags`, `description`, `default_args` with `retries >= 2`
- No top-level `Variable.get()` or `BaseHook.get_connection` calls (enforced by CI AST test)
- All Python code should pass `flake8` linting
- Add or update tests under `tests/dags/` for new DAGs

### 4. Run Checks Locally

```bash
# Lint
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --exclude=.astro,venv,__pycache__,spark

# Tests
pytest tests/ -v --tb=short
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
|------|---------|------------|
| `dags/` | Production Airflow DAGs | Must pass integrity tests; no top-level DB calls |
| `include/` | Shared helpers, assets, callbacks | Keep imports at top; defer connection lookups into tasks |
| `docs/` | Architecture, troubleshooting, guides | Use Mermaid diagrams; keep updated with code changes |
| `infra/` | Docker Compose, Kubernetes configs | Reference templates; not for production use as-is |
| `spark/` | PySpark jobs & container definitions | Test against local Spark cluster |
| `tests/` | DAG integrity & import tests | Add tests for new DAGs |
| `course-materials/` | Udemy course snapshots | Reference only; do not modify derived data |

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

See our [Security Policy](SECURITY.md) for vulnerability disclosure.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).