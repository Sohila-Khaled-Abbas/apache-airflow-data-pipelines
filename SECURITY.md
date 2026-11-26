# Security Policy

## Supported Versions

This repository is a learning portfolio for Apache Airflow coursework. Security patches are not formally issued for historical course snapshots.

| Version | Supported |
|---|---|
| Current `main` branch | Yes |

## Reporting a Vulnerability

If you discover a security vulnerability in this repository (e.g., exposed credentials, insecure DAG patterns, hardcoded secrets), please:

1. **Do not** open a public issue for security concerns.
2. Contact the repository maintainer directly via GitHub.
3. Include details of the vulnerability, steps to reproduce, and suggested remediation.

## Security Best Practices in This Repo

- **No secrets committed**: `.env`, `infra/celery/.env`, `airflow_settings.yaml`, and `airflow.db` are ignored in [`.gitignore`](.gitignore).
- **Example configs only**: [`airflow_settings.example.yaml`](airflow_settings.example.yaml), [`.env.example`](.env.example), and [`infra/celery/.env.example`](infra/celery/.env.example) contain local-dev defaults/placeholders only.
- **Connection IDs referenced, not hardcoded**: DAGs reference Airflow connections by ID; values are injected at runtime.

## Disclosure

We will acknowledge receipt of your report within 5 business days and provide an initial assessment within 10 business days.
