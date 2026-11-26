# Course Materials Guide — Udemy "Apache Airflow: The Hands-On Guide"

This repository preserves snapshots of the **Udemy course materials** under [`course-materials/udemy-hands-on-guide/`](../course-materials/udemy-hands-on-guide/) for reference and comparison against the capstone portfolio implementation.

---

## What's Included

Course sections are organized by their original section number:

| Section | Description |
|---------|-------------|
| `course_sec_4_2` | DAGs, include, plugins, tests, Astro runtime config |
| `course_sec_4_6` | Airflow project scaffolding |
| `course_sec_4_9` | DAG integrity tests, Astro runtime config |
| `course_sec_4_10` | Section 4.10 course materials |
| `course_sec_4_11` | Section 4.11 course materials |
| `course_sec_4_12` | Section 4.12 course materials |
| `course_sec_4_13` | Section 4.13 course materials |

Each section directory mirrors the course's original project structure (`dags/`, `include/`, `tests/`, `spark/`, etc.) so you can compare section-by-section progress against the final capstone in the root of this repo.

---

## What Was Excluded (for security & cleanliness)

The following files were intentionally excluded from the course materials snapshot:

| Excluded | Reason |
|----------|--------|
| `.env` | Contains local credentials; use `.env.example` instead |
| `airflow_settings.yaml` | Local connection/variable config; use `.example` variant |
| `airflow.db` | Local SQLite metadata DB |
| `__pycache__/` | Python bytecode cache |
| `*.pyc` | Compiled Python files |
| `__MACOSX/` | macOS metadata |
| `.DS_Store` | macOS Finder metadata |
| `include/data/`, `spark/*/data/` | Local runtime data volumes |

---

## How to Use the Course Materials

1. **Browse by section** — Each `course_sec_4_*` folder contains that section's project files.
2. **Compare with capstone** — The root `dags/`, `include/`, `spark/`, and `tests/` represent the final, refactored portfolio implementation.
3. **Run locally** — Copy a section's files into a fresh Astro project and run `astro dev start` (adjust connections as needed).
4. **Do not modify** — These are reference snapshots; treat them as read-only evidence of course progress.

---

## Relationship to Capstone

| Aspect | Course Materials | Capstone (root) |
|--------|-----------------|-----------------|
| Structure | Section-by-section snapshots | Consolidated production layout |
| DAGs | Learning examples per section | Refactored, tested, production-ready |
| Docs | Minimal | Full architecture, troubleshooting, executor guides |
| CI/CD | None | GitHub Actions workflow + AST guardrails |

---

## License

Course materials are included for personal learning evidence. The capstone implementation and all new files in this repo are under the [MIT License](../LICENSE).