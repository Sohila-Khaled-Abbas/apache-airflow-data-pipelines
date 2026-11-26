# Course Materials Guide — Udemy "Apache Airflow: The Hands-On Guide"

This repository preserves snapshots of the **Udemy course materials** under [`course-materials/udemy-hands-on-guide/`](../course-materials/udemy-hands-on-guide/) for reference and comparison against the capstone portfolio implementation.

---

## What's Included

Course sections are organized by their original section number:

| Section | Description | Link |
|---|---|---|
| `course_sec_4_2` | Initial Astro project scaffolding & empty `ecom` DAG | [`course_sec_4_2/`](../course-materials/udemy-hands-on-guide/course_sec_4_2/) |
| `course_sec_4_6` | Basic `ecom` DAG structure | [`course_sec_4_6/`](../course-materials/udemy-hands-on-guide/course_sec_4_6/) |
| `course_sec_4_9` | Early `extractor` + `ecom` DAGs & integrity tests | [`course_sec_4_9/`](../course-materials/udemy-hands-on-guide/course_sec_4_9/) |
| `course_sec_4_10` | `extractor` and `ecom` progression (Section 4.10) | [`course_sec_4_10/`](../course-materials/udemy-hands-on-guide/course_sec_4_10/) |
| `course_sec_4_11` | `Dataset` scheduling & `include/tasks.py` introduction | [`course_sec_4_11/`](../course-materials/udemy-hands-on-guide/course_sec_4_11/) |
| `course_sec_4_12` | Extractor lifecycle callbacks (`include/extractor/callbacks.py`) | [`course_sec_4_12/`](../course-materials/udemy-hands-on-guide/course_sec_4_12/) |
| `course_sec_4_13` | Unit testing introduction (`tests/unit_tests/test_check_size.py`) | [`course_sec_4_13/`](../course-materials/udemy-hands-on-guide/course_sec_4_13/) |

Each section directory mirrors the original Astro project structure (`dags/`, `include/`, `tests/`, etc.) so you can compare section-by-section progress against the consolidated portfolio in the repository root.

---

## What Was Excluded (Security & Cleanliness)

The following runtime or local-only files are excluded from tracking in course snapshots:

| Excluded | Reason |
|---|---|
| `.env` | Contains local credentials; use `.env.example` instead |
| `airflow_settings.yaml` | Local connection/variable config; use `airflow_settings.example.yaml` |
| `airflow.db` | Local SQLite metadata DB |
| `__pycache__/`, `*.pyc` | Python bytecode cache |
| `__MACOSX/`, `.DS_Store` | macOS metadata |
| `include/data/`, `spark/*/data/` | Local runtime data volumes |

---

## How to Use the Course Materials

1. **Browse by section** — Inspect any `course_sec_4_*` folder to trace how concepts were introduced step-by-step.
2. **Compare with capstone** — The root [`dags/`](../dags/), [`include/`](../include/), [`spark/`](../spark/), [`infra/`](../infra/), and [`tests/`](../tests/) contain the refactored, tested portfolio implementation.
3. **Run locally** — Copy a section's files into a fresh Astro project and run `astro dev start` (adjusting connections as needed).
4. **Keep snapshots immutable** — Treat `course-materials/` as read-only historical reference snapshots.

---

## Relationship to Capstone

| Aspect | Course Materials | Capstone (Root) |
|---|---|---|
| Structure | Section-by-section snapshots | Consolidated production-style layout |
| Airflow Version | Course-era Astro/Airflow 2.x patterns | Airflow 3.1.8 SDK + Airflow 2.10.5 Celery reference stack |
| DAGs | Lesson examples per section | Refactored, tested, AST-guarded pipelines |
| Docs | Starter Astro READMEs | Architecture, executors, Celery pattern, troubleshooting, authoring guides |
| CI/CD | None | GitHub Actions workflow (`flake8` + `pytest` + AST guardrails) |

---

## License

Course materials are preserved for personal learning evidence. The capstone implementation and all original files in this repository are licensed under the [MIT License](../LICENSE).
