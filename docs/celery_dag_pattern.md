# Celery DAG Pattern - Learning Guide

## Overview
This document explains the correct pattern for using Airflow decorators with CeleryExecutor, based on the fix applied to `celery.py`.

## Key Concepts

### 1. `@dag` Decorator
- Applied to the DAG function
- Configures the DAG-level settings
- Must be on its own line above `def dag_name():`

```python
@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def my_dag():
    ...
```

### 2. `@task` Decorator
- Applied to individual task functions **inside** the DAG function
- Does NOT take `executor` parameter on the DAG function
- When using `CeleryExecutor`, tasks automatically run as celery tasks

```python
@task
def my_task():
    ...
```

### 3. Common Mistake (Fixed)
**Before (INCORRECT):**
```python
@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def celery():@task(executor="celery")  # ← Syntax error! Decorator on same line
```

**After (CORRECT):**
```python
@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def celery():  # ← Proper newline
    @task       # ← task decorator on individual tasks inside
    def a():
        ...
```

## The Fixed Code (`celery.py`)

```python
from airflow.decorators import dag, task
from pendulum import datetime
from time import sleep

@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def celery():
    """DAG using CeleryExecutor with decorated tasks"""

    @task
    def a():
        print("A")
        sleep(15)

    @task
    def b():
        print("B")
        sleep(15)

    @task
    def c():
        print("C")
        sleep(15)

    @task
    def d():
        print("D")
        sleep(15)
    
    # Set task dependencies
    a() >> [b(), c(), d()]

# Instantiate the DAG
celery()
```

## Diagram: DAG Task Flow

```mermaid
graph TD
    style Airflow fill:#f9f9f9,stroke:#333,stroke-width:2px
    
    subgraph "DAG: celery"
        A[Task A] -->|dependency| B[Task B]
        A -->|dependency| C[Task C]
        A -->|dependency| D[Task D]
        B -->|parallel| C
        C -->|parallel| D
    end
    
    style A fill:#4CAF50,color:white
    style B fill:#4CAF50,color:white
    style C fill:#4CAF50,color:white
    style D fill:#4CAF50,color:white
    
    note Airflow: "a() >> [b(), c(), d()]\ncreates: A -> B, C, D"
```

## Execution Flow

1. **DAG Definition**: The `@dag` decorator defines the DAG with `start_date`, `schedule`, and `catchup` settings
2. **Task Registration**: Each `@task` decorator registers the function as an Airflow task
3. **Dependency Setup**: `a() >> [b(), c(), d()]` sets up:
   - A runs first
   - B, C, and D run in parallel after A completes
4. **Execution**: With `CeleryExecutor`, tasks are offloaded to Celery workers

## Best Practices

| ✅ Do | ❌ Don't |
|------|---------|
| Use `@dag` on its own line above `def` | Put decorators on the same line as `def` |
| Use `@task` on individual functions **inside** the DAG | Use `@task(executor="...")` on the DAG function |
| Standardize spacing: `sleep(15)` not `sleep (15)` | Inconsistent spacing around operators |
| End DAG function with `celery()` instantiation | Forget to call the DAG function |

## Troubleshooting

**Error**: `[Pyrefly Error] Line 9: Parse error: Expected class, function definition or async function definition after decorator`

**Cause**: Decorator syntax error - decorator on same line as function definition

**Fix**: Add a newline between the decorator and function definition

```python
# Wrong:
@dag(...)
def celery():@task(executor="celery")

# Right:
@dag(...)
def celery():
    @task
    def a():
        ...
```