"""
Comprehensive DAG Integrity, Structure, and Static Code Verification Suite.
Validates:
  1. File Import Health: Zero syntax or import errors across all DAGs.
  2. DAG Bag Completeness: All expected production and demo DAGs are registered.
  3. Cyclicity: No cyclic dependency graphs exist.
  4. Best Practices: Task retry minimums (retries >= 2), tags, and descriptions.
  5. Architecture Guardrail: AST static analysis ensuring no top-level database or variable queries.
"""

import ast
import glob
import pytest

EXPECTED_DAG_IDS = {
    "stock_market",
    "extractor",
    "cocktail_loader",
    "ecom",
    "celery_parallelism_demo",
}


@pytest.fixture(scope="session")
def dag_bag():
    """Load all workspace DAGs without tutorial examples."""
    from airflow.models import DagBag

    return DagBag(include_examples=False)


def test_no_import_errors(dag_bag):
    """Ensure all DAG files compile and load with zero import errors."""
    assert not dag_bag.import_errors, f"DAG import errors detected: {dag_bag.import_errors}"


def test_expected_dags_present(dag_bag):
    """Ensure all core pipeline DAGs are registered in the DagBag."""
    loaded_dag_ids = set(dag_bag.dags.keys())
    missing = EXPECTED_DAG_IDS - loaded_dag_ids
    assert not missing, f"Expected DAGs missing from DagBag: {missing}"


def test_dag_retries_configured(dag_bag):
    """Ensure all DAGs enforce appropriate retry minimums (retries >= 2)."""
    for dag_id, dag in dag_bag.dags.items():
        retries = dag.default_args.get("retries", None)
        assert retries is not None and retries >= 2, (
            f"DAG '{dag_id}' default_args['retries'] must be >= 2 for reliability, got {retries}"
        )


def test_dag_tags_and_descriptions(dag_bag):
    """Verify all DAGs are tagged and documented for cataloging and UI discoverability."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' must have tags defined for searchability."
        assert dag.description, f"DAG '{dag_id}' must include a descriptive summary."


def test_no_cyclic_dependencies(dag_bag):
    """Ensure none of the loaded DAGs contain dependency cycles or deadlocks."""
    for dag_id, dag in dag_bag.dags.items():
        try:
            dag.check_cycle()
        except Exception as err:
            pytest.fail(f"DAG '{dag_id}' contains a dependency cycle: {err}")


def test_no_top_level_variable_or_hook_calls():
    """
    Static Code Analysis Guardrail:
    Parses DAG and include Python files as AST trees to ensure module scope does not
    invoke Variable.get or BaseHook.get_connection during scheduler DAG parsing loops.
    """
    python_files = (
        glob.glob("dags/**/*.py", recursive=True)
        + glob.glob("include/**/*.py", recursive=True)
    )

    disallowed_calls = {"Variable.get", "BaseHook.get_connection"}
    violations = []

    for file_path in python_files:
        if "__pycache__" in file_path:
            continue

        with open(file_path, "r", encoding="utf-8") as f:
            try:
                tree = ast.parse(f.read(), filename=file_path)
            except SyntaxError:
                continue

        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue

            for subnode in ast.walk(node):
                if isinstance(subnode, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue

                if isinstance(subnode, ast.Call) and isinstance(subnode.func, ast.Attribute):
                    if isinstance(subnode.func.value, ast.Name):
                        call_name = f"{subnode.func.value.id}.{subnode.func.attr}"
                        if call_name in disallowed_calls:
                            violations.append(
                                f"{file_path}:{subnode.lineno} calls {call_name} at module scope"
                            )

    assert not violations, (
        "Detected top-level Airflow database query anti-patterns causing scheduler lag:\n"
        + "\n".join(violations)
    )
