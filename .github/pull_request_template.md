## Pull Request Overview

### Summary of Changes
- [ ] Added/Updated DAG: 
- [ ] Updated Task or Operator:
- [ ] Updated Configuration or Connection:
- [ ] Added/Updated Documentation or Tests:

### Description
Provide a concise description of what changes this PR introduces, the architectural rationale, and how it impacts pipeline execution.

### Architectural Checklist
- [ ] **No Top-Level Database/Variable Calls**: Verified with static tests that `Variable.get` or `BaseHook.get_connection` are not executed in global module scope.
- [ ] **Data-Aware Scheduling**: Inlets/Outlets and Datasets/Assets are properly configured without cross-DAG race conditions.
- [ ] **Idempotence**: Tasks can safely be rerun without causing duplicate records or corrupt data.
- [ ] **Retries & Timeouts**: Appropriate retry parameters (`retries >= 2`) and network timeouts are configured on all external calls.
- [ ] **Tags & Metadata**: DAG contains meaningful `tags`, `description`, and `default_args`.

### Testing & Verification
- [ ] `pytest tests/` passed cleanly.
- [ ] Tested locally with `astro dev start` / Astro CLI.
