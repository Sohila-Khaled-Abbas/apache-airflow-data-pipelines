name: Bug Report
description: Report a broken DAG, task execution failure, or environment issue
labels: ["bug"]
body:
  - type: markdown
    attributes:
      value: Thank you for reporting an issue in the Apache Airflow course repository!
  - type: input
    id: dag_name
    attributes:
      label: Affected DAG / Task
      placeholder: e.g. stock_market / format_prices or extractor / check_data
    validations:
      required: true
  - type: textarea
    id: description
    attributes:
      label: Problem Description
      description: What unexpected behavior or failure occurred?
    validations:
      required: true
  - type: textarea
    id: logs
    attributes:
      label: Relevant Airflow / Task Logs
      description: Paste scheduler, task, or Docker logs here.
      render: shell
