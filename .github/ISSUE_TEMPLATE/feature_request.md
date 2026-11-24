name: Feature / Pipeline Proposal
description: Suggest a new DAG, custom operator, or architecture enhancement
labels: ["enhancement"]
body:
  - type: markdown
    attributes:
      value: Share your idea to expand this Airflow portfolio repository!
  - type: input
    id: title
    attributes:
      label: Feature Title
      placeholder: e.g. Add Snowflake data warehouse target to stock_market pipeline
    validations:
      required: true
  - type: textarea
    id: details
    attributes:
      label: Detailed Description
      description: Explain the goal and implementation architecture.
    validations:
      required: true
