# Day 7 – Databricks Job Pipeline (Bronze → Silver → Gold)

## Objective
To orchestrate an end-to-end data pipeline using Databricks Jobs by converting existing Bronze, Silver, and Gold notebooks into a multi-task workflow.

## Pipeline Design
The pipeline is designed using a modular approach:
- Bronze Module: Raw data ingestion
- Silver Module: Data cleaning and validation
- Gold Module: Business-level aggregations

Each module is implemented as a separate notebook and executed in sequence using Databricks Jobs.

## Workflow Orchestration
- Bronze task runs first
- Silver task depends on Bronze
- Gold task depends on Silver
- The pipeline is scheduled to run automatically

## Tools Used
- Databricks Community Edition
- Apache Spark (PySpark)
- Delta Lake
- Databricks Jobs (Workflows)

## Notes
This implementation reuses the transformation logic developed earlier and focuses on automation, dependency management, and production-style execution.
