# Migration Summary: Airflow 2.9 to 3.2

This repository has been upgraded to Apache Airflow 3.2. The following changes were implemented to ensure compatibility and follow best practices.

## Changes Implemented

### 1. Updated Imports
- Migrated from `airflow.operators.python_operator` to `airflow.operators.python`.
- Migrated from `airflow.operators.dummy_operator` to `airflow.operators.empty`.
- Updated provider imports for `TriggerDagRunOperator` and `PostgresHook` to their latest stable paths:
  - `airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator`
  - `airflow.providers.postgres.hooks.postgres.PostgresHook`

### 2. Operator Upgrades
- Replaced all instances of `DummyOperator` with `EmptyOperator`.
- Updated `PythonOperator` usage by removing the deprecated `provide_context=True` argument (it is now redundant).

### 3. DAG Definition Improvements
- Refactored the DAG to use the **Context Manager** pattern (`with DAG(...) as dag:`), which is the recommended approach in Airflow 3.
- Updated the DAG scheduling parameter from `schedule_interval` to `schedule`.

### 4. Task Management
- Renamed task variables (e.g., `call_disaster_api_task`) to avoid shadowing the python callable function names, improving code clarity and preventing potential scoping issues.

### 5. Security and Configuration
- Removed hardcoded API tokens and sensitive IDs.
- Integrated `airflow.models.Variable` to retrieve configuration dynamically:
  - `DISASTER_API_TOKEN`
  - `DISASTER_CATALOG_ID`
  - `DISASTER_CONN_ID` (defaults to `farmai_conn`)

### 6. Performance Optimization
- Refactored the database insertion logic to use **Bulk Insertion** via `execute_values`.
- Implemented **Idempotency** using `ON CONFLICT (_id) DO NOTHING`, ensuring that duplicate runs do not fail or create duplicate data, and eliminating unnecessary per-record existence checks.

## Verification
- Validated syntax and import structure using `python3 -m py_compile`.
- Ensured all tasks are correctly associated with the DAG via the context manager.
- Cleaned up environment-specific artifacts (`__pycache__`).
