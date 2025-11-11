# Dagster Jobs

This document describes the Dagster jobs and orchestration patterns used in the warehouse.

## Overview

Dagster serves as the orchestration engine for all data pipelines. It manages:

- Job scheduling and execution
- Dependency resolution
- Resource management
- Execution monitoring
- Asset materialization

## Job Architecture

### Dagster Configuration

Dagster is configured in `dagster/dagster.yaml`:

```yaml
scheduler:
  module: dagster.core.scheduler
  class: DagsterDaemonScheduler

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 5
```

**Storage**: PostgreSQL stores all Dagster metadata including:
- Run history and logs
- Asset materializations
- Schedule state
- Event logs

### Workspace Configuration

The workspace (`dagster/workspace.yaml`) loads pipeline definitions:

```yaml
load_from:
  - python_package:
      package_name: dags
```

All job definitions are in the `dags` package.

### dbt Assets (if available)

**Purpose**: Materializes dbt models as Dagster assets.

**Location**: `dagster/dags/__init__.py`

**Behavior**:

- Checks for dbt project at `/dbt`
- Runs `dbt parse` if manifest is missing
- Creates asset for each dbt model
- Executes `dbt build` on materialization

**Asset Structure**:

```python
@dbt_assets(manifest=manifest_path)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

## Standalone Jobs

### Amundsen Metadata Ingestion

**Purpose**: Populates Amundsen with PostgreSQL metadata.

**Location**: `dagster/jobs/amundsen_postgres_ingest.py`

**Execution**: Manual run (not integrated with Dagster scheduler)

```bash
python /jobs/amundsen_postgres_ingest.py
```

