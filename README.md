## Repository Layout

```text
databricks.yml                         # Bundle targets and environment variables
resources/                              # Databricks pipeline definitions
src/databricks_telemetry_etl/
    transformations/
        bronze.py                           # Incremental raw JSON ingestion
        silver.py                           # Regional split and cleansing
        gold.py                             # Dimensions and aggregated regional facts
src/raw_telemetry_json_generator/
    json_generator.py                     # Manual telemetry landing-data simulator
pyproject.toml                          # Python project and development dependencies
.gitignore                              # Local and generated-file exclusions
```

## Environment Separation

The same code is deployed to separate catalogs:

```text
Development:
    Catalog: dtbrtelemetrydev
    Schema:  the current developer schema
    Raw:     /Volumes/dtbrtelemetrydev/raw/telemetry_json/incoming

Production:
    Catalog: dtbrtelemetryprod
    Schemas: bronze, silver, gold
    Raw:     /Volumes/dtbrtelemetryprod/raw/telemetry_json/incoming
```

Environment-specific values are supplied by bundle target configuration rather than hardcoded in transformation logic.

## Local Prerequisites

- Python 3.10, 3.11, or 3.12
- Databricks CLI
- Access to the development and production Databricks workspaces
- Unity Catalog permissions for the configured catalogs, schemas, and volumes
- `uv` for local dependency management

Install development dependencies:

```powershell
uv sync --dev
```

## Validate and Deploy

Authenticate the Databricks CLI using your organization-approved authentication method, then validate before deploying:

```powershell
databricks bundle validate --target dev
*  Run `uv sync --dev` to install the project's dependencies.
```

Production promotion follows this sequence:

```text
dev branch -> test in development workspace -> pull request -> main -> validate prod -> deploy prod
```

From an up-to-date local `main` branch:

```powershell


# Using this project using the CLI
```

Do not deploy the `dev` branch with the `prod` target.

## Testing

Run local Python tests with:

```powershell
uv run pytest
```

The generator requires Databricks runtime objects such as `dbutils` and Unity Catalog volumes, so it is executed in the development Databricks workspace rather than as a plain local Python process.

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a pipeline called
    `[dev yourname] databricks_telemetry_etl` to your workspace.
    You can find that resource by opening your workpace and clicking on **Jobs & Pipelines**.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```
   Note the default template has a includes a job that runs the pipeline every day
   (defined in resources/sample_job.job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
