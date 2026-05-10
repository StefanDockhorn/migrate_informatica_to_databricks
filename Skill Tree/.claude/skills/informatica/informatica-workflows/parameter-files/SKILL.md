---
name: informatica-parameter-files
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter parameter files. Covers mapping parameters, mapping variables, workflow variables, $PM service variables, and parameter substitution. Includes Databricks job parameters and Spark configuration equivalents. Do NOT use for general configuration management."
---

# Informatica PowerCenter Parameter Files

## Purpose

Parameter files externalize configuration values for reuse across sessions and workflows. They enable environment-specific behavior (DEV / QA / PROD) without modifying session or mapping definitions.

## Parameter Types

| Type | Prefix | Scope | Mutability |
|------|--------|-------|------------|
| **Mapping Parameters** | `$$ParamName` | Single session | Immutable during session |
| **Mapping Variables** | `$$VarName` | Single session | Mutable; last value persisted to repository |
| **Workflow Variables** | `$$WFVarName` | Workflow | Set via Assignment task or session results |
| **Service Variables** | `$PMVariable` | Integration Service | System-level configuration |

### Mapping Parameters (`$$ParamName`)

- Constant values set at session start; cannot change during session execution
- Use for environment-specific values: database names, schema names, batch dates
- Referenced in Source Qualifier SQL, Expression transformations, and session overrides

### Mapping Variables (`$$VarName`)

- Persisted values that can change during session execution
- Last value saved to the PowerCenter repository for the next run
- Primary use case: incremental extraction (`$$LAST_EXTRACT_DATE` updated each run)
- Aggregator and Expression transformations can set variable values

### Workflow Variables (`$$WFVarName`)

- Scope is within the workflow; set via Assignment task or session results
- Pass values between tasks: Session A sets `$$RowCount`; Decision task reads `$$RowCount`
- Not persisted to repository unless explicitly configured

## File Format

Parameter files are plain text with section headers:

```ini
[Global]
$$DatabaseName=PROD_DB
$$BatchDate=$ $SessStartTime
$PMSessionLogDir=/logs/informatica/prod

[Session:S_Load_EMP]
$$LAST_EXTRACT_DATE='2024-01-01'
$PMConnectionName=ORA_PROD

[Session:S_Load_DEPT]
$$LAST_EXTRACT_DATE='2024-01-01'
$PMConnectionName=ORA_PROD

[Workflow:WF_DAILY_LOAD]
$$EmailRecipient=dba-team@company.com
$$NotificationEnabled=YES
```

| Section | Scope |
|---------|-------|
| `[Global]` | All sessions and workflows |
| `[Session:session_name]` | Specific session only |
| `[Workflow:wf_name]` | Specific workflow only |

Section order matters: Session-specific values override Global values.

## $PM Service Variables

| Variable | Purpose |
|----------|---------|
| `$PMRootDir` | Integration Service installation root |
| `$PMSessionLogDir` | Session log file directory |
| `$PMConnectionName` | Default database connection (can be overridden per session) |
| `$PMSessionErrorThreshold` | Max errors before session stops |
| `$PMSourceFileDir` | Source flat file directory |
| `$PMTargetFileDir` | Target flat file directory |
| `$PMBadFileDir` | Rejected rows (.bad) file directory |
| `$PMCacheDir` | Lookup and aggregator cache directory |
| `$PMTempDir` | Temporary file directory |
| `$PMSessionName` | Current session name (read-only) |

## Pre-defined Session Variables

| Variable | Purpose |
|----------|---------|
| `$ $SessStartTime` | Session start timestamp (format: `YYYY-MM-DD HH24:MI:SS`) |
| `$ $SessEndTime` | Session end timestamp (format: `YYYY-MM-DD HH24:MI:SS`) |

Use `$ $SessStartTime` in SQL overrides for incremental extraction with consistent timestamp:
```sql
SELECT * FROM ORDERS WHERE LAST_MODIFIED > TO_DATE('$ $SessStartTime', 'YYYY-MM-DD HH24:MI:SS')
```

## Behavior Rules

- Mapping parameters referenced as `$$ParamName` in expressions and SQL overrides
- Parameter file paths specified in session config or `pmcmd` command:
  ```bash
  pmcmd startworkflow ... -paramfile /path/to/prod.params WF_DAILY_LOAD
  ```
- Use mapping variables for incremental extraction -- `$$LAST_EXTRACT_DATE` updated each run via Expression transformation
- Workflow variables pass values between tasks via Assignment task
- `$PMConnectionName` changes database connection dynamically -- use with environment-specific parameter files
- Parameter values are resolved at session start; changes require session restart to take effect
- Use single quotes around date string values to prevent Informatica date parsing errors
- Parameter file paths can use `$PMRootDir` for portability: `$PMRootDir/params/prod.params`

## When NOT to Use This Skill

- General configuration management (Ansible, Chef, Puppet)
- Environment variable management in operating systems
- Non-Informatica Spark configuration (`spark-defaults.conf`)
- Secret management (passwords should not be stored in plaintext parameter files -- use a credential vault)

## Spark / Databricks Equivalent

| Informatica Concept | Databricks Equivalent |
|---------------------|----------------------|
| Parameter file | `dbutils.widgets.text("param", "default")` |
| `$$ParamName` | Widget parameters or job parameters |
| `$$VarName` (persisted) | Delta table for state persistence; `spark.read`/`spark.write` |
| `$ $SessStartTime` | `current_timestamp()` in Spark SQL |
| `$PMConnectionName` | `spark.conf.set("spark.databricks.jdbc.url", url)` |
| `[Global]` section | Cluster-level Spark config |
| `[Session:]` section | Task-level parameters in Jobs UI |
| `pmcmd -paramfile` | Jobs API `notebook_params` in `runs/submit` |
| Mapping variable persistence | Managed Delta table with `MERGE INTO` for state tracking |

## Example: Environment-Specific Parameter Files

### Development

```ini
[Global]
$$DatabaseName=DEV_DB
$$SchemaName=ETL_DEV
$$BatchDate=$ $SessStartTime
$PMConnectionName=ORA_DEV
$PMSessionLogDir=/logs/informatica/dev
$PMBadFileDir=/data/bad/dev

[Session:S_Load_EMP]
$$LAST_EXTRACT_DATE='1900-01-01'
$$MaxRows=1000

[Session:S_Load_ORDERS]
$$LAST_EXTRACT_DATE='1900-01-01'
```

### Production

```ini
[Global]
$$DatabaseName=PROD_DW
$$SchemaName=ETL_PROD
$$BatchDate=$ $SessStartTime
$PMConnectionName=ORA_PROD
$PMSessionLogDir=/logs/informatica/prod
$PMBadFileDir=/data/bad/prod

[Session:S_Load_EMP]
$$LAST_EXTRACT_DATE='2024-01-01'
$$MaxRows=0

[Session:S_Load_ORDERS]
$$LAST_EXTRACT_DATE='2024-01-01'
```

## Example: Incremental Load with Mapping Variable

```
Mapping: M_INCREMENTAL_ORDERS
  Source Qualifier SQL:
    SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, AMOUNT
    FROM ORDERS
    WHERE ORDER_DATE > $$LAST_EXTRACT_DATE

  Expression: EXP_SetVariable
    Variable Port: $$LAST_EXTRACT_DATE = MAX(ORDER_DATE)
    (Configured as Mapping Variable, aggregation type: MAX)

Parameter File:
[Session:S_Load_ORDERS]
$$LAST_EXTRACT_DATE='2024-01-01'

Result: After each run, $$LAST_EXTRACT_DATE is updated to the latest ORDER_DATE
        processed, so the next run only fetches new records.
```
