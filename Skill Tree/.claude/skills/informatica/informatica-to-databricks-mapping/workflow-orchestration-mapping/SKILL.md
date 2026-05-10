---
name: informatica-to-databricks-workflow-mapping
description: "Use when mapping Informatica PowerCenter workflows to Databricks orchestration. Covers task types, dependencies, scheduling, parameter passing, and event handling patterns. Do NOT use for general workflow design, transformation mapping, or performance tuning."
---

# Workflow Orchestration Mapping: Informatica → Databricks

## Workflow Task Mapping

| Informatica Task | Databricks Equivalent | Notes |
|---|---|---|
| Workflow | Databricks Workflow / Job | Multi-task job with DAG dependencies |
| Session | Notebook task / Python task / JAR task | One task per session. Python task for PySpark jobs. |
| Command Task | `%sh` magic in notebook / Shell task | Or `os.system()` / `subprocess` in Python task |
| Decision Task | `IF/ELSE` condition task | Databricks Workflows supports `IF/ELSE` conditions |
| Email Task | Notification destination | Configure in job settings or use task-level notifications |
| Event Wait | File arrival trigger / Auto Loader | Or external event-based trigger via REST API |
| Event Raise | Job completion trigger | Trigger downstream jobs via `run-now` or webhooks |
| Timer Task | Not directly supported | Use `time.sleep()` in notebook, or schedule offset in cron |
| Worklet | Sub-workflow / nested workflow | Databricks supports calling other jobs as tasks |
| Assignment Task | Widget / `dbutils.widgets` | Or job parameters passed via Jobs API |
| Control Task | Not directly supported | Use conditional task execution (`IF/ELSE`) |
| Session (on failure) | Retry policy / timeout | Configure `max_retries`, `timeout_seconds` on task |
| Link (success) | `on_success` dependency | Default task dependency behavior |
| Link (failure) | `on_failure` dependency | Route to recovery/cleanup task |
| Link (expression) | Conditional dependency | Evaluate expression before task run |
| Scheduler | Job schedule (cron expression) | Quartz cron format: `0 30 5 * * ?` |
| File Watch | Auto Loader / `cloudFiles` | Recommended for incremental file ingestion |
| `pmcmd startworkflow` | `databricks jobs run-now` | Or `POST /api/2.1/jobs/run-now` via REST API |
| `pmcmd getsessionlog` | Task run output / driver logs | Query via `GET /api/2.1/jobs/runs/get-output` |

---

## Dependency Patterns

### Linear Chain

```yaml
# Informatica:
# Session1 → Session2 → Session3 (on success)

# Databricks Workflow:
tasks:
  - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_1"
  - task_key: "session_2"
    depends_on:
      - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_2"
  - task_key: "session_3"
    depends_on:
      - task_key: "session_2"
    notebook_task:
      notebook_path: "/Shared/jobs/session_3"

# WHY: Each depends_on creates an edge in the task DAG.
# Only on_success is needed for linear chains (default behavior).
```

### Fan-Out (Router → Multiple Parallel Sessions)

```yaml
# Informatica:
# Session1 → [Session2a, Session2b, Session2c] (all on success from Session1)

# Databricks Workflow:
tasks:
  - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_1"
  - task_key: "session_2a"
    depends_on:
      - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_2a"
  - task_key: "session_2b"
    depends_on:
      - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_2b"
  - task_key: "session_2c"
    depends_on:
      - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_2c"

# WHY: Parallel tasks with the same dependency run concurrently.
# No explicit parallel construct needed.
```

### Fan-In (Join → Single Session)

```yaml
# Informatica:
# [Session1a, Session1b, Session1c] → Session2 (all must succeed)

# Databricks Workflow:
tasks:
  - task_key: "session_1a"
    notebook_task:
      notebook_path: "/Shared/jobs/session_1a"
  - task_key: "session_1b"
    notebook_task:
      notebook_path: "/Shared/jobs/session_1b"
  - task_key: "session_1c"
    notebook_task:
      notebook_path: "/Shared/jobs/session_1c"
  - task_key: "session_2"
    depends_on:
      - task_key: "session_1a"
      - task_key: "session_1b"
      - task_key: "session_1c"
    notebook_task:
      notebook_path: "/Shared/jobs/session_2"

# WHY: Task with multiple depends_on waits for ALL dependencies.
# This is the Informatica "join" equivalent.
```

### Conditional Branch (Decision Task)

```python
# Informatica:
# Session1 → Decision ($$ROW_COUNT > 0) → [Session2 on true, Session3 on false]

# Databricks Workflow (IF/ELSE condition):
tasks:
  - task_key: "session_1"
    notebook_task:
      notebook_path: "/Shared/jobs/session_1"
  - task_key: "check_row_count"
    depends_on:
      - task_key: "session_1"
    condition_task:
      expression: "{{tasks.session_1.output.row_count}} > 0"
      if_true:
        task_key: "session_2"
      if_false:
        task_key: "session_3"
  - task_key: "session_2"
    notebook_task:
      notebook_path: "/Shared/jobs/session_2"
  - task_key: "session_3"
    notebook_task:
      notebook_path: "/Shared/jobs/session_3"

# WHY: Condition tasks evaluate expressions using Jinja templating.
# Reference upstream task outputs for dynamic branching.
```

---

## Parameter Mapping

| Informatica Parameter | Databricks Equivalent | Access Pattern | Notes |
|---|---|---|---|
| `$$Parameter` | Job parameter / `dbutils.widgets.get("param")` | `dbutils.widgets.get("param")` | Define widgets at notebook start |
| `$$$SessStartTime` | `spark.conf.get("spark.databricks.job.startTime")` | Session-scoped config | Or capture `current_timestamp()` at notebook start |
| `$$WorkflowVariable` | Delta table for state / job parameters | Read/write Delta table | Use Delta for persistence across runs |
| `$PMConnectionName` | Spark connection options / Secrets | `dbutils.secrets.get("scope", "key")` | Store credentials in Databricks secrets |
| `$PMRootDir` | Volume / DBFS path | `/Volumes/catalog/schema/vol/` | Or DBFS: `/dbfs/mnt/data/` |
| `$PMSessionLogDir` | Task run logs / driver logs | Query via Jobs API | Centralized in Databricks UI |
| `$PMBadFileDir` | `/Volumes/.../bad_records/` | `df_bad.write.mode("append")` | Write rejected rows to separate location |
| `$$InputFile` | CloudFiles path / job parameter | `spark.read.format("cloudFiles")` | Use Auto Loader for incremental ingestion |
| `$$TargetTable` | Job parameter + `saveAsTable()` | `spark.table(param_table)` | Pass table name as parameter |

### Parameter Passing Example

```python
# Informatica:
# $$TARGET_SCHEMA = 'staging'
# $$TARGET_TABLE = 'customers'
# $$BATCH_DATE = '2024-01-15'

# Databricks (notebook header with widgets):
dbutils.widgets.text("TARGET_SCHEMA", "staging", "Target Schema")
dbutils.widgets.text("TARGET_TABLE", "customers", "Target Table")
dbutils.widgets.text("BATCH_DATE", "", "Batch Date (YYYY-MM-DD)")

target_schema = dbutils.widgets.get("TARGET_SCHEMA")
target_table = dbutils.widgets.get("TARGET_TABLE")
batch_date = dbutils.widgets.get("BATCH_DATE")

# Use parameters in code
spark.table(f"{target_schema}.{target_table}") \
  .filter(col("BATCH_DATE") == lit(batch_date))

# WHY: dbutils.widgets define parameters that can be passed from
# the Jobs API, making notebooks reusable across environments.
```

---

## Scheduling Patterns

| Informatica Schedule | Databricks Cron | Example |
|---|---|---|
| Daily at 6 AM | `0 0 6 * * ?` | `schedule: { quartz_cron_expression: "0 0 6 * * ?" }` |
| Every hour | `0 0 * * * ?` | Hourly on the hour |
| Every 15 minutes | `0 */15 * * * ?` | `0 0/15 * * * ?` also valid |
| Weekly on Sunday 3 AM | `0 0 3 ? * 1` | Sunday = 1 in Quartz |
| Last day of month | `0 0 22 L * ?` | At 10 PM on last day |
| Weekdays at 8 AM | `0 0 8 ? * 2-6` | Monday (2) to Friday (6) |

```yaml
# Databricks Workflow schedule definition:
schedule:
  quartz_cron_expression: "0 30 5 * * ?"
  timezone_id: "America/New_York"
  pause_status: "UNPAUSED"

# WHY: Quartz cron supports more expressions than standard cron.
# Use `?` for "no specific value" in day-of-month / day-of-week.
```

---

## File Watch / Event-Driven Patterns

### Auto Loader (cloudFiles) for File Arrival

```python
# Informatica:
# Event Wait → File Watch → Session (process new file)

# Databricks equivalent:
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/mnt/schema")
    .load("/mnt/incoming/")
)

df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/mnt/checkpoint") \
  .toTable("target.staging_table")

# WHY: Auto Loader replaces file watch + manual trigger.
# It handles schema inference, file listing optimization, and exactly-once processing.
```

---

## Worklet → Sub-Workflow

```yaml
# Informatica:
# Worklet "ETL_CORE" contains: Extract → Transform → Load
# Called from multiple parent workflows

# Databricks:
# Create a separate job for the worklet, reference it as a "job" task
tasks:
  - task_key: "run_etl_core"
    run_job_task:
      job_id: 123456789  # The worklet job ID

# WHY: Databricks has no "worklet" concept. Use job-task references
# to compose reusable sub-workflows. Pass parameters via job_parameters.
```

---

## Error Handling & Recovery

| Informatica Feature | Databricks Equivalent | Configuration |
|---|---|---|
| Session failure → email | Task-level notification | `email_notifications: { on_failure: ["ops@company.com"] }` |
| Stop on N errors | No direct equivalent | Spark fails fast; use try_cast + quarantine pattern |
| Bad file for rejected rows | Write to quarantine Delta table | `df_bad.write.mode("append").saveAsTable("quarantine.records")` |
| Session recovery (run from last) | Retry from failed task | `max_retries: 3` + idempotent writes |
| Workflow restart | Job re-run from failed task | Databricks preserves checkpoint for streaming |
| Suspension email | Notification on task pause | Configure in job notification settings |

---

## When This Skill Should NOT Fire

- Do NOT use for transformation-level mapping (see `informatica-to-databricks-transformation-mapping`).
- Do NOT use for function-level syntax mapping (see `informatica-to-databricks-function-mapping`).
- Do NOT use for performance optimization (see `informatica-to-databricks-performance-patterns`).
- Do NOT use for identifying anti-patterns (see `informatica-to-databricks-anti-patterns`).
- Do NOT use for general Databricks job creation without Informatica context.
