---
name: informatica-error-handling
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter error handling configurations. Covers fatal vs non-fatal errors, error logging, recovery strategy, and session recovery. Includes Databricks job retry and Spark write error handling equivalents. Do NOT use for general application error handling."
---

# Informatica PowerCenter Error Handling

## Purpose

Error handling defines how sessions respond to data and system errors. Proper configuration prevents silent data corruption, bounds recovery time, and enables reprocessing of failed data.

## Error Types

| Error Type | Behavior | Examples |
|------------|----------|----------|
| **Fatal errors** | Stop session immediately | Repository connection lost, Integration Service failure, mapping invalid |
| **Non-fatal errors** | Log and continue (up to threshold) | Data conversion error, lookup miss (configured), expression evaluation error |
| **Data errors** | Transformation-level failures | String-to-number conversion failure, date format mismatch, truncation |
| **Writer errors** | Target database constraint violations | Primary key violation, foreign key constraint failure, NOT NULL constraint |
| **Reader errors** | Source data access failures | Source database down, missing source file, network timeout |

## Error Threshold

The error threshold controls when a session stops due to non-fatal errors:

| Setting | Behavior |
|---------|----------|
| **Stop on Errors: N** | Session stops after N non-fatal errors |
| **Stop on Errors: 0** | Session continues regardless of error count |
| **Stop on Errors: 1** | Session stops on first non-fatal error (fail-fast) |

- Always set an error threshold for production sessions -- prevent infinite error loops that waste resources and produce incomplete loads
- A threshold of 1-10 is typical for production; 0 is acceptable only for best-effort loads with downstream reconciliation

## Error Log

PowerCenter can write rejected rows and error context to relational error log tables:

| Table | Content |
|-------|---------|
| **PMERR_DATA** | Rejected row data (source row values) |
| **PMERR_MSG** | Error message and error code |
| **PMERR_TRANS** | Transformation name, session name, timestamp |

- Row error logging adds overhead; disable for high-volume performance sessions
- Error log tables must be created in a database accessible to the Integration Service
- Use error logs for debugging data quality issues; not a substitute for data quality monitoring

## Bad File

Rejected rows are written to a `.bad` file for later analysis and reprocessing:

```
Bad File: $PMBadFileDir/EMP_$PMSessionName.bad
```

- Bad files enable reprocessing of rejected data after fixing source issues
- One bad file per target definition in the session
- Format matches the target flat file structure (for flat file targets) or is a delimited text representation (for relational targets)
- Clean bad files regularly to prevent disk space exhaustion on the Integration Service node

## Recovery Strategy

Recovery determines how a session resumes after failure:

| Strategy | Behavior |
|----------|----------|
| **Fail task and continue** | Failed task stops; workflow continues with remaining tasks |
| **Fail task and continue workflow** | Same as above; explicit naming for workflow-level recovery |
| **Resume from last checkpoint** | Session resumes from last committed checkpoint; requires recovery configuration |

### Session Recovery

- Suspended sessions can be resumed; requires recovery strategy configured
- Recovery requires source to support recovery -- relational sources with primary keys or change data capture
- Flat file sources do not support recovery (no checkpoint concept for files)
- Recovery information is stored in the PowerCenter repository
- Resume from checkpoint avoids reprocessing already-committed target rows

## Pre-Session and Post-Session Error Handling

| Phase | Error Behavior |
|-------|---------------|
| **Pre-Session** | On error, always stop session -- pre-session tasks are prerequisites |
| **Post-Session** | On error, can continue or stop based on configuration |

- Pre-session errors always stop session -- the session data pipeline cannot start if prerequisites fail
- Post-session errors can be configured to continue to avoid blocking downstream workflows

## Behavior Rules

- Always set an error threshold for production sessions (prevent infinite error loops)
- Bad files enable reprocessing of rejected data after fixing source issues
- Recovery requires source to support recovery (relational sources with primary keys)
- Row error logging adds overhead; disable for high-volume performance sessions
- Pre-session errors always stop session -- pre-session tasks are prerequisites for data movement
- Post-session errors can continue to avoid blocking downstream workflows
- Set bad file naming to include `$PMSessionName` and timestamp for unique identification
- Monitor bad file directory disk space -- rejected rows can accumulate rapidly with bad source data

## When NOT to Use This Skill

- General application error handling (try/catch in Python/Java)
- Database transaction management (COMMIT/ROLLBACK patterns)
- Data quality framework design (Great Expectations, dbt tests)
- Monitoring and alerting infrastructure (PagerDuty, Datadog)

## Spark / Databricks Equivalent

| Informatica Concept | Spark / Databricks Equivalent |
|---------------------|------------------------------|
| Error threshold | `spark.sql.files.maxRecordsPerFile` (indirect); `badRecordsPath` |
| Bad file | `.option("badRecordsPath", "/path/to/bad")` in DataFrameWriter |
| Error log tables | Delta table with `_rescued_data` column; custom error table |
| Fatal error | Spark stage failure; cluster termination |
| Non-fatal error | `PERMISSIVE` mode in `spark.read` with `_corrupt_record` column |
| Recovery (checkpoint) | Delta Lake time travel; structured streaming checkpoints |
| Pre-session error | Notebook cell failure before write operation |
| Post-session error | Notebook cell failure after write (can be caught with try/finally) |
| Resume from checkpoint | Structured streaming `checkpointLocation` |
| Session retry on failure | Databricks Jobs retry policy (`max_retries`, `min_retry_interval_millis`) |

## Example: Production Error Handling Configuration

```
Session: S_Load_EMP
- Stop on Errors: 10
- Error Log Type: Relational
  Connection: ORA_LOG
  Tables: PMERR_DATA, PMERR_MSG, PMERR_TRANS
- Bad File: $PMBadFileDir/EMP_$PMSessionName.bad
- Recovery Strategy: Resume from last checkpoint
- Pre-Session Command:
    On error: Stop
    Command: /scripts/validate_source.sh EMP
- Post-Session Command:
    On error: Continue
    Command: /scripts/notify_completion.sh EMP
- Session Log:
    Log File: $PMSessionLogDir/EMP_$PMSessionName.log
    Tracing: Normal
```

## Example: Error Handling Workflow Pattern

```
Workflow: WF_LOAD_WITH_RECOVERY
  |-- Start
  |-- Session: S_Load_Dimensions
  |   |-- Link (Success) --> Session: S_Load_Facts
  |   |-- Link (Failure) --> Command: CMD_LogError
  |
  |-- Session: S_Load_Facts
  |   |-- Link (Success) --> Email: EM_Success
  |   |-- Link (Failure) --> Command: CMD_LogError
  |   |-- Link (Failure) --> Session: S_Run_Recovery  (on error threshold exceeded)
  |
  |-- Command: CMD_LogError
  |   |-- Command: /scripts/log_error.sh "$PMFolderName" "$PMSessionName" "$PMWorkflowName"
  |   |-- Link (Always) --> Email: EM_ErrorAlert
  |
  |-- Email: EM_ErrorAlert
  |   |-- To: oncall@company.com
  |   |-- Subject: "FAILURE: $PMWorkflowName - $PMSessionName"
  |
  |-- Email: EM_Success
  |   |-- To: etl-team@company.com
  |   |-- Subject: "SUCCESS: $PMWorkflowName completed"
  |
  |-- Session: S_Run_Recovery
  |   |-- Mapping: M_REPROCESS_BAD_ROWS
  |   |-- Source: $PMBadFileDir/EMP_*.bad (flat file)
  |   |-- Target: ORA_TGT (with relaxed constraints)
```

## Error Handling Checklist

| Check | Configuration |
|-------|--------------|
| Error threshold set | `Stop on Errors: 10` (or appropriate for data volume) |
| Bad file configured | `$PMBadFileDir/<TABLE>_<SESSION>.bad` |
| Bad file directory monitored | Disk space alert on Integration Service node |
| Error log tables accessible | `PMERR_*` tables in reachable database |
| Recovery configured | Resume from checkpoint (for relational sources) |
| Pre-session error handling | Set to Stop (prerequisites must succeed) |
| Post-session error handling | Set to Continue (avoid blocking downstream) |
| Retry on failure | Set to 3 retries for transient failures |
| Retry on deadlock | Set to 5 retries for database deadlocks |
