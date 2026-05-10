---
name: informatica-scheduling
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter scheduling configurations. Covers time-based, event-based, and file-watch triggers. Includes Databricks Jobs scheduling and Auto Loader equivalents. Do NOT use for general cron or scheduling patterns."
---

# Informatica PowerCenter Scheduling

## Purpose

Schedulers determine when workflows start execution. PowerCenter provides built-in scheduling through the Integration Service, as well as integration points for external enterprise schedulers.

## Schedule Types

| Schedule Type | Behavior |
|---------------|----------|
| **Run On Demand** | Workflow runs only when manually started or triggered by `pmcmd` |
| **Run Continuously** | Workflow restarts immediately after completion; use with care |
| **Run On Server Initialization** | Workflow starts when the Integration Service starts |
| **Run At** | Runs once at a specific date and time |
| **Customized** | Repeats at a configurable interval (minutes, hours, days, weeks) |

## Customized Repeat Options

```
Repeat: Every N [minutes | hours | days | weeks]
Start: YYYY-MM-DD HH24:MI:SS
End:   YYYY-MM-DD HH24:MI:SS  (or "Never", or "After N occurrences")
```

- Customized repeat is the most common production pattern for recurring ETL jobs
- End date prevents runaway workflows when the integration window closes
- Use weekly repeat with day-of-week selection for standard business-day schedules

## Event-Based Scheduling

| Trigger Type | Behavior |
|--------------|----------|
| **File-Watch** | Polls a directory at configurable intervals; starts workflow when matching file appears |
| **Event-Wait** | Workflow starts immediately but blocks at Event-Wait task until event is raised |
| **Event-Raise (cross-workflow)** | Workflow A raises an event; Workflow B's Event-Wait unblocks |

### File-Watch Configuration

```
File Watch Directory: /data/incoming/
File Pattern: customer_*.csv
Poll Interval: 60 seconds
```

- File-watch scheduling polls the directory at configurable intervals -- shorter intervals increase I/O overhead
- Use file stabilization time to avoid processing incomplete files (files still being written)
- File patterns support wildcards (`*.csv`, `prefix_*_YYYYMMDD.dat`)

## Workflow Scheduler Service

The Integration Service component manages scheduling:

- Scheduler runs as a thread within the Integration Service process
- Schedule persistence survives Integration Service restarts
- Workflow must be valid and not already running for schedule to trigger
- Schedules can be suspended without stopping the workflow definition

## External Scheduler Integration

For complex dependency chains, use `pmcmd` with enterprise schedulers:

```bash
# Start workflow from Control-M / Tidal / Autosys
pmcmd startworkflow -sv INFA_SVC -d INFA_DOMAIN -u user -p pass -f folder WF_DAILY_LOAD

# Wait for completion and capture return code
pmcmd startworkflow -sv INFA_SVC -d INFA_DOMAIN -u user -p pass -w -f folder WF_DAILY_LOAD
echo $?  # 0 = success, non-zero = failure
```

| Enterprise Scheduler | Integration Method |
|----------------------|-------------------|
| **Control-M** | `pmcmd` via job definition; return code drives downstream jobs |
| **Tidal** | Native Informatica adapter or `pmcmd` command |
| **Autosys** | `pmcmd` in command job; exit code mapped to success/failure |
| **Cron** | Direct `pmcmd` invocation with environment setup |

- Use `pmcmd` with external schedulers for complex dependency chains -- PowerCenter scheduler does not support cross-workflow dependencies natively
- `pmcmd getworkflowdetails` can poll workflow status for external orchestration

## Behavior Rules

- File-watch scheduling polls the directory at configurable intervals -- balance responsiveness against I/O overhead
- Run Continuously restarts workflow immediately after completion; ensure the workflow includes a delay or exit condition to avoid tight loops
- Run On Server Initialization starts workflow when Integration Service starts; use for always-on listeners
- Use `pmcmd` with external schedulers for complex dependency chains
- Schedule can be suspended without stopping the workflow -- useful for maintenance windows
- Workflows with active schedules must be checked in to the repository to trigger
- Timezone awareness: scheduler uses Integration Service host timezone unless overridden

## When NOT to Use This Skill

- General cron expression syntax or Linux cron questions
- Non-Informatica scheduling (Airflow schedules, Databricks cron without migration context)
- Windows Task Scheduler configuration
- Real-time streaming architecture (Informatica is batch-oriented)

## Spark / Databricks Equivalent

| Informatica Concept | Databricks Equivalent |
|---------------------|----------------------|
| Run At / Customized | Jobs UI cron schedule or API (`/api/2.1/jobs/create`) |
| File-Watch trigger | Auto Loader (`cloudFiles` format) for incremental file ingestion |
| Event-Wait / Event-Raise | `dbutils.notebook.run()` with status checks; Jobs API event triggers |
| Run Continuously | Streaming job with `Trigger.ProcessingTime` |
| `pmcmd` integration | Databricks Jobs API (`runs/submit`, `runs/get`) |
| External scheduler integration | Jobs API + enterprise scheduler HTTP tasks |

## Example: Daily Production Schedule

```
Workflow: WF_DAILY_LOAD
Schedule: Customized
  Start: 2024-01-01 02:00:00
  Repeat: Every 1 day
  End: Never
  Days: Mon, Tue, Wed, Thu, Fri, Sat, Sun

File Watch (alternative trigger):
  Directory: /data/landing/daily/
  Pattern: extract_*.ready
  Poll Interval: 60 seconds
  Stabilization Time: 30 seconds
```

## Example: External Scheduler Integration (Control-M)

```bash
#!/bin/bash
# Control-M job definition for Informatica workflow

INFA_HOME=/opt/informatica/10.5.1/server/bin
DOMAIN_NAME=PROD_DOMAIN
SERVICE_NAME=INT_SVC_PROD
FOLDER=PROD_FOLDER
WORKFLOW=WF_DAILY_LOAD
USER=ctm_user
PASS=$(cat /secure/ctm_pass.txt)

${INFA_HOME}/pmcmd startworkflow \
  -sv ${SERVICE_NAME} \
  -d ${DOMAIN_NAME} \
  -u ${USER} \
  -p ${PASS} \
  -f ${FOLDER} \
  -wait ${WORKFLOW}

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo "Workflow ${WORKFLOW} failed with exit code ${EXIT_CODE}"
  exit 1
fi

echo "Workflow ${WORKFLOW} completed successfully"
exit 0
```
