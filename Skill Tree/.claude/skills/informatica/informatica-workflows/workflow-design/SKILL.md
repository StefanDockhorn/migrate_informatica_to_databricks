---
name: informatica-workflow-design
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter workflow designs. Covers tasks, links, events, worklets, decision tasks, and workflow variables. Includes Databricks Workflows job orchestration equivalents. Do NOT use for general workflow patterns or non-Informatica orchestration."
---

# Informatica PowerCenter Workflow Design

## Purpose

Workflows define the order of task execution in PowerCenter. A workflow is a graphical orchestration of tasks connected by links that specify execution sequence and conditional branching.

## Task Types

| Task | Purpose |
|------|---------|
| **Session** | Executes a mapping; the primary data integration task |
| **Command** | Runs an OS command or shell script on the Integration Service node |
| **Decision** | Evaluates a Boolean expression to determine workflow branch |
| **Email** | Sends email notifications on success, failure, or condition |
| **Event Wait** | Blocks execution until an event file appears or an event is raised |
| **Event Raise** | Signals an event that another workflow's Event Wait task is listening for |
| **Timer** | Introduces a delay (absolute or relative) before the next task runs |
| **Worklet** | Embeds a reusable collection of tasks as a sub-workflow |
| **Assignment** | Sets workflow variable values used by downstream tasks |
| **Control** | Stops or aborts the workflow or a target task |

## Links

Links connect tasks and define execution flow. There are three types:

| Link Type | Visual | Behavior |
|-----------|--------|----------|
| **Unconditional** | Solid line | Task always runs after the previous task completes |
| **Conditional** | Dotted line with expression | Task runs only if the expression evaluates to true |
| **Evaluation** | Task-specific outcome | Triggered by success (`S`), failure (`F`), or completion (`C`) |

Always use conditional links for error handling paths. A session task should have at least two outgoing links: one on success and one on failure that routes to an email task or error handler.

## Worklets

A worklet is a reusable collection of tasks within a workflow. It has its own variables, scheduling context, and task logic.

- Worklets enable modular workflow design -- reuse common task sequences across multiple workflows
- A worklet can contain any task type except another worklet (no nesting)
- Worklet variables are scoped to the worklet; use parent workflow variables for cross-boundary communication
- Changes to a reusable worklet propagate to all workflows that reference it

```
Workflow: WF_MASTER
  |-- Start
  |-- Worklet: WL_COMMON_EXTRACT (reusable, shared across 5 workflows)
  |   |-- Session: S_Extract_Source1
  |   |-- Session: S_Extract_Source2
  |-- Session: S_Transform
```

## Decision Tasks

Decision tasks perform conditional branching based on Boolean expressions.

- Decision tasks use workflow variables set by Assignment tasks or session results
- Expression syntax: `($$RowCount > 0 AND $$Status = 'SUCCESS') ? 1 : 0`
- Link condition `1` = true (follow this path); `0` = false (do not follow)
- Decision tasks have no data movement overhead -- they only evaluate expressions

```
Decision: DEC_Check_Load
Expression: ($$SessRowCount > 0)
  |-- Link Condition: 1 -> Session: S_Load_Facts
  |-- Link Condition: 0 -> Email: EM_NoDataAlert
```

## Events

Events enable event-driven workflow orchestration across workflows.

| Task | Behavior |
|------|----------|
| **Event-Wait** | Blocks until an event file appears in a watched directory, or until an Event-Raise signals the named event |
| **Event-Raise** | Signals a named event that one or more Event-Wait tasks are listening for |

- Event-Wait with file watch enables file-triggered processing without external schedulers
- Event-Raise and Event-Wait pairs decouple workflow dependencies -- Workflow A raises an event; Workflow B waits for it
- Event names are defined in the workflow properties and referenced by both tasks

## Workflow Variables

Workflow variables store state and pass values between tasks.

| Variable | Scope | Purpose |
|----------|-------|---------|
| `$$SessStartTime` | Session | Start time of the current session run |
| `$$SessEndTime` | Session | End time of the current session run |
| `$$WorkflowName` | Workflow | Name of the current workflow |
| `$$VariableName` | User-defined | Custom variables for business logic and task communication |

## Built-in Variables

| Variable | Purpose |
|----------|---------|
| `$PMRootDir` | Integration Service root directory |
| `$PMSessionLogDir` | Directory for session log files |
| `$PMBadFileDir` | Directory for rejected row (.bad) files |
| `$PMTargetFileDir` | Directory for target flat files |
| `$PMSourceFileDir` | Directory for source flat files |
| `$PMConnectionName` | Default database connection name |

## Behavior Rules

- **Always use conditional links for error handling paths** -- every session task must have a failure branch
- Worklets enable modular workflow design -- reuse common task sequences across workflows
- Decision tasks use workflow variables set by Assignment tasks or session results
- Timer tasks can introduce delays between dependent workflows (e.g., wait 10 minutes for source system commit)
- Event-Wait with file watch enables file-triggered processing without external schedulers
- Assignment tasks must run before the variables they set are consumed by downstream tasks
- Control tasks stop the workflow immediately -- use sparingly for critical failure scenarios

## When NOT to Use This Skill

- General workflow patterns or state machine design (not Informatica-specific)
- Non-Informatica orchestration tools (Airflow DAGs, Azure Data Factory pipelines, AWS Step Functions)
- Data pipeline architecture without Informatica context
- Mapping or transformation design (use `informatica-mapping-design`)

## Spark / Databricks Equivalent

| Informatica Concept | Databricks Equivalent |
|---------------------|----------------------|
| Workflow | Databricks Workflow (Jobs UI / multi-task job) |
| Session task | Notebook task, JAR task, or SQL task |
| Decision task | `if` condition in a notebook or pipeline branch |
| Link (dependency) | Task dependency in Jobs UI |
| Event-Wait / Event-Raise | `dbutils.notebook.run()` with status checks, or Jobs API callbacks |
| Worklet | Nested workflow or shared job definition |
| Assignment task | `dbutils.widgets` or Spark config variables |
| Timer task | `time.sleep()` or scheduled job offset |

## Example: Daily Load Workflow

```
Workflow: WF_DAILY_LOAD
  |-- Start
  |-- Session: S_Load_Dimensions
  |   |-- Link (Success) --> Session: S_Decision_Check
  |   |-- Link (Failure) --> Email: EM_LoadFailed
  |
  |-- Decision: DEC_RowsExist
  |   |-- Expression: ($$RowCount > 0)
  |   |-- Link (1) --> Session: S_Load_Facts
  |   |-- Link (0) --> Email: EM_NoDataAlert
  |
  |-- Session: S_Load_Facts
  |   |-- Link (Success) --> Email: EM_SuccessNotify
  |   |-- Link (Failure) --> Email: EM_FactLoadFailed
  |
  |-- Email: EM_SuccessNotify
  |   Subject: "Daily load completed successfully"
  |
  |-- Email: EM_LoadFailed
  |   Subject: "Dimension load failed -- check session logs"
  |
  |-- Email: EM_NoDataAlert
  |   Subject: "No dimension rows loaded -- skipping fact load"
  |
  |-- Email: EM_FactLoadFailed
  |   Subject: "Fact load failed -- data may be incomplete"
```

## Example: Event-Driven File Processing

```
Workflow: WF_FILE_PROCESSOR
  |-- Start
  |-- Event-Wait: EW_NewFile
  |   |-- Type: File Watch
  |   |-- File: /data/incoming/customer_*.csv
  |   |-- Link (Success) --> Session: S_Load_Customers
  |
  |-- Session: S_Load_Customers
  |   |-- Link (Success) --> Command: CMD_ArchiveFile
  |   |-- Link (Failure) --> Email: EM_LoadError
  |
  |-- Command: CMD_ArchiveFile
  |   |-- Command: mv $PMSourceFileDir/customer_*.csv /data/archive/
  |   |-- Link (Success) --> Event-Raise: ER_Done
  |
  |-- Event-Raise: ER_Done
  |   |-- Event Name: CUSTOMER_LOAD_COMPLETE
  |
  |-- Email: EM_LoadError
  |   Subject: "Customer file load failed"
```
