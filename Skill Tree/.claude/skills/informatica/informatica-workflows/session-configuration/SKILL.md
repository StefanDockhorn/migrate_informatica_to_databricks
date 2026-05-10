---
name: informatica-session-configuration
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter session configurations. Covers connection settings, partitioning, pushdown optimization, performance tuning, and session properties. Includes Databricks Spark configuration equivalents. Do NOT use for general database connection management."
---

# Informatica PowerCenter Session Configuration

## Purpose

Sessions are executable configurations of mappings. They define how, when, and where data moves from source to target, including connection settings, partitioning, performance tuning, and error handling.

## Components

| Component | Purpose |
|-----------|---------|
| **General** | Session name, associated mapping, and description |
| **Properties** | Session-level settings (retry, performance, logging) |
| **Config Object** | Resource configuration (buffer size, DTM threads) |
| **Mapping** | Source/target connections, SQL overrides, lookup policies |
| **Components** | Pre-session and post-session command tasks |

## Connection Settings

Every session defines connections for each pipeline endpoint:

| Connection Type | Purpose |
|-----------------|---------|
| **Source** | Database, flat file, XML, or application connection for reading |
| **Target** | Database or flat file connection for writing |
| **Lookup** | Connection used by Lookup transformations |
| **Stored Procedure** | Connection for pre/post-session or mid-session stored procedures |
| **Transaction Control** | Connection for transaction boundary logic |

Always verify connection object permissions in the target environment after migration. Connection names referenced in parameter files (`$PMConnectionName`) must match connection objects defined in the Workflow Manager.

## Partitions

Partitions enable parallel processing within a single session. Add partitions to increase throughput for large data volumes.

| Partition Type | Use Case |
|----------------|----------|
| **Pass-Through** | Default; single pipeline, no redistribution |
| **Round-Robin** | Evenly distribute rows across partitions for balanced load |
| **Hash Auto-Keys** | Hash on all transformation ports; automatic key selection |
| **Hash User Keys** | Hash on user-specified ports (e.g., `EMPNO`) |
| **Key Range** | Divide data by value ranges (e.g., `SALARY` 0-5000, 5000-10000) |

- Session partitions must evenly distribute data to avoid skew -- Hash User Keys on high-cardinality columns
- More partitions increase memory usage; scale DTMD buffer size proportionally
- Key Range requires source data to be well-distributed across the key column

```
Partitions: 4
Type: Hash User Keys
Key Ports: EMPNO
```

## Pushdown Optimization

Pushdown optimization offloads transformation logic to the source or target database, reducing data movement across the network.

| Pushdown Type | Behavior |
|---------------|----------|
| **Full Pushdown** | Entire mapping logic pushed to database; Integration Service only issues SQL |
| **Source-Side Pushdown** | Source Qualifier and early transformations pushed to source database |
| **Target-Side Pushdown** | Final transformations and target load pushed to target database |

- Pushdown optimization requires source/target support for transformation logic -- verify database version compatibility
- Full pushdown eliminates PowerCenter transformation engine overhead entirely
- Aggregator, Filter, and Expression transformations are most commonly pushdown-eligible
- Joiner pushdown requires the join to occur at the source database

```
Session: S_Load_EMP
- Pushdown Optimization: Full
- Pushdown Type: To Source (Oracle 19c)
```

## Session Properties

### General Properties

| Property | Purpose |
|----------|---------|
| **Session retry on failure** | Retry count when session fails (default: 0) |
| **Session retry on deadlock** | Retry count when database deadlock detected |
| **Override tracing** | Set session log detail level (Normal, Terse, Verbose, Verbose Data) |

### Performance Properties

| Property | Purpose |
|----------|---------|
| **DTMD buffer size** | Memory buffer for data transformation (default: 64000 bytes) |
| **Collect performance data** | Enable performance counters in session log |
| **Test data load** | Load first N rows only for testing |

### Target Properties

| Property | Behavior |
|----------|----------|
| **Target load type** | `Bulk` (faster, disables constraints/triggers) or `Normal` (slower, enforces constraints) |
| **Target update strategy** | `Update as Update`, `Update as Insert`, `Update else Insert` |

## Target Load Behavior

| Mode | Speed | Constraints | Triggers | Use Case |
|------|-------|-------------|----------|----------|
| **Bulk** | Fastest | Disabled | Disabled | Initial loads, large historical backfills |
| **Normal** | Slower | Enforced | Fired | Incremental loads, constraint-critical tables |

- Bulk load is faster but disables constraints and triggers; use for initial loads
- Normal load enables constraint enforcement; use for incremental loads
- Target update strategy `Update else Insert` performs an UPSERT -- attempts update, falls back to insert
- Override SQL in session for incremental extraction: `WHERE LAST_MODIFIED > '$ $SessStartTime'`

## Behavior Rules

- Always set override tracing to `Normal` in production; use `Verbose Data` only for debugging
- Bulk load is faster but disables constraints and triggers; use for initial loads
- Normal load enables constraint enforcement; use for incremental loads
- Pushdown optimization requires source/target support for transformation logic
- Session partitions must evenly distribute data to avoid skew -- choose hash keys with high cardinality
- Override SQL in session for incremental extraction (`WHERE LAST_MODIFIED > '$ $SessStartTime'`)
- Increase DTMD buffer size for wide rows or complex transformations; monitor memory usage
- Collect performance data during tuning phases; disable in production to reduce log overhead

## When NOT to Use This Skill

- General database connection management (connection pooling, JDBC tuning)
- Non-Informatica Spark or Databricks configuration questions
- SQL tuning without session context
- Mapping transformation design (use `informatica-mapping-design`)

## Spark / Databricks Equivalent

| Informatica Concept | Spark / Databricks Equivalent |
|---------------------|------------------------------|
| Session properties | `spark.conf.set("key", "value")` |
| Partitions | `df.repartition(N)` / `df.coalesce(N)` |
| Hash partition | `df.repartition("EMPNO")` |
| Key range partition | `bucketBy(numBuckets, "column")` |
| Pushdown optimization | Spark predicate pushdown (`spark.sql.optimizer.metadataOnly` = true) |
| SQL override | Filter pushdown in `spark.read.jdbc()` with `predicates` array |
| Bulk load | `spark.write.mode("overwrite").option("bulkLoad", true)` |
| Normal load with constraints | Delta Lake `CONSTRAINT` + `MERGE INTO` |
| Update else Insert | `MERGE INTO target USING source ON ... WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT ...` |
| Bad file | `.option("badRecordsPath", "/path/to/bad")` |

## Example: Production Session Configuration

```
Session: S_Load_EMP
- Mapping: M_EMP_TO_DW
- Source: ORACLE_SRC
  SQL Override: SELECT * FROM EMP WHERE HIRE_DATE > TO_DATE('$ $LAST_RUN', 'YYYY-MM-DD HH24:MI:SS')
- Target: ORACLE_TGT
  Load Type: Normal
  Update Strategy: Update else Insert
  Constraint-Based Loading: Enabled
- Partitions: 4 (Hash on EMPNO)
- DTMD Buffer: 256000
- Pushdown: Source-Side (Oracle)
- Retry on Failure: 3
- Retry on Deadlock: 5
- Stop on Errors: 100
- Bad File: $PMBadFileDir/EMP_$PMSessionName.bad
```
