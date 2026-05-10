---
name: informatica-source-qualifier
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Source Qualifier transformations. Covers SQL override, joins, filters, sorted ports, and select distinct. Includes Databricks Spark equivalents using spark.read and SQL hints. Do NOT use for general database query optimization or non-Informatica data ingestion."
---

# Source Qualifier Transformation

## Purpose

Represents the rows that the Integration Service reads from a source when it runs a session. Always used with relational sources.

## Configuration

| Property | Description |
|----------|-------------|
| SQL Query | Overrides the default SELECT statement |
| User-Defined Join | Custom join condition (replaces default PK-FK join) |
| Source Filter | WHERE clause applied at source |
| Number Of Sorted Ports | Enables ORDER BY for sorted input downstream |
| Select Distinct | Adds DISTINCT to the generated query |
| Pre SQL | Executes before session reads from source |
| Post SQL | Executes after session finishes reading |
| Tracing Level | Verbosity of transformation session log |

## Default Query

Automatically generated `SELECT` statement. Can be overridden but must include same ports in same order.

## Join Support

| Join Type | Syntax / Behavior |
|-----------|-------------------|
| Default Join | Primary key-foreign key relationship |
| Custom Join | User-Defined Join property |
| Heterogeneous Join | Requires database links or gateway |
| Outer Join | Informatica syntax: `{ target_table LEFT OUTER JOIN source_table ON join_condition }` |

## Behavior Rules

- Always use Source Qualifier for relational sources; cannot read relational data without it
- SQL override must list SELECT fields in same order as source qualifier ports
- When using sorted ports, Integration Service adds ORDER BY clause -- must match Sorter transformation sort order
- Pre-SQL executes before the session reads from source; Post-SQL executes after
- Heterogeneous joins require database links or Informatica gateway connections

## Spark Equivalent

```python
# JDBC read with SQL override equivalent
df = spark.read.jdbc(
    url="jdbc:oracle:thin:@host:1521/db",
    table="(SELECT EMP.EMPNO, EMP.ENAME, DEPT.DNAME FROM EMP, DEPT WHERE EMP.DEPTNO = DEPT.DEPTNO AND EMP.SAL > 1000) sq",
    properties={"user": "user", "password": "pass"}
)

# Or using option("query", ...) pattern
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://host/db") \
    .option("query", "SELECT * FROM EMP WHERE SAL > 1000") \
    .load()
```

## Edge Cases

- SQL override with datetime parameters must use Informatica `$$$SessStartTime` format
- Heterogeneous joins require database links or gateway -- prefer pushing joins to database when possible
- Sorted ports must match the exact sort order used by downstream Sorter or Joiner transformations
- Pre-SQL/Post-SQL runs on the source database connection, not the Integration Service host

## Example

```sql
-- Informatica Source Qualifier SQL Override
SELECT EMP.EMPNO, EMP.ENAME, DEPT.DNAME
FROM EMP, DEPT
WHERE EMP.DEPTNO = DEPT.DEPTNO
AND EMP.SAL > 1000
```

```python
# Spark equivalent
spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT EMP.EMPNO, EMP.ENAME, DEPT.DNAME FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO WHERE EMP.SAL > 1000) sq",
    properties=db_props
)
```