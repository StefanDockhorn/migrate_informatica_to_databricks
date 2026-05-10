---
name: informatica-filter
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Filter transformations. Covers filter conditions and null handling. Includes Spark SQL WHERE clause equivalents. Do NOT use for general filtering patterns."
---

# Filter Transformation

## Purpose

Passive transformation that filters rows based on a single boolean condition.

## Configuration

| Property | Description |
|----------|-------------|
| Filter Condition | Boolean expression evaluated per row |

## Null Handling

Rows with NULL in the filter condition are **dropped** unless explicitly handled with `ISNULL()`.

## Behavior Rules

- Only rows where filter condition evaluates to TRUE pass through
- NULL values in filter condition evaluate to FALSE (row is dropped)
- Cannot route dropped rows elsewhere -- use Router for conditional routing
- Single condition only; use AND/OR for complex logic
- Filter is a passive transformation -- one output row per input row (that passes)

## Spark Equivalent

```python
from pyspark.sql.functions import col, isnan

# Simple filter
df = df.filter(col("SAL") > 1000)

# Complex filter with AND/OR
df = df.filter((col("SAL") > 1000) & (col("DEPTNO") == 10))

# NULL-safe filter
df = df.filter((col("STATUS") == "ACTIVE") | col("STATUS").isNull())

# String filter
df = df.filter(col("ENAME").like("A%"))

# SQL equivalent
df = spark.sql("SELECT * FROM emp WHERE SAL > 1000 AND DEPTNO = 10")
```

## Example

```
-- Filter Condition: SAL > 1000 AND DEPTNO = 10

# Spark equivalent:
df.filter((col("SAL") > 1000) & (col("DEPTNO") == 10))
```