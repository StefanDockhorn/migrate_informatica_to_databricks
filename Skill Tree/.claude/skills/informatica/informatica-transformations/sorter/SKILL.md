---
name: informatica-sorter
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Sorter transformations. Covers sort keys, distinct output, case sensitivity, null handling, and sorter cache. Includes Spark SQL ORDER BY and dropDuplicates() equivalents. Do NOT use for general sorting patterns."
---

# Sorter Transformation

## Purpose

Active transformation that sorts data on one or more ports.

## Properties

| Property | Description |
|----------|-------------|
| Sorter Cache Size | Memory allocated for sorting; spills to Work Directory when exceeded |
| Case Sensitive | When enabled, sorts uppercase before lowercase for string ports |
| Work Directory | Directory for disk-based merge sort spill files |
| Distinct Output Rows | Removes duplicate rows (all ports compared) |
| Null Treated Low | When enabled, NULL sorts before non-null values |
| Transformation Scope | Transaction or All Input |

## Sort Keys

Ports designated as "Key" with sort order:
- **Ascending**: A-Z, 0-9, NULL last (unless Null Treated Low)
- **Descending**: Z-A, 9-0, NULL first (unless Null Treated Low)

Sort priority follows port order (first key = primary sort).

## Distinct

Setting "Distinct Output Rows" removes duplicate rows. Distinct applies to **ALL** output ports, not just key ports.

## Case Sensitivity

When enabled, sorts uppercase before lowercase for string ports (ASCII ordering: 'A' < 'a').

## Null Treated Low

When enabled, NULL values sort before non-null values. Affects downstream Joiner and Aggregator when using sorted input.

## Sorter Cache

Data cache for sorting. When data exceeds cache size, Sorter performs disk-based merge sort using the Work Directory.

## Behavior Rules

- Distinct applies to ALL output ports, not just key ports
- Sorter cache size should accommodate all data for in-memory sort; otherwise disk merge sort is used
- Always sort before Aggregator (sorted input) or Joiner (sorted input) when possible
- Null Treated Low affects join and aggregate results when used with sorted input
- Sort order in Sorter must exactly match sort order expected by downstream transformations

## Spark Equivalent

```python
from pyspark.sql.functions import col, desc, asc_nulls_first, desc_nulls_first

# Single column sort
df = df.orderBy(col("SAL").desc())

# Multi-column sort
df = df.orderBy(col("SAL").desc(), col("ENAME").asc())

# Distinct rows (all columns)
df = df.dropDuplicates()

# Distinct on specific columns
df = df.dropDuplicates(["DEPTNO", "JOB"])

# Null handling
df = df.orderBy(asc_nulls_first("COMM"))  # Null Treated Low
df = df.orderBy(desc_nulls_first("COMM"))  # Nulls first, then descending

# Sort within partitions (co-located sort)
df = df.repartition("DEPTNO").sortWithinPartitions("SAL").desc()
```

## Example

```
-- Sort key 1: SAL (Descending)
-- Sort key 2: ENAME (Ascending)
-- Distinct: No
-- Case Sensitive: Yes
-- Null Treated Low: Yes

# Spark equivalent:
df.orderBy(col("SAL").desc(), col("ENAME").asc())
```