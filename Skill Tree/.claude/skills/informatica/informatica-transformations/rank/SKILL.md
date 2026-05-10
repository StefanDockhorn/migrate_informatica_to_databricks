---
name: informatica-rank
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Rank transformations. Covers rank index, rank port, group by, top/bottom ranking, and rank cache. Includes Spark SQL window functions (RANK, DENSE_RANK, ROW_NUMBER) equivalents. Do NOT use for general ranking patterns."
---

# Rank Transformation

## Purpose

Active transformation that filters top or bottom N rows per group.

## Components

| Component | Description |
|-----------|-------------|
| Rank port | Value to rank on (numeric or string) |
| Group By ports | Optional -- defines ranking groups |
| Rank Index | Automatically generated output port showing rank position |
| Top/Bottom | Ranking direction |
| Number of Ranks | Maximum rank to output per group |

## Rank Index

Automatically generated output port. Values start at 1 for each group.
- Ties receive the same rank value; next rank skips (1, 2, 2, 4...)
- **Not** `DENSE_RANK` -- uses standard competition ranking (gaps after ties)

## Top/Bottom

| Setting | Behavior |
|---------|----------|
| Top | Highest values rank first (1 = highest) |
| Bottom | Lowest values rank first (1 = lowest) |

## Number of Ranks

Maximum number of rows to output per group. Example: `Number of Ranks = 3` returns top 3 rows per department.

## Rank Cache

Stores group information and rank calculations. Spills to disk when memory is exceeded.

## String Ranking

Case-sensitive comparison; uppercase < lowercase by ASCII value. 'APPLE' ranks before 'apple'.

## Behavior Rules

- Rank Index starts at 1 for each group
- Ties receive same rank value; next rank skips (1, 2, 2, 4...)
- Rank is **blocking** -- all rows must be received before ranking
- Cannot use Rank with aggregate functions in the same transformation
- Number of Ranks limits output rows per group; remaining rows are dropped

## Spark Equivalent

```python
from pyspark.sql.functions import rank, dense_rank, row_number, col
from pyspark.sql.window import Window

# Top N per group (equivalent to Rank transformation)
window_spec = Window.partitionBy("DEPTNO").orderBy(col("SAL").desc())
result = df.withColumn("rank_index", rank().over(window_spec)) \
    .filter(col("rank_index") <= 3)

# Using row_number() for deterministic top N (no ties sharing rank)
window_spec = Window.partitionBy("DEPTNO").orderBy(col("SAL").desc(), col("EMPNO").asc())
result = df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") <= 3)

# Bottom N (ascending order)
window_spec = Window.partitionBy("DEPTNO").orderBy(col("SAL").asc())
result = df.withColumn("rank_index", rank().over(window_spec)) \
    .filter(col("rank_index") <= 5)

# Without group by (global rank)
window_spec = Window.orderBy(col("SAL").desc())
result = df.withColumn("rank_index", rank().over(window_spec)) \
    .filter(col("rank_index") <= 10)
```

## Example

```
-- Rank Port: SAL (Top)
-- Group By: DEPTNO
-- Number of Ranks: 3
-- Returns top 3 salaries per department

# Spark equivalent:
from pyspark.sql.window import Window
window_spec = Window.partitionBy("DEPTNO").orderBy(col("SAL").desc())
df.withColumn("RANK_INDEX", rank().over(window_spec)) \
  .filter(col("RANK_INDEX") <= 3)
```