---
name: informatica-aggregator
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Aggregator transformations. Covers aggregate functions, group by ports, sorted input, aggregate cache, and conditional aggregation. Includes Spark SQL GROUP BY and window function equivalents. Do NOT use for general aggregation patterns."
---

# Aggregator Transformation

## Purpose

Active transformation that performs aggregate calculations on groups.

## Components

| Component | Description |
|-----------|-------------|
| Aggregate expression | Expression per output port using aggregate functions |
| Group By ports | Non-aggregate output ports that define grouping |
| Sorted Input | Optional -- enables streaming aggregation |

## Aggregate Functions

`SUM`, `AVG`, `COUNT`, `MAX`, `MIN`, `FIRST`, `LAST`, `MEDIAN`, `PERCENTILE`, `STDDEV`, `VARIANCE`

- **Nested Aggregate Functions**: Not supported (e.g., `MAX(SUM(amount))` is invalid)
- **Conditional Clauses**: Use `IIF` within aggregate for conditional aggregation: `SUM(IIF(REGION='EAST', SAL, 0))`
- **Null Values**: Aggregate functions ignore NULL rows; `COUNT(*)` counts all rows including NULLs

## Group By

Non-aggregate output ports must be designated as Group By ports. Each unique combination of Group By values produces one output row.

## Sorted Input

Requires pre-sorted data on all Group By ports. Enables aggregator to use less memory by streaming groups rather than caching all groups.

## Aggregate Cache

- **Data Cache**: Stores group aggregation values (running sums, counts, etc.)
- **Index Cache**: Stores group key pointers
- Spills to disk when cache size is exceeded

## Behavior Rules

- Use sorted input when data is pre-sorted -- dramatically reduces memory usage
- All non-aggregate output ports must be designated as Group By
- Aggregator is **blocking** -- must receive all rows before outputting
- `FIRST`/`LAST` require sorted input to be meaningful
- `COUNT` ignores NULLs; `COUNT(*)` counts all rows
- Conditional aggregation uses `IIF` inside the aggregate function

## Spark Equivalent

```python
from pyspark.sql.functions import sum, avg, count, max, min, col, when

# Basic groupBy + aggregation
result = df.groupBy("DEPTNO").agg(
    sum("SAL").alias("TOTAL_SAL"),
    avg("SAL").alias("AVG_SAL"),
    count("*").alias("EMP_COUNT")
)

# Conditional aggregation
result = df.groupBy("DEPTNO").agg(
    sum(when(col("REGION") == "EAST", col("SAL")).otherwise(0)).alias("EAST_SAL"),
    sum(when(col("REGION") == "WEST", col("SAL")).otherwise(0)).alias("WEST_SAL")
)

# Running aggregates with window functions
from pyspark.sql.window import Window
window_spec = Window.partitionBy("DEPTNO").orderBy("EMPNO")
df = df.withColumn("RUNNING_TOTAL", sum("SAL").over(window_spec))

# Rollup and cube
result = df.rollup("DEPTNO", "JOB").agg(sum("SAL").alias("TOTAL_SAL"))
```

## Example

```
-- Aggregate expression: SUM(SAL)
-- Group By: DEPTNO
-- Conditional: SUM(IIF(REGION='EAST', SAL, 0))

-- Spark equivalent:
df.groupBy("DEPTNO").agg(
    sum("SAL").alias("SUM_SAL"),
    sum(when(col("REGION") == "EAST", col("SAL")).otherwise(0)).alias("EAST_SAL")
)
```