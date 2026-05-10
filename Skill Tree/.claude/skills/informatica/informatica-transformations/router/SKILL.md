---
name: informatica-router
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Router transformations. Covers input/output groups, group filter conditions, and default group. Includes Spark SQL CASE WHEN and filter equivalents. Do NOT use for general routing logic."
---

# Router Transformation

## Purpose

Active transformation that routes data to multiple output groups based on conditions.

## Components

| Component | Description |
|-----------|-------------|
| Input Group | Single input group receiving all upstream rows |
| Output Groups | Multiple output groups with filter conditions |
| Default Group | Catch-all group for rows matching no condition |

## Group Filter Condition

Boolean expression per output group. A row goes to **all** groups whose condition evaluates to TRUE.

## Default Group

Contains rows not matching any output group condition. Can be enabled or disabled.

## Behavior Rules

- One input row can go to **multiple** output groups (unlike Filter which drops non-matching rows)
- If no group filter condition matches and default group is disabled, row is dropped
- Group filter conditions evaluate for each row independently
- Cannot modify data in Router -- use Expression before/after Router for data transformation
- Output groups are independent pipelines after the Router

## Spark Equivalent

```python
from pyspark.sql.functions import when, col

# Router with multiple output groups (multiple DataFrames)
high_value_df = df.filter(col("SAL") > 5000)
low_value_df = df.filter(col("SAL") <= 5000)

# Or using CASE WHEN for conditional column values (single DataFrame)
df = df.withColumn("CATEGORY",
    when(col("SAL") > 5000, "High_Value")
    .when(col("SAL") > 2000, "Medium_Value")
    .otherwise("Low_Value")
)

# Multiple overlapping conditions (row goes to multiple outputs)
exec_df = df.filter(col("TITLE").isin("VP", "Director"))
sales_df = df.filter(col("DEPT") == "Sales")
# A VP in Sales appears in both DataFrames
```

## Example

```
-- Output Group "High_Value": SAL > 5000
-- Output Group "Low_Value": SAL <= 5000
-- Default Group: (disabled)

-- Row with SAL=10000 goes to High_Value only
-- Row with SAL=3000 goes to Low_Value only
-- Row with SAL=5000 goes to Low_Value (condition is <=)

# Spark equivalent:
high_value_df = df.filter(col("SAL") > 5000)
low_value_df = df.filter(col("SAL") <= 5000)
```