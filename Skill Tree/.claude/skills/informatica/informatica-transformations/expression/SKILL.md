---
name: informatica-expression
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Expression transformations. Covers port expressions, local variables, default values, and function calls. Includes Spark SQL expression equivalents. Do NOT use for general SQL expression writing or non-Informatica transformation logic."
---

# Expression Transformation

## Purpose

Passive transformation to calculate values in a single row using expressions.

## Components

| Port Type | Direction | Purpose |
|-----------|-----------|---------|
| Input | Input | Receives data from upstream transformation |
| Output | Output | Returns calculated expression result |
| Variable | Internal | Stores temporary or cross-row values |

## Configuration

- **Expression Editor**: Build expressions using functions, ports, variables, and parameters
- **Functions**: Numeric, string, date, conversion, aggregation, and test functions
- **Parameters**: Use `$$Parameter` and `$Source/Target` variables in expressions

## Local Variables

Store values temporarily; evaluated top-to-bottom per row. Can store values across rows using variable ports with assignment.

```
-- Variable port evaluated before output ports
V_RUNNING_TOTAL = IIF(ISNULL(V_RUNNING_TOTAL), 0, V_RUNNING_TOTAL + IN_SAL)

-- Output port references variable
OUT_TOTAL_COMP = V_RUNNING_TOTAL + IN_BONUS
```

## Default Values

| Default Type | Trigger | Behavior |
|--------------|---------|----------|
| Default Input Value | Input port is NULL | Substitutes specified value |
| Default Output Value | Expression error | Substitutes specified value instead of failing |

## Behavior Rules

- Expressions execute top-to-bottom for each output port
- Variable ports must appear before output ports that reference them
- Use `RTRIM` on string comparisons; Informatica pads strings to declared precision
- Default values prevent errors from propagating through the pipeline
- Variable ports persist values across rows within a partition

## Spark Equivalent

```python
from pyspark.sql.functions import col, when, expr, isnan, lit

# Simple expression: withColumn
df = df.withColumn("OUT_TOTAL", col("SAL") + col("COMM"))

# Conditional expression: CASE WHEN equivalent
df = df.withColumn("OUT_TIER",
    when(col("SAL") > 5000, "High")
    .when(col("SAL") > 2000, "Medium")
    .otherwise("Low")
)

# Multiple expressions: selectExpr
df = df.selectExpr(
    "EMPNO",
    "ENAME",
    "SAL + COMM as OUT_TOTAL",
    "CASE WHEN SAL > 5000 THEN 'High' ELSE 'Low' END as OUT_TIER"
)
```

## Example

```
-- Variable port: V_COUNT = IIF(ISNULL(V_COUNT), 0, V_COUNT + 1)
-- Output port: OUT_SEQ = V_COUNT

-- Spark equivalent: monotonically_increasing_id() or window function
df = df.withColumn("OUT_SEQ", row_number().over(Window.orderBy("EMPNO")))
```