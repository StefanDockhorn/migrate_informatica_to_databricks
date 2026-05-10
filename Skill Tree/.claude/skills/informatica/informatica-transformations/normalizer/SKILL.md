---
name: informatica-normalizer
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Normalizer transformations. Covers VSAM source normalizer and pipeline normalizer, generated keys (GK, GCID), and column pivoting. Includes Spark explode() and pivot() equivalents. Do NOT use for general normalization patterns."
---

# Normalizer Transformation

## Purpose

**VSAM Normalizer**: Flattens COBOL/VSAM sources with OCCURS clauses.
**Pipeline Normalizer**: Pivots columns to rows.

## VSAM Normalizer

Processes `OCCURS` clauses in COBOL copybooks into separate rows. Each occurrence becomes one output row.

## Pipeline Normalizer

Converts multiple-occurring column groups into separate rows. Input has repeating columns; output has normalized rows.

## Generated Keys

| Key | Description |
|-----|-------------|
| GCID (Generated Column ID) | Identifies which source occurrence the row came from (1-based index) |
| GK (Generated Key) | Provides unique row identifier; increments per output row |

## Ports

- **VSAM**: Input ports for each occurring element; output ports for normalized rows
- **Pipeline**: Input ports for each occurring column group; output row port

## Behavior Rules

- VSAM Normalizer requires OCCURS clause in source definition
- Pipeline Normalizer requires at least one occurring column group
- GCID values are 1-based index of the source occurrence (1, 2, 3, ...)
- GK values increment for each output row and reset per input row
- Number of output rows = number of occurrences per input row

## Spark Equivalent

```python
from pyspark.sql.functions import explode, arrays_zip, col, lit, array, struct

# Pipeline Normalizer: pivot columns to rows
df = spark.createDataFrame([
    (1, 100, 200, 300, 400),
    (2, 150, 250, 350, 450)
], ["EMPNO", "SALES_Q1", "SALES_Q2", "SALES_Q3", "SALES_Q4"])

# Using arrays_zip + explode
df.select(
    col("EMPNO"),
    explode(
        arrays_zip(
            lit("Q1").alias("QUARTER"), col("SALES_Q1").alias("SALES"),
            lit("Q2").alias("QUARTER"), col("SALES_Q2").alias("SALES"),
            lit("Q3").alias("QUARTER"), col("SALES_Q3").alias("SALES"),
            lit("Q4").alias("QUARTER"), col("SALES_Q4").alias("SALES")
        )
    ).alias("zipped")
)

# Simpler: melt/unpivot using stack()
df.selectExpr(
    "EMPNO",
    "stack(4, 'Q1', SALES_Q1, 'Q2', SALES_Q2, 'Q3', SALES_Q3, 'Q4', SALES_Q4) as (QUARTER, SALES)"
)

# VSAM Normalizer: explode array-like structures
df.withColumn("element", explode("occurs_array"))
```

## Example

```
-- Input: SALES_Q1, SALES_Q2, SALES_Q3, SALES_Q4
-- Output rows: (SALES_Q1, GCID=1), (SALES_Q2, GCID=2), ...

# Spark equivalent:
df.selectExpr(
    "EMPNO",
    "stack(4, 'Q1', SALES_Q1, 'Q2', SALES_Q2, 'Q3', SALES_Q3, 'Q4', SALES_Q4) as (QUARTER, SALES)"
)
```