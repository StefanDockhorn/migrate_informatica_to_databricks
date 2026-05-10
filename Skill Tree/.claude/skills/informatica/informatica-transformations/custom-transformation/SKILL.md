---
name: informatica-custom-transformation
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Custom transformations (API-based C/C++ extensions). Covers procedure development, blocking, transaction control, row-based vs array-based modes. Includes Spark UDF and pandas_udf equivalents. Do NOT use for general custom code patterns."
---

# Custom Transformation

## Purpose

Extends PowerCenter with custom C/C++ or Java transformation logic via the Transformation Developer API.

## API Modes

| Mode | Description | Use When |
|------|-------------|----------|
| Row-based | Processes one row at a time | Simple logic, low volume |
| Array-based | Processes batches of rows | High volume, complex logic |

## Key API Functions

| Function | Purpose |
|----------|---------|
| `p_<tx>_init()` | Initialize resources, open connections |
| `p_<tx>_deinit()` | Clean up resources, close connections |
| `p_<tx>_inputRowNotification()` | Called for each input row |
| `p_<tx>_outputRowNotification()` | Called when output row is generated |
| `INFA_CTGetData()` | Read input port data |
| `INFA_CTASetData()` | Write output port data |

## Blocking

Custom transformations can be configured as blocking transformations. This affects pipeline parallelism -- downstream transformations wait until the custom transformation completes processing all input rows.

## Transaction Control

Supports generating transactions and handling transaction boundaries within custom code. Can emit `TC_COMMIT_BEFORE`, `TC_COMMIT_AFTER`, etc.

## Behavior Rules

- Custom transformations must implement required API functions (`init`, `deinit`, row processing)
- Array-based mode processes batches of rows for better performance -- always prefer for high-volume scenarios
- Blocking custom transformations affect pipeline parallelism -- use only when necessary
- Code page compatibility required between procedure and Integration Service
- Memory allocated in `init` must be freed in `deinit`
- Return `INFA_SUCCESS` or `INFA_FAILURE` from all API functions

## Spark Equivalent

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType

# Python UDF (row-based, simple logic)
@udf("string")
def custom_transform(input_val):
    return input_val.upper() + "_MODIFIED" if input_val else None

df = df.withColumn("OUT_COL", custom_transform(col("IN_COL")))

# Pandas UDF (array-based, vectorized, high performance)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def custom_transform_batch(inputs: pd.Series) -> pd.Series:
    return inputs.str.upper() + "_MODIFIED"

df = df.withColumn("OUT_COL", custom_transform_batch(col("IN_COL")))

# For complex multi-port logic, use mapInPandas

def transform_batch(iterator):
    for pdf in iterator:
        pdf["OUT_COL"] = pdf["IN_COL"].str.upper() + "_MODIFIED"
        yield pdf

df = df.mapInPandas(transform_batch, schema=df.schema.add("OUT_COL", "string"))
```

## Example

```
-- Custom transformation: validate and enrich customer records
-- Input: CUSTOMER_ID, RAW_NAME, RAW_ADDRESS
-- Output: CUSTOMER_ID, CLEAN_NAME, CLEAN_ADDRESS, VALIDATION_STATUS

# Spark equivalent (pandas UDF for batch processing):
@pandas_udf(result_schema)
def enrich_customers(pdf):
    pdf["CLEAN_NAME"] = pdf["RAW_NAME"].str.strip().str.title()
    pdf["CLEAN_ADDRESS"] = pdf["RAW_ADDRESS"].str.strip()
    pdf["VALIDATION_STATUS"] = np.where(
        pdf["CUSTOMER_ID"].notna(), "VALID", "INVALID"
    )
    return pdf
```