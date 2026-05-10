---
name: informatica-stored-procedure
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Stored Procedure transformations. Covers connected/unconnected modes, pre/post session execution, parameter passing, and return values. Includes Spark UDF and JDBC call equivalents. Do NOT use for general stored procedure patterns."
---

# Stored Procedure Transformation

## Purpose

Calls database stored procedures from mappings or sessions.

## Execution Modes

| Mode | When Called | Return Behavior |
|------|-------------|-----------------|
| Connected | Per row in pipeline | Returns result set |
| Unconnected | Called from expression | Returns single value via PROC_RESULT |
| Pre-Session | Once before session starts | Session stops on error |
| Post-Session | Once after session completes | Error logged but session succeeds |

## Parameter Types

| Type | Direction | Behavior |
|------|-----------|----------|
| IN | Input | Passes value from pipeline to procedure |
| OUT | Output | Returns value from procedure to pipeline |
| INOUT | Both | Passes in and returns modified value |

## Calling from Expression

```
:SP.stored_procedure_name(param1, param2, PROC_RESULT)
```

`PROC_RESULT` is a special port that captures the stored procedure return code or OUT parameter.

## Return Values

- **PROC_RESULT**: Captures stored procedure return code
- **OUT parameters**: Mapped to output ports by position

## Error Handling

- Pre-session errors: Stop the session
- Post-session errors: Logged; session succeeds
- Connected mode errors: Row-level error handling applies

## Behavior Rules

- Unconnected stored procedures called with `:SP` syntax in Expression transformation
- Pre-session SQL runs once before session starts
- PROC_RESULT is a special port that captures the stored procedure return code
- Output ports must match stored procedure OUT parameters by position
- Pre-session errors are fatal; post-session errors are non-fatal
- Connected mode calls procedure once per input row

## Spark Equivalent

```python
from pyspark.sql.functions import udf
import subprocess

# Spark UDF equivalent for row-level processing
@udf("string")
def call_procedure(emp_id):
    # JDBC callable statement
    conn = get_jdbc_connection()
    cursor = conn.cursor()
    cursor.callproc("GET_EMP_NAME", [emp_id])
    result = cursor.fetchone()[0]
    conn.close()
    return result

df = df.withColumn("EMP_NAME", call_procedure(col("EMP_ID")))

# JDBC direct execution
jdbc_url = "jdbc:postgresql://host/db"
result = spark.read.jdbc(url=jdbc_url, table="(CALL my_procedure()) tmp")

# Pre/post session: handled in pipeline orchestration
```

## Example

```
-- Expression call: :SP.GET_EMP_NAME(EMP_ID, PROC_RESULT)
-- PROC_RESULT linked to output port containing employee name

# Spark equivalent:
@udf("string")
def get_emp_name(emp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("CALL GET_EMP_NAME(?)", (emp_id,))
    return cursor.fetchone()[0]

df = df.withColumn("EMP_NAME", get_emp_name(col("EMP_ID")))
```