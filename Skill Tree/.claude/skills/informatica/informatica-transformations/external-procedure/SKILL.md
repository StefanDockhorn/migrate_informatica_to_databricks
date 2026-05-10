---
name: informatica-external-procedure
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter External Procedure transformations. Covers COM and Informatica procedure types, dispatch functions, and parameter access. Includes Spark external library integration. Do NOT use for general external code patterns."
---

# External Procedure Transformation

## Purpose

Calls external procedures implemented as COM components (Windows) or Informatica native modules (cross-platform).

## Types

| Type | Platform | Implementation |
|------|----------|----------------|
| COM External Procedure | Windows only | COM-registered DLL |
| Informatica External Procedure | Cross-platform | Shared library (.so / .dll) |

## Execution

- **Row-level**: Called once per input row
- **Module-level**: Initialized once, used across all rows in the session

## Return Values

Must return success/failure codes:
- `ISSUCCESS`: Procedure completed successfully
- `ISFAILURE`: Procedure failed; error logged

## Memory Management

Allocate and deallocate memory for string parameters:
- Use `INFA_CTAllocateMemory()` for output strings
- Use `INFA_CTFreeMemory()` to release allocated memory

## Error Handling

Generate error and tracing messages via the External Procedure API.

## Unconnected Mode

Can be called similar to unconnected lookup or unconnected stored procedure using expression syntax.

## Behavior Rules

- COM procedures require Windows platform and COM registration
- Informatica procedures built as shared libraries (.so on Linux, .dll on Windows)
- Must handle memory allocation for output string parameters -- memory leaks crash the Integration Service
- Row-level procedures called once per row; module-level procedures initialized once
- Return codes must be `ISSUCCESS` or `ISFAILURE`
- Unconnected external procedures called from Expression transformation

## Spark Equivalent

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# External Python/Java libraries
import external_library

@udf("string")
def call_external_procedure(input_val):
    try:
        result = external_library.process(input_val)
        return result
    except Exception as e:
        return None  # Or raise with error handling

# JAR dependencies (for Java external procedures)
spark.sparkContext.addPyFile("/path/to/external-lib.zip")

# Native library integration via subprocess
import subprocess

@udf("string")
def call_native_procedure(input_val):
    result = subprocess.run(
        ["/path/to/native_proc", input_val],
        capture_output=True, text=True, timeout=30
    )
    return result.stdout.strip()
```

## Example

```
-- External Procedure: validate_tax_id
-- Input: TAX_ID (string)
-- Output: IS_VALID (boolean)
-- Implementation: Native shared library calling government API

# Spark equivalent:
@udf("boolean")
def validate_tax_id(tax_id):
    if tax_id is None or len(tax_id) != 9:
        return False
    return external_validation_api.check(tax_id)

df = df.withColumn("IS_VALID", validate_tax_id(col("TAX_ID")))
```