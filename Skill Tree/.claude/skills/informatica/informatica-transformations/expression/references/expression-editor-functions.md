# Expression Editor Functions Reference

> **When to use this file:** When the main `informatica-expression` skill does not cover specific Expression Editor functions or complex nested expression patterns.

## Expression Editor Quick Reference

The Expression transformation uses the Informatica function library. Every output port contains one expression that can reference:

- Input ports (by name)
- Variable ports (by name, must appear before the output port that uses them)
- Built-in functions
- Mapping parameters and variables ($$ParamName)
- Pre-defined session variables ($$$SessStartTime, etc.)

## Port Execution Order

Expressions evaluate top-to-bottom. Variable ports must be physically above output ports that reference them.

```
Input Ports:    IN_SAL, IN_BONUS, IN_DEPTNO
Variable Ports: V_NET_PAY = IN_SAL + NVL(IN_BONUS, 0)      -- evaluates first
                V_TAX = V_NET_PAY * 0.25                    -- can reference V_NET_PAY
Output Ports:   OUT_TOTAL = V_NET_PAY - V_TAX               -- can reference both variables
                OUT_DEPTNO = IN_DEPTNO                      -- pass-through
```

## Nested Function Patterns

### Null-Handling Chain
```
NVL(NVL(FIELD_A, FIELD_B), 'DEFAULT')
```
Spark: `coalesce(col('FIELD_A'), col('FIELD_B'), lit('DEFAULT'))`

### Conditional with Multiple Checks
```
IIF(ISNULL(CUSTOMER_ID), 'UNKNOWN',
    IIF(LENGTH(CUSTOMER_ID) < 5, 'INVALID', CUSTOMER_ID))
```
Spark: `when(col('CUSTOMER_ID').isNull(), 'UNKNOWN').when(length(col('CUSTOMER_ID')) < 5, 'INVALID').otherwise(col('CUSTOMER_ID'))`

### Date Parsing with Fallback
```
IIF(IS_DATE(ORDER_DATE, 'YYYY-MM-DD'), TO_DATE(ORDER_DATE, 'YYYY-MM-DD'),
    IIF(IS_DATE(ORDER_DATE, 'MM/DD/YYYY'), TO_DATE(ORDER_DATE, 'MM/DD/YYYY'),
        TO_DATE('1900-01-01', 'YYYY-MM-DD')))
```

### String Normalization
```
UPPER(RTRIM(LTRIM(REPLACECHR(0, INPUT_NAME, '  ', ' '))))
```
Spark: `trim(regexp_replace(upper(col('INPUT_NAME')), ' +', ' '))`

## Special Variables in Expressions

| Variable | Meaning | Spark Equivalent |
|---|---|---|
| $$Parameter | Mapping parameter (constant) | `spark.conf.get()` or job parameter |
| $$Variable | Mapping variable (can change) | Delta table for state |
| $$$SessStartTime | Session start timestamp | Job start timestamp |
| SYSDATE | Current date/time | `current_date()` / `current_timestamp()` |

## Default Values

| Type | Default Input | Default Output (on error) |
|---|---|---|
| String | `ERROR('default')` | `ABORT('transform error')` |
| Number | `0` or `-1` | `0` |
| Date | `TO_DATE('1900-01-01', 'YYYY-MM-DD')` | `NULL` |

Always set explicit default output values — Informatica silently returns NULL on transformation errors unless configured otherwise.
