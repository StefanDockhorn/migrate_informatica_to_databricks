---
name: informatica-conditional-functions
description: "Use when analyzing or migrating Informatica PowerCenter conditional functions to Spark SQL. Covers IIF, DECODE, NVL, ISNULL, IS_DATE, IS_NUMBER, IS_SPACES, and ERROR. Includes exact Spark SQL CASE WHEN and coalesce equivalents. Do NOT use for general conditional logic."
---

# Informatica PowerCenter Conditional Functions → Spark SQL

## Quick Reference Table

| Informatica Function | Spark SQL Equivalent | Notes |
|---|---|---|
| `IIF(cond, true, false)` | `CASE WHEN cond THEN true ELSE false END` or `if(cond, t, f)` | Spark `if` is lazy |
| `DECODE(val, s1, r1, ..., def)` | `CASE WHEN val=s1 THEN r1 ... ELSE def END` | No direct equivalent |
| `NVL(val, replacement)` | `coalesce(val, replacement)` or `nvl(val, rep)` | `coalesce` takes N args |
| `ISNULL(val)` | `val IS NULL` | Boolean test |
| `IS_DATE(str, fmt)` | `try_cast(str as date) IS NOT NULL` | No direct equivalent |
| `IS_NUMBER(str)` | `try_cast(str as double) IS NOT NULL` | No direct equivalent |
| `IS_SPACES(str)` | `trim(str) = '' AND length(str) > 0` | All whitespace check |
| `ERROR(message)` | `assert_true(false)` or `raise_error(message)` | Error propagation differs |
| `ABORT(message)` | No direct equivalent | Immediate stop |

---

## IIF

**Syntax:** `IIF( condition, true_value, false_value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `condition` | Boolean | Yes | Condition to evaluate |
| `true_value` | Any | Yes | Value if condition is true |
| `false_value` | Any | Yes | Value if condition is false |

**Return Type:** Same as `true_value`/`false_value`

**Spark SQL Equivalent:** `CASE WHEN condition THEN true_value ELSE false_value END` or `if(condition, true_value, false_value)`

**Example:**
```sql
-- Informatica
IIF(Salary > 100000, 'High', 'Low')
IIF(ISNULL(MiddleName), FirstName || ' ' || LastName, FirstName || ' ' || MiddleName || ' ' || LastName)
IIF(Quantity = 0, 0, Revenue / Quantity)

-- Spark SQL
CASE WHEN Salary > 100000 THEN 'High' ELSE 'Low' END
if(Salary > 100000, 'High', 'Low')
CASE WHEN MiddleName IS NULL THEN concat(FirstName, ' ', LastName) ELSE concat(FirstName, ' ', MiddleName, ' ', LastName) END
CASE WHEN Quantity = 0 THEN 0 ELSE Revenue / Quantity END
```

**WHY it matters:** Informatica's `IIF` **always evaluates both branches** regardless of condition. This can cause division-by-zero errors even when the condition guards against it. Spark's `CASE WHEN` is **lazy** -- only evaluates the matching branch. Spark's `if()` function is also lazy.

**Negative case:** Do NOT directly translate nested `IIF` chains without reviewing branch safety. An expression like `IIF(denom = 0, 0, num / denom)` works in Spark's `CASE WHEN` but may fail in Informatica if `denom = 0` because both branches are evaluated.

---

## DECODE

**Syntax:** `DECODE( value, search1, result1 [, search2, result2, ...], default )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `value` | Any | Yes | Value to compare |
| `searchN` | Any | Yes | Value to match against |
| `resultN` | Any | Yes | Result if searchN matches |
| `default` | Any | Yes | Default if no match |

**Return Type:** Same as result/default values

**Spark SQL Equivalent:** `CASE WHEN` chain

**Example:**
```sql
-- Informatica
DECODE(StatusCode, 'A', 'Active', 'I', 'Inactive', 'D', 'Deleted', 'Unknown')
DECODE(RegionCode, 1, 'North', 2, 'South', 3, 'East', 4, 'West', 'Other')

-- Spark SQL
CASE StatusCode
    WHEN 'A' THEN 'Active'
    WHEN 'I' THEN 'Inactive'
    WHEN 'D' THEN 'Deleted'
    ELSE 'Unknown'
END

CASE RegionCode
    WHEN 1 THEN 'North'
    WHEN 2 THEN 'South'
    WHEN 3 THEN 'East'
    WHEN 4 THEN 'West'
    ELSE 'Other'
END
```

**WHY it matters:** DECODE is concise but limited to equality comparisons. `CASE WHEN` in Spark is more flexible (supports ranges, inequalities, etc.). DECODE with NULL search values requires special handling in Spark -- use `CASE WHEN value IS NULL THEN ...`.

---

## NVL

**Syntax:** `NVL( value, replacement_value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `value` | Any | Yes | Value to test for NULL |
| `replacement_value` | Any | Yes | Value to return if first is NULL |

**Return Type:** Same as input types

**Spark SQL Equivalent:** `coalesce(value, replacement_value)` or `nvl(value, replacement_value)`

**Example:**
```sql
-- Informatica
NVL(Commission, 0)
NVL(MiddleName, '')
NVL(ShipDate, OrderDate)

-- Spark SQL
coalesce(Commission, 0)
coalesce(MiddleName, '')
coalesce(ShipDate, OrderDate)
-- or:
nvl(Commission, 0)
```

**WHY it matters:** Spark's `coalesce` accepts any number of arguments (`coalesce(a, b, c, 0)`), making it more powerful than Informatica's 2-argument `NVL`. Spark also has `nvl()` which takes exactly 2 args and behaves identically to Informatica.

---

## ISNULL

**Syntax:** `ISNULL( value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `value` | Any | Yes | Value to test |

**Return Type:** Integer (1 = true, 0 = false)

**Spark SQL Equivalent:** `value IS NULL`

**Example:**
```sql
-- Informatica
ISNULL(MiddleName)           -- returns 1 if NULL, 0 if not
IIF(ISNULL(MiddleName), 'N/A', MiddleName)

-- Spark SQL
MiddleName IS NULL           -- returns true/false
CASE WHEN MiddleName IS NULL THEN 'N/A' ELSE MiddleName END
```

**WHY it matters:** Informatica's `ISNULL` returns `1`/`0` (integer); Spark's `IS NULL` returns `true`/`false` (boolean). Do NOT use `ISNULL` as a direct replacement in boolean contexts -- it returns the wrong type.

---

## IS_DATE

**Syntax:** `IS_DATE( string [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Value to test |
| `format` | String | No | Expected format mask |

**Return Type:** Integer (1 = valid date, 0 = invalid)

**Spark SQL Equivalent:** `try_cast` with IS NOT NULL check, or regex validation.

**Example:**
```sql
-- Informatica
IS_DATE('2023-12-25', 'YYYY-MM-DD')     -- returns 1
IS_DATE('not-a-date', 'YYYY-MM-DD')     -- returns 0
IS_DATE('25/12/2023', 'DD/MM/YYYY')     -- returns 1

-- Spark SQL
CASE WHEN try_cast('2023-12-25' AS DATE) IS NOT NULL THEN 1 ELSE 0 END
CASE WHEN '2023-12-25' RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END

-- With format validation
to_date('25/12/2023', 'dd/MM/yyyy') IS NOT NULL
```

**WHY it matters:** There is NO single Spark function that validates date format strings like `IS_DATE`. Use `try_cast` for a close approximation, or regex for strict format checking.

---

## IS_NUMBER

**Syntax:** `IS_NUMBER( string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Value to test |

**Return Type:** Integer (1 = valid number, 0 = invalid)

**Spark SQL Equivalent:** `try_cast` with IS NOT NULL check, or regex.

**Example:**
```sql
-- Informatica
IS_NUMBER('42')           -- returns 1
IS_NUMBER('3.14159')      -- returns 1
IS_NUMBER('1.5e10')       -- returns 1
IS_NUMBER('abc')          -- returns 0

-- Spark SQL
CASE WHEN try_cast('42' AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END
CASE WHEN '42' RLIKE '^-?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?$' THEN 1 ELSE 0 END
```

---

## IS_SPACES

**Syntax:** `IS_SPACES( string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Value to test |

**Return Type:** Integer (1 = all spaces, 0 = otherwise)

**Spark SQL Equivalent:** `trim(string) = '' AND length(string) > 0`

**Example:**
```sql
-- Informatica
IS_SPACES('   ')          -- returns 1
IS_SPACES('')             -- returns 0 (empty, not spaces)
IS_SPACES(' abc ')        -- returns 0
IS_SPACES(NULL)           -- returns 0

-- Spark SQL
trim('   ') = '' AND length('   ') > 0     -- returns true
trim('') = '' AND length('') > 0           -- returns false (length is 0)
trim(' abc ') = '' AND length(' abc ') > 0 -- returns false
```

**WHY it matters:** `IS_SPACES` returns `0` (false) for empty strings and NULL -- it only returns `1` when the string contains **only whitespace characters** and has length > 0. Spark's `trim() = ''` matches empty strings too, so the `length > 0` guard is essential.

---

## ERROR

**Syntax:** `ERROR( message )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `message` | String | Yes | Error message to log |

**Return Type:** None (raises error)

**Spark SQL Equivalent:** `assert_true(false)` or `raise_error(message)`

**Example:**
```sql
-- Informatica
IIF(BusinessRuleCheck = 0, ERROR('Business rule violation'), 'OK')

-- Spark SQL
CASE WHEN BusinessRuleCheck = 0 THEN raise_error('Business rule violation') ELSE 'OK' END
-- or:
assert_true(BusinessRuleCheck <> 0)   -- raises error with default message if false
```

**WHY it matters:** Informatica's `ERROR` logs the message and skips the row (configurable). Spark's `raise_error` aborts the entire query. For row-level error handling in Spark, use `CASE WHEN` to route bad rows to a separate column or use `try_cast` patterns instead.

---

## ABORT

**Syntax:** `ABORT( message )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `message` | String | Yes | Abort message |

**Return Type:** None (immediately stops session)

**Spark SQL Equivalent:** No direct equivalent -- Spark errors are query-level, not session-level.

**Example:**
```sql
-- Informatica
IIF(CriticalFlag = 'INVALID', ABORT('Critical validation failed'), 'OK')

-- Spark SQL -- NO DIRECT EQUIVALENT
-- Use assert_true for critical checks:
assert_true(CriticalFlag <> 'INVALID')
-- Or filter invalid rows before processing:
SELECT * FROM data WHERE CriticalFlag <> 'INVALID'
```

---

## Key Migration Patterns

### Nested IIF for Multiple Conditions
```sql
-- Informatica
IIF(Score >= 90, 'A', IIF(Score >= 80, 'B', IIF(Score >= 70, 'C', IIF(Score >= 60, 'D', 'F'))))

-- Spark SQL (simpler with CASE)
CASE
    WHEN Score >= 90 THEN 'A'
    WHEN Score >= 80 THEN 'B'
    WHEN Score >= 70 THEN 'C'
    WHEN Score >= 60 THEN 'D'
    ELSE 'F'
END
```

### Null-Safe Division
```sql
-- Informatica (both branches evaluated!)
IIF(Denominator = 0 OR ISNULL(Denominator), 0, Numerator / Denominator)

-- Spark SQL (lazy evaluation - safe)
CASE WHEN Denominator = 0 OR Denominator IS NULL THEN 0 ELSE Numerator / Denominator END
```

### Default Value Cascade
```sql
-- Informatica
NVL(NVL(NVL(PreferredName, Nickname), FirstName), 'Unknown')

-- Spark SQL (cleaner)
coalesce(PreferredName, Nickname, FirstName, 'Unknown')
```

### Data Validation with IS_DATE
```sql
-- Informatica
IIF(IS_DATE(DateString, 'YYYY-MM-DD') = 0, ERROR('Invalid date'), TO_DATE(DateString, 'YYYY-MM-DD'))

-- Spark SQL
CASE
    WHEN try_cast(DateString AS DATE) IS NULL THEN raise_error('Invalid date')
    ELSE to_date(DateString, 'yyyy-MM-dd')
END
```
