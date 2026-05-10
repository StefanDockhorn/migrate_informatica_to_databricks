---
name: informatica-conversion-functions
description: "Use when analyzing or migrating Informatica PowerCenter conversion functions to Spark SQL. Covers TO_INTEGER, TO_DECIMAL, TO_FLOAT, TO_CHAR, TO_DATE, and casting behaviors. Includes exact Spark SQL cast equivalents and null/error handling differences. Do NOT use for general type casting."
---

# Informatica PowerCenter Conversion Functions → Spark SQL

## Quick Reference Table

| Informatica Function | Spark SQL Equivalent | Notes |
|---|---|---|
| `TO_INTEGER(string)` | `cast(string as int)` | Returns NULL on failure |
| `TO_DECIMAL(string [, scale])` | `cast(string as decimal(p,s))` | Precision/scale required in Spark |
| `TO_FLOAT(string)` | `cast(string as float)` | Returns NULL on failure |
| `TO_CHAR(number)` | `cast(number as string)` | Simple cast |
| `TO_CHAR(date, format)` | `date_format(date, format)` | Format mask translation needed |
| `TO_DATE(string, format)` | `to_date(string, format)` | Format mask translation needed |
| `TO_TIMESTAMP(string, format)` | `to_timestamp(string, format)` | Format mask translation needed |
| `HEX_TO_INTEGER(hex)` | `conv(hex, 16, 10)` | Base conversion |
| `CHRCODE(string)` | `ascii(string)` | First character's ASCII value |

---

## TO_INTEGER

**Syntax:** `TO_INTEGER( string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to convert to integer |

**Return Type:** Integer

**Spark SQL Equivalent:** `cast(string as int)` or `try_cast(string as int)`

**Example:**
```sql
-- Informatica
TO_INTEGER('42')         -- returns 42
TO_INTEGER('3.99')       -- returns 3 (truncates, not rounds)
TO_INTEGER('abc')        -- returns NULL
TO_INTEGER(NULL)         -- returns NULL

-- Spark SQL
cast('42' as int)        -- returns 42
cast('3.99' as int)      -- returns 3 (truncates)
try_cast('abc' as int)   -- returns NULL (safe)
```

**WHY it matters:** Informatica returns `NULL` on conversion failure. In Spark, `cast` may throw on failure depending on configuration. Always use `try_cast` for safe conversions that mirror Informatica behavior.

**Negative case:** Do NOT use `cast` without null handling when the source data may contain invalid values. Use `try_cast` or wrap in `CASE WHEN` with a regex pre-check.

---

## TO_DECIMAL

**Syntax:** `TO_DECIMAL( string [, scale] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to convert to decimal |
| `scale` | Integer | No | Number of decimal places |

**Return Type:** Decimal

**Spark SQL Equivalent:** `cast(string as decimal(precision, scale))`

**Example:**
```sql
-- Informatica
TO_DECIMAL('123.456')       -- returns 123.456 (default precision/scale)
TO_DECIMAL('123.456', 2)    -- returns 123.46 (rounded to 2 places)
TO_DECIMAL('999.999', 1)    -- returns 1000.0

-- Spark SQL
cast('123.456' as decimal(10,3))     -- returns 123.456
cast('123.456' as decimal(10,2))     -- returns 123.46
cast('999.999' as decimal(10,1))     -- returns 1000.0
```

**WHY it matters:** Informatica's `TO_DECIMAL` infers precision from the port definition. Spark requires explicit `decimal(precision, scale)` declaration. The `scale` parameter in Informatica truncates/rounds; in Spark it defines storage precision.

---

## TO_FLOAT

**Syntax:** `TO_FLOAT( string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to convert to floating-point |

**Return Type:** Double

**Spark SQL Equivalent:** `cast(string as float)` or `cast(string as double)`

**Example:**
```sql
-- Informatica
TO_FLOAT('3.14159')      -- returns 3.14159
TO_FLOAT('2.5e10')       -- returns 25000000000.0
TO_FLOAT('not numeric')  -- returns NULL

-- Spark SQL
cast('3.14159' as float)     -- returns 3.14159
cast('3.14159' as double)    -- returns 3.14159 (higher precision)
try_cast('not numeric' as float)  -- returns NULL
```

---

## TO_CHAR (Number)

**Syntax:** `TO_CHAR( numeric_value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numeric_value` | Numeric | Yes | Number to convert to string |

**Return Type:** String

**Spark SQL Equivalent:** `cast(numeric_value as string)`

**Example:**
```sql
-- Informatica
TO_CHAR(42)           -- returns '42'
TO_CHAR(3.14159)      -- returns '3.14159'
TO_CHAR(NULL)         -- returns NULL

-- Spark SQL
cast(42 as string)           -- returns '42'
cast(3.14159 as string)      -- returns '3.14159'
cast(NULL as string)         -- returns NULL
```

**WHY it matters:** For simple numeric-to-string conversion, a plain `cast` suffices. For formatted output (e.g., `'$1,234.50'`), use `format_number()` in Spark.

---

## TO_DATE

**Syntax:** `TO_DATE( string [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to convert |
| `format` | String | No | Informatica date format mask |

**Return Type:** Date/Time

**Spark SQL Equivalent:** `to_date(string, format)`

**Example:**
```sql
-- Informatica
TO_DATE('2023-12-25', 'YYYY-MM-DD')
TO_DATE('25-DEC-2023', 'DD-MON-YYYY')
TO_DATE('12/25/2023 14:30:00', 'MM/DD/YYYY HH24:MI:SS')

-- Spark SQL
to_date('2023-12-25', 'yyyy-MM-dd')
to_date('25-Dec-2023', 'dd-MMM-yyyy')
to_date('12/25/2023 14:30:00', 'MM/dd/yyyy HH:mm:ss')
```

---

## TO_TIMESTAMP

**Syntax:** `TO_TIMESTAMP( string [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to convert |
| `format` | String | No | Format mask |

**Return Type:** Timestamp

**Spark SQL Equivalent:** `to_timestamp(string, format)`

**Example:**
```sql
-- Informatica
TO_TIMESTAMP('2023-12-25 14:30:00.123', 'YYYY-MM-DD HH24:MI:SS.MS')

-- Spark SQL
to_timestamp('2023-12-25 14:30:00.123', 'yyyy-MM-dd HH:mm:ss.SSS')
```

---

## HEX_TO_INTEGER

**Syntax:** `HEX_TO_INTEGER( hex_string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `hex_string` | String | Yes | Hexadecimal string to convert |

**Return Type:** Integer

**Spark SQL Equivalent:** `conv(hex_string, 16, 10)`

**Example:**
```sql
-- Informatica
HEX_TO_INTEGER('FF')     -- returns 255
HEX_TO_INTEGER('1A3F')   -- returns 6719
HEX_TO_INTEGER('0')      -- returns 0

-- Spark SQL
conv('FF', 16, 10)       -- returns '255' (returns as string!)
cast(conv('FF', 16, 10) as int)  -- returns 255 as integer
```

**WHY it matters:** Spark's `conv` returns a **string**, not an integer. Always wrap with `cast(... as int)` or `cast(... as bigint)` to match Informatica's return type.

---

## CHRCODE

**Syntax:** `CHRCODE( string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Returns ASCII code of first character |

**Return Type:** Integer

**Spark SQL Equivalent:** `ascii(string)`

**Example:**
```sql
-- Informatica
CHRCODE('A')     -- returns 65
CHRCODE('ABC')   -- returns 65 (only first char)
CHRCODE('')      -- returns NULL

-- Spark SQL
ascii('A')       -- returns 65
ascii('ABC')     -- returns 65 (only first char)
ascii('')        -- returns NULL
```

---

## Format Mask Reference

When converting between Informatica and Spark format masks:

| Informatica | Spark | Meaning |
|---|---|---|
| `YYYY` | `yyyy` | 4-digit year |
| `MM` | `MM` | Month 01-12 |
| `DD` | `dd` | Day 01-31 |
| `HH24` | `HH` | Hour 00-23 |
| `HH12` | `hh` | Hour 01-12 |
| `MI` | `mm` | Minutes |
| `SS` | `ss` | Seconds |
| `MS` | `SSS` | Milliseconds |
| `US` | `SSSSSS` | Microseconds |
| `MON` | `MMM` | Month abbreviation (Jan, Feb) |
| `AM/PM` | `a` | AM/PM marker |

---

## Null and Error Handling

| Scenario | Informatica | Spark |
|---|---|---|
| Invalid input string | Returns `NULL` | `try_cast` returns `NULL`; `cast` may throw |
| `NULL` input | Returns `NULL` | Returns `NULL` |
| Overflow | Returns `NULL` | Returns `NULL` or throws |
| Truncation | Silently truncates | Depends on ANSI mode |

**Always use `try_cast` in Spark to match Informatica's safe conversion behavior.**

---

## Key Migration Patterns

### Safe Numeric Conversion with Default
```sql
-- Informatica
NVL(TO_INTEGER(StringCol), 0)

-- Spark SQL
coalesce(try_cast(StringCol as int), 0)
```

### String Date to Timestamp
```sql
-- Informatica
TO_TIMESTAMP(DateString, 'YYYY-MM-DD HH24:MI:SS')

-- Spark SQL
to_timestamp(DateString, 'yyyy-MM-dd HH:mm:ss')
```

### Currency Amount with Scale
```sql
-- Informatica
TO_DECIMAL(AmountString, 2)

-- Spark SQL
cast(AmountString as decimal(18,2))
```

### Hex String to Integer (Common for Surrogate Keys)
```sql
-- Informatica
HEX_TO_INTEGER(HexKey)

-- Spark SQL
cast(conv(HexKey, 16, 10) as bigint)
```
