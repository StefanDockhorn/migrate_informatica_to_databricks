---
name: informatica-numeric-functions
description: "Use when analyzing or migrating Informatica PowerCenter numeric functions to Spark SQL. Covers ABS, MOD, POWER, SQRT, SIGN, ROUND, TRUNC (numeric), CEIL, FLOOR, and more. Includes exact Spark SQL equivalents. Do NOT use for general numeric computation."
---

# Informatica PowerCenter Numeric Functions → Spark SQL

## Quick Reference Table

| Informatica Function | Spark SQL Equivalent | Notes |
|---|---|---|
| `ABS(number)` | `abs(number)` | Identical |
| `MOD(num, denom)` | `mod(num, denom)` or `num % denom` | Identical |
| `POWER(base, exp)` | `power(base, exp)` or `pow(base, exp)` | Identical |
| `SQRT(number)` | `sqrt(number)` | Identical |
| `SIGN(number)` | `sign(number)` | Returns -1, 0, 1 |
| `ROUND(num, prec)` | `round(num, prec)` | Identical |
| `TRUNC(num, prec)` | `trunc(num, prec)` or `bround(num, prec)` | Banker's rounding option |
| `CEIL(number)` | `ceil(number)` | Identical |
| `FLOOR(number)` | `floor(number)` | Identical |
| `EXP(exponent)` | `exp(exponent)` | Identical |
| `LN(number)` | `log(number)` or `ln(number)` | Natural log |
| `LOG(number)` | `log10(number)` | Base-10 log |
| `GREATEST(v1, v2...)` | `greatest(v1, v2...)` | Identical |
| `LEAST(v1, v2...)` | `least(v1, v2...)` | Identical |
| `CUME()` | `sum() OVER (ORDER BY ...)` | Cumulative sum |
| `MOVINGSUM(col, s, e)` | `sum() OVER (ROWS BETWEEN s AND e)` | Sliding window |
| `MOVINGAVG(col, s, e)` | `avg() OVER (ROWS BETWEEN s AND e)` | Sliding window |

---

## ABS

**Syntax:** `ABS( numeric_value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numeric_value` | Numeric | Yes | Value to get absolute value of |

**Return Type:** Same as input (Integer → Integer, Decimal → Decimal)

**Spark SQL Equivalent:** `abs(numeric_value)`

**Example:**
```sql
-- Informatica
ABS(-42)          -- returns 42
ABS(42)           -- returns 42
ABS(-3.14)        -- returns 3.14
ABS(NULL)         -- returns NULL

-- Spark SQL
abs(-42)          -- returns 42
abs(42)           -- returns 42
abs(-3.14)        -- returns 3.14
abs(NULL)         -- returns NULL
```

---

## MOD

**Syntax:** `MOD( numerator, denominator )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numerator` | Numeric | Yes | Dividend |
| `denominator` | Numeric | Yes | Divisor (cannot be zero) |

**Return Type:** Same as input types

**Spark SQL Equivalent:** `mod(numerator, denominator)` or `numerator % denominator`

**Example:**
```sql
-- Informatica
MOD(17, 5)        -- returns 2
MOD(17, 0)        -- returns NULL (division by zero)
MOD(-17, 5)       -- returns -2
MOD(17, -5)       -- returns 2

-- Spark SQL
mod(17, 5)        -- returns 2
17 % 5            -- returns 2
mod(17, 0)        -- returns NULL
mod(-17, 5)       -- returns -2
```

**WHY it matters:** When the numerator is negative, the result has the same sign as the numerator in both platforms. Division by zero returns `NULL` in both (not an error).

---

## POWER

**Syntax:** `POWER( base, exponent )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `base` | Numeric | Yes | Base value |
| `exponent` | Numeric | Yes | Exponent value |

**Return Type:** Double (or higher precision if inputs are decimal)

**Spark SQL Equivalent:** `power(base, exponent)` or `pow(base, exponent)`

**Example:**
```sql
-- Informatica
POWER(2, 3)       -- returns 8
POWER(10, -1)     -- returns 0.1
POWER(4, 0.5)     -- returns 2.0 (square root)

-- Spark SQL
power(2, 3)       -- returns 8
pow(2, 3)         -- returns 8 (alias)
power(10, -1)     -- returns 0.1
power(4, 0.5)     -- returns 2.0
```

---

## SQRT

**Syntax:** `SQRT( numeric_value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numeric_value` | Numeric | Yes | Non-negative value |

**Return Type:** Double

**Spark SQL Equivalent:** `sqrt(numeric_value)`

**Example:**
```sql
-- Informatica
SQRT(16)          -- returns 4.0
SQRT(2)           -- returns 1.414...
SQRT(-1)          -- returns NULL (not a number)

-- Spark SQL
sqrt(16)          -- returns 4.0
sqrt(2)           -- returns 1.414...
sqrt(-1)          -- returns NULL
```

---

## SIGN

**Syntax:** `SIGN( numeric_value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numeric_value` | Numeric | Yes | Value to test |

**Return Type:** Integer (-1, 0, or 1)

**Spark SQL Equivalent:** `sign(numeric_value)`

**Example:**
```sql
-- Informatica
SIGN(-42)         -- returns -1
SIGN(0)           -- returns 0
SIGN(42)          -- returns 1
SIGN(NULL)        -- returns NULL

-- Spark SQL
sign(-42)         -- returns -1.0
sign(0)           -- returns 0.0
sign(42)          -- returns 1.0
sign(NULL)        -- returns NULL
```

**WHY it matters:** Spark returns `Double` (-1.0, 0.0, 1.0) while Informatica returns `Integer` (-1, 0, 1). Use `cast(sign(x) as int)` if an integer result is required downstream.

---

## ROUND

**Syntax:** `ROUND( numeric_value [, precision] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numeric_value` | Numeric | Yes | Value to round |
| `precision` | Integer | No | Decimal places (default: 0, negative rounds to tens/hundreds) |

**Return Type:** Same as input type

**Spark SQL Equivalent:** `round(numeric_value, precision)`

**Example:**
```sql
-- Informatica
ROUND(3.14159, 2)     -- returns 3.14
ROUND(1234.5, -2)     -- returns 1200
ROUND(2.5)            -- returns 3 (half-up rounding)
ROUND(3.5)            -- returns 4

-- Spark SQL
round(3.14159, 2)     -- returns 3.14
round(1234.5, -2)     -- returns 1200
round(2.5)            -- returns 3 (half-up rounding)
round(3.5)            -- returns 4
```

**WHY it matters:** Both use "half-up" rounding by default. Spark also offers `bround()` for "banker's rounding" (round half to even).

---

## TRUNC (Numeric)

**Syntax:** `TRUNC( numeric_value [, precision] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `numeric_value` | Numeric | Yes | Value to truncate |
| `precision` | Integer | No | Decimal places (default: 0) |

**Return Type:** Same as input type

**Spark SQL Equivalent:** `trunc(numeric_value, precision)`

**Example:**
```sql
-- Informatica
TRUNC(3.14159, 2)     -- returns 3.14 (not rounded!)
TRUNC(3.99999, 0)     -- returns 3 (always toward zero)
TRUNC(-3.99999, 0)    -- returns -3
TRUNC(1234.5, -2)     -- returns 1200

-- Spark SQL
trunc(3.14159, 2)     -- returns 3.14
trunc(3.99999, 0)     -- returns 3
trunc(-3.99999, 0)    -- returns -3
trunc(1234.5, -2)     -- returns 1200
```

**WHY it matters:** `TRUNC` always cuts off digits without rounding. `TRUNC(3.9) = 3`, while `ROUND(3.9) = 4`. This distinction is critical for financial calculations.

---

## CEIL / FLOOR

**Syntax:** `CEIL( numeric_value )` / `FLOOR( numeric_value )`

**Return Type:** Integer or Long

**Spark SQL Equivalent:** `ceil(numeric_value)` / `floor(numeric_value)`

**Example:**
```sql
-- Informatica
CEIL(3.2)         -- returns 4
CEIL(-3.2)        -- returns -3
FLOOR(3.8)        -- returns 3
FLOOR(-3.8)       -- returns -4

-- Spark SQL
ceil(3.2)         -- returns 4
ceil(-3.2)        -- returns -3
floor(3.8)        -- returns 3
floor(-3.8)       -- returns -4
```

---

## EXP / LN / LOG

**Syntax:** `EXP(exponent)` / `LN(number)` / `LOG(number)`

**Return Type:** Double

**Spark SQL Equivalent:**

| Informatica | Spark | Notes |
|---|---|---|
| `EXP(x)` | `exp(x)` | e^x |
| `LN(x)` | `log(x)` or `ln(x)` | Natural log (base e) |
| `LOG(x)` | `log10(x)` | Base-10 log |

**Example:**
```sql
-- Informatica
EXP(1)            -- returns 2.718... (e)
LN(10)            -- returns 2.302... (natural log)
LOG(100)          -- returns 2.0 (base-10 log)

-- Spark SQL
exp(1)            -- returns 2.718...
log(10)           -- returns 2.302... (natural log)
ln(10)            -- returns 2.302... (alias)
log10(100)        -- returns 2.0 (base-10)
```

**WHY it matters:** Spark's `log` is natural log (same as `ln`), while Informatica's `LOG` is base-10. This is a critical difference. Always map `LOG` → `log10`, `LN` → `log`/`ln`.

---

## GREATEST / LEAST

**Syntax:** `GREATEST( value1, value2 [, ...] )` / `LEAST( value1, value2 [, ...] )`

**Arguments:** Two or more comparable values of the same type.

**Return Type:** Same as input type

**Spark SQL Equivalent:** `greatest(value1, value2, ...)` / `least(value1, value2, ...)`

**Example:**
```sql
-- Informatica
GREATEST(10, 20, 5)     -- returns 20
LEAST(10, 20, 5)        -- returns 5
GREATEST(Salary, 50000) -- returns higher of two

-- Spark SQL
greatest(10, 20, 5)     -- returns 20
least(10, 20, 5)        -- returns 5
greatest(Salary, 50000) -- returns higher of two
```

**WHY it matters:** `NULL` handling differs. In both platforms, if any argument is `NULL`, the result is `NULL`. Use `coalesce` to provide defaults if needed.

---

## CUME (Cumulative)

**Syntax:** `CUME()` -- used in Aggregator transformation with group-by.

**Return Type:** Numeric

**Spark SQL Equivalent:** `sum(column) OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)`

**Example:**
```sql
-- Informatica (in Aggregator, sorted by OrderDate)
CUME()  -- cumulative sum of the aggregated column

-- Spark SQL
sum(Revenue) OVER (
    PARTITION BY Region
    ORDER BY OrderDate
    ROWS UNBOUNDED PRECEDING
) as CumulativeRevenue
```

**WHY it matters:** Informatica's `CUME()` is used inside an Aggregator with sorted input. In Spark, always use window functions with explicit `PARTITION BY` and `ORDER BY`.

---

## MOVINGSUM / MOVINGAVG

**Syntax:** `MOVINGSUM( column, start_row, end_row )` / `MOVINGAVG( column, start_row, end_row )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Numeric | Yes | Column to aggregate |
| `start_row` | Integer | Yes | Row offset for window start (0 = current) |
| `end_row` | Integer | Yes | Row offset for window end |

**Return Type:** Numeric

**Spark SQL Equivalent:** `sum/avg() OVER (ROWS BETWEEN start AND end)`

**Example:**
```sql
-- Informatica
MOVINGSUM(Revenue, -2, 0)   -- sum of current row + 2 preceding
MOVINGAVG(Revenue, -4, 0)   -- 5-row moving average

-- Spark SQL
sum(Revenue) OVER (
    ORDER BY DateCol
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) as MovingSum3

avg(Revenue) OVER (
    ORDER BY DateCol
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
) as MovingAvg5
```

**WHY it matters:** Informatica row offsets are relative to current row (0 = current). Spark uses `PRECEDING`/`FOLLOWING` keywords. Map `-2, 0` → `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`.

---

## Key Migration Patterns

### Safe Division (Avoid Divide-by-Zero)
```sql
-- Informatica
IIF(Denominator = 0, NULL, Numerator / Denominator)

-- Spark SQL
CASE WHEN Denominator = 0 THEN NULL ELSE Numerator / Denominator END
-- or:
nullif(Denominator, 0)  -- returns NULL if zero, used as divisor
```

### Round to 2 Decimal Places for Currency
```sql
-- Informatica
ROUND(Price * Quantity, 2)

-- Spark SQL
round(Price * Quantity, 2)
```

### Percentage Calculation with NULL Handling
```sql
-- Informatica
IIF(Total = 0 OR ISNULL(Total), 0, ROUND(Part / Total * 100, 2))

-- Spark SQL
CASE WHEN Total = 0 OR Total IS NULL THEN 0 ELSE round(Part / Total * 100, 2) END
```

### Moving Average with NULL Handling
```sql
-- Informatica
MOVINGAVG(NVL(Revenue, 0), -2, 0)

-- Spark SQL
avg(coalesce(Revenue, 0)) OVER (
    ORDER BY DateCol
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)
```
