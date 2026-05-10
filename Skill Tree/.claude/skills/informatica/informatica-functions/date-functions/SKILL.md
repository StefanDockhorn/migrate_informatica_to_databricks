---
name: informatica-date-functions
description: "Use when analyzing or migrating Informatica PowerCenter date functions to Spark SQL. Covers TO_DATE, TO_CHAR, ADD_TO_DATE, DATE_DIFF, TRUNC, ROUND, GET_DATE_PART, LAST_DAY, and more. Includes exact Spark SQL equivalents and syntax differences. Do NOT use for general date handling."
---

# Informatica PowerCenter Date Functions → Spark SQL

## Quick Reference Table

| Informatica Function | Spark SQL Equivalent | Notes |
|---|---|---|
| `TO_DATE(string, format)` | `to_date(string, format)` | Format masks differ |
| `TO_CHAR(date, format)` | `date_format(date, format)` | Format masks differ |
| `ADD_TO_DATE(date, 'MM', 3)` | `add_months(date, 3)` | Unit must be mapped |
| `DATE_DIFF(date1, date2, 'D')` | `datediff(date1, date2)` | Unit must be mapped |
| `GET_DATE_PART(date, 'MONTH')` | `month(date)`, `year(date)`, `day(date)` | One function per part |
| `TRUNC(date, 'MM')` | `trunc(date, 'MM')` | Same pattern, same units |
| `ROUND(date, 'MI')` | `date_trunc('minute', date)` | Spark uses `date_trunc` |
| `LAST_DAY(date)` | `last_day(date)` | Identical |
| `MAKE_DATE_TIME(y,m,d,h,mi,s)` | `make_timestamp(y,m,d,h,mi,s)` | Similar |
| `SYSTIMESTAMP` | `current_timestamp()` | Spark requires parens |
| `SYSDATE` | `current_date()` | Spark requires parens |
| `IS_DATE(string, format)` | `try_cast(string as date)` or regexp | No direct equivalent |

---

## TO_DATE

**Syntax:** `TO_DATE( string [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to convert to date |
| `format` | String | No | Informatica date format mask (default: `MM/DD/YYYY HH24:MI:SS`) |

**Return Type:** Date/Time

**Spark SQL Equivalent:** `to_date(string, format)`

**Example:**
```sql
-- Informatica
TO_DATE('12/25/2023 14:30:00', 'MM/DD/YYYY HH24:MI:SS')

-- Spark SQL
to_date('12/25/2023 14:30:00', 'MM/dd/yyyy HH:mm:ss')
```

**WHY it matters:** Format masks are completely different. Informatica uses `HH24` for 24-hour; Spark uses `HH` (or `kk`). Informatica uses `YYYY`; Spark uses `yyyy`. Always translate format masks character-by-character.

**Format Mask Mapping:**

| Informatica | Spark | Meaning |
|---|---|---|
| `YYYY` | `yyyy` | 4-digit year |
| `YY` | `yy` | 2-digit year |
| `MM` | `MM` | Month (01-12) |
| `MON` | `MMM` | Month abbreviation |
| `DD` | `dd` | Day of month |
| `HH24` | `HH` | Hour 00-23 |
| `HH12` | `hh` | Hour 01-12 |
| `MI` | `mm` | Minutes |
| `SS` | `ss` | Seconds |
| `US` | `SSSSSS` | Microseconds |
| `AM/PM` | `a` | AM/PM marker |
| `DAY` | `EEEE` | Day of week name |
| `D` | `u` | Day of week (1-7) |

---

## TO_CHAR

**Syntax:** `TO_CHAR( date [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date` | Date/Time | Yes | Date value to format |
| `format` | String | No | Output format mask |

**Return Type:** String

**Spark SQL Equivalent:** `date_format(date, format)`

**Example:**
```sql
-- Informatica
TO_CHAR(OrderDate, 'YYYY-MM-DD')

-- Spark SQL
date_format(OrderDate, 'yyyy-MM-dd')
```

**Negative case:** Do NOT use `TO_CHAR` for numeric-to-string conversion in Spark; use `cast(number as string)` instead. `date_format` only works on date/timestamp types.

---

## ADD_TO_DATE

**Syntax:** `ADD_TO_DATE( date, format, value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date` | Date/Time | Yes | Starting date |
| `format` | String | Yes | Unit to add: `YYYY`, `MM`, `DD`, `HH24`, `MI`, `SS`, `MS` |
| `value` | Integer | Yes | Amount to add (negative to subtract) |

**Return Type:** Date/Time

**Spark SQL Equivalent:** Multiple functions depending on unit.

**Example:**
```sql
-- Informatica
ADD_TO_DATE(OrderDate, 'MM', 3)    -- add 3 months
ADD_TO_DATE(OrderDate, 'DD', -7)   -- subtract 7 days
ADD_TO_DATE(OrderDate, 'YYYY', 1)  -- add 1 year
ADD_TO_DATE(OrderDate, 'HH24', 8)  -- add 8 hours

-- Spark SQL
add_months(OrderDate, 3)
date_add(OrderDate, -7)
date_add(OrderDate, 365)  -- or add_months(OrderDate, 12)
date_add(OrderDate, 8)    -- or expr("OrderDate + interval 8 hours")
```

**WHY it matters:** Informatica uses a single function for all date arithmetic; Spark splits by unit. `MS` (millisecond) has no direct Spark equivalent for date types -- use `timestamp` arithmetic instead.

---

## DATE_DIFF

**Syntax:** `DATE_DIFF( date1, date2, format )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date1` | Date/Time | Yes | First date |
| `date2` | Date/Time | Yes | Second date |
| `format` | String | Yes | Unit: `YYYY`, `MM`, `DD`, `HH24`, `MI`, `SS` |

**Return Type:** Integer (or Double for fractional units)

**Spark SQL Equivalent:** `datediff`, `months_between`, or interval arithmetic.

**Example:**
```sql
-- Informatica
DATE_DIFF(ShipDate, OrderDate, 'DD')   -- days difference
DATE_DIFF(ShipDate, OrderDate, 'MM')   -- months difference
DATE_DIFF(ShipDate, OrderDate, 'YYYY') -- years difference

-- Spark SQL
datediff(ShipDate, OrderDate)
months_between(ShipDate, OrderDate)
year(ShipDate) - year(OrderDate)
```

**WHY it matters:** `datediff` in Spark returns integer days only. For months, use `months_between` which returns a decimal (e.g., `1.5` months). For years, extract year components.

---

## GET_DATE_PART

**Syntax:** `GET_DATE_PART( date, format )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date` | Date/Time | Yes | Source date |
| `format` | String | Yes | Part to extract: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `JULIAN_DAY`, `WEEK` |

**Return Type:** Integer

**Spark SQL Equivalent:** Individual extraction functions.

**Example:**
```sql
-- Informatica
GET_DATE_PART(OrderDate, 'MONTH')  -- returns 1-12
GET_DATE_PART(OrderDate, 'YEAR')   -- returns 2023
GET_DATE_PART(OrderDate, 'DAY')    -- returns 1-31
GET_DATE_PART(OrderDate, 'HOUR')   -- returns 0-23
GET_DATE_PART(OrderDate, 'WEEK')   -- returns 1-52

-- Spark SQL
month(OrderDate)
year(OrderDate)
day(OrderDate)
hour(OrderDate)
weekofyear(OrderDate)
```

---

## TRUNC (Date)

**Syntax:** `TRUNC( date [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date` | Date/Time | Yes | Date to truncate |
| `format` | String | No | Precision: `YYYY`, `MM`, `DD`, `HH24`, `MI`, `Q` (default: `DD`) |

**Return Type:** Date/Time

**Spark SQL Equivalent:** `trunc(date, format)`

**Example:**
```sql
-- Informatica
TRUNC(OrderDate, 'MM')     -- truncate to first of month
TRUNC(OrderDate, 'YYYY')   -- truncate to Jan 1st
TRUNC(OrderDate, 'Q')      -- truncate to quarter start
TRUNC(OrderDate, 'WW')     -- truncate to week start (Sunday)

-- Spark SQL
trunc(OrderDate, 'MM')
trunc(OrderDate, 'year')
trunc(OrderDate, 'quarter')
trunc(OrderDate, 'week')
```

**WHY it matters:** Spark `trunc` is identical for `MM`/`month`, but quarter uses `quarter` not `Q`, and year uses `year` not `YYYY`. Week truncation defaults differ -- Informatica `WW` starts on Sunday; Spark `week` starts on Sunday (same).

---

## ROUND (Date)

**Syntax:** `ROUND( date, format )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date` | Date/Time | Yes | Date to round |
| `format` | String | Yes | Precision: `YYYY`, `MM`, `DD`, `HH24`, `MI` |

**Return Type:** Date/Time

**Spark SQL Equivalent:** `date_trunc(format, date)` -- rounds to nearest unit.

**Example:**
```sql
-- Informatica
ROUND(OrderDate, 'YYYY')  -- round to nearest year
ROUND(OrderDate, 'MM')    -- round to nearest month
ROUND(OrderDate, 'DD')    -- round to nearest day

-- Spark SQL
date_trunc('year', OrderDate)
date_trunc('month', OrderDate)
date_trunc('day', OrderDate)
```

**WHY it matters:** There is NO Spark `round()` for dates. Use `date_trunc` instead, which truncates down. For true rounding (e.g., June 15 rounds up to next year), add conditional logic.

---

## LAST_DAY

**Syntax:** `LAST_DAY( date )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `date` | Date/Time | Yes | Date in target month |

**Return Type:** Date

**Spark SQL Equivalent:** `last_day(date)`

**Example:**
```sql
-- Informatica
LAST_DAY(OrderDate)  -- returns last day of the month

-- Spark SQL
last_day(OrderDate)  -- identical behavior
```

---

## MAKE_DATE_TIME

**Syntax:** `MAKE_DATE_TIME( year, month, day, hour, minute, second [, nanosecond] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `year` | Integer | Yes | Year component |
| `month` | Integer | Yes | Month (1-12) |
| `day` | Integer | Yes | Day (1-31) |
| `hour` | Integer | Yes | Hour (0-23) |
| `minute` | Integer | Yes | Minute (0-59) |
| `second` | Integer | Yes | Second (0-59) |
| `nanosecond` | Integer | No | Nanoseconds |

**Return Type:** Timestamp

**Spark SQL Equivalent:** `make_timestamp(year, month, day, hour, min, sec)`

**Example:**
```sql
-- Informatica
MAKE_DATE_TIME(2023, 12, 25, 14, 30, 0)

-- Spark SQL
make_timestamp(2023, 12, 25, 14, 30, 0)
```

---

## SYSDATE / SYSTIMESTAMP

**Syntax:** `SYSDATE` (no parentheses) / `SYSTIMESTAMP` (no parentheses)

**Return Type:** Date (`SYSDATE`) / Timestamp (`SYSTIMESTAMP`)

**Spark SQL Equivalent:**
```sql
-- Informatica
SYSDATE        -- current date
SYSTIMESTAMP   -- current timestamp

-- Spark SQL
current_date()       -- requires parentheses
current_timestamp()  -- requires parentheses
```

**WHY it matters:** Informatica functions do NOT use parentheses; Spark requires them. This is a common migration error.

---

## IS_DATE

**Syntax:** `IS_DATE( string [, format] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Value to test |
| `format` | String | No | Expected format mask |

**Return Type:** Integer (1 = true, 0 = false)

**Spark SQL Equivalent:** No direct equivalent. Use `try_cast` or regex.

**Example:**
```sql
-- Informatica
IS_DATE('2023-12-25', 'YYYY-MM-DD')  -- returns 1
IS_DATE('not-a-date', 'YYYY-MM-DD')  -- returns 0

-- Spark SQL (workaround)
CASE WHEN try_cast('2023-12-25' AS DATE) IS NOT NULL THEN 1 ELSE 0 END
CASE WHEN '2023-12-25' RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END
```

---

## Key Migration Patterns

### Date Arithmetic Chain
```sql
-- Informatica: add 3 months then truncate to month start
TRUNC(ADD_TO_DATE(OrderDate, 'MM', 3), 'MM')

-- Spark SQL
trunc(add_months(OrderDate, 3), 'MM')
```

### Extract Year-Month as String
```sql
-- Informatica
TO_CHAR(OrderDate, 'YYYY-MM')

-- Spark SQL
date_format(OrderDate, 'yyyy-MM')
```

### Date Difference in Months
```sql
-- Informatica
DATE_DIFF(ShipDate, OrderDate, 'MM')

-- Spark SQL (returns decimal)
months_between(ShipDate, OrderDate)
```

### Filter Current Year
```sql
-- Informatica
GET_DATE_PART(OrderDate, 'YEAR') = GET_DATE_PART(SYSDATE, 'YEAR')

-- Spark SQL
year(OrderDate) = year(current_date())
```
