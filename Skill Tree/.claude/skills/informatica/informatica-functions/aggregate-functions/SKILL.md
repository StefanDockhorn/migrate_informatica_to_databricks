---
name: informatica-aggregate-functions
description: "Use when analyzing or migrating Informatica PowerCenter aggregate functions to Spark SQL. Covers SUM, AVG, COUNT, MAX, MIN, FIRST, LAST, COUNT DISTINCT, and nested aggregate restrictions. Includes exact Spark SQL equivalents and window function alternatives. Do NOT use for general aggregation patterns."
---

# Informatica PowerCenter Aggregate Functions → Spark SQL

## Quick Reference Table

| Informatica Function | Spark SQL Equivalent | Notes |
|---|---|---|
| `SUM(column)` | `sum(column)` | Ignores NULLs |
| `AVG(column)` | `avg(column)` | Ignores NULLs |
| `COUNT(column)` | `count(column)` | Ignores NULLs |
| `COUNT(*)` | `count(*)` | Counts all rows |
| `COUNT(DISTINCT col)` | `count(DISTINCT col)` | Same syntax |
| `MAX(column)` | `max(column)` | Ignores NULLs |
| `MIN(column)` | `min(column)` | Ignores NULLs |
| `FIRST(column)` | `first(column)` | Requires sorted window in Spark |
| `LAST(column)` | `last(column)` | Requires sorted window in Spark |
| `MEDIAN(column)` | `percentile_approx(column, 0.5)` | Approximate in Spark |
| `PERCENTILE(col, val)` | `percentile_approx(col, val)` | Approximate in Spark |
| `STDDEV(column)` | `stddev(column)` | Sample stddev |
| `VARIANCE(column)` | `variance(column)` | Sample variance |
| `LISTAGG` (not native) | `collect_list(column)` | Returns array; use concat_ws to stringify |

---

## SUM

**Syntax:** `SUM( column )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Numeric | Yes | Column to sum |

**Return Type:** Same as input (or wider to prevent overflow)

**Spark SQL Equivalent:** `sum(column)`

**Example:**
```sql
-- Informatica
SUM(Revenue)

-- Spark SQL
sum(Revenue)

-- With GROUP BY
SELECT Region, sum(Revenue) as TotalRevenue
FROM Sales
GROUP BY Region
```

**WHY it matters:** Both ignore `NULL` values in the column. `SUM` of all `NULL`s returns `NULL` (not 0) in both platforms.

---

## AVG

**Syntax:** `AVG( column )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Numeric | Yes | Column to average |

**Return Type:** Double (or Decimal if input is Decimal)

**Spark SQL Equivalent:** `avg(column)`

**Example:**
```sql
-- Informatica
AVG(Salary)

-- Spark SQL
avg(Salary)

-- With GROUP BY
SELECT Department, avg(Salary) as AvgSalary
FROM Employees
GROUP BY Department
```

**WHY it matters:** `AVG` divides by the count of **non-NULL** values. If 5 rows exist but 2 have NULL salaries, `AVG` divides by 3, not 5. Use `coalesce(column, 0)` before averaging if NULLs should count as zeros.

---

## COUNT

**Syntax:** `COUNT( column )` or `COUNT(*)` or `COUNT(DISTINCT column)`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Any | Yes | Column to count (optional for `*`) |
| `DISTINCT` | Keyword | No | Count unique values only |

**Return Type:** Long

**Spark SQL Equivalent:** `count(column)`, `count(*)`, `count(DISTINCT column)`

**Example:**
```sql
-- Informatica
COUNT(*)                     -- all rows
COUNT(EmployeeID)            -- non-NULL EmployeeIDs
COUNT(DISTINCT Region)       -- unique regions

-- Spark SQL
count(*)                     -- all rows
count(EmployeeID)            -- non-NULL EmployeeIDs
count(DISTINCT Region)       -- unique regions
```

**WHY it matters:** `COUNT(*)` counts every row including those with all-NULL values. `COUNT(column)` excludes rows where that column is `NULL`. This distinction is critical for accurate row counting.

---

## MAX / MIN

**Syntax:** `MAX( column )` / `MIN( column )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Comparable | Yes | Column to find max/min of |

**Return Type:** Same as input

**Spark SQL Equivalent:** `max(column)` / `min(column)`

**Example:**
```sql
-- Informatica
MAX(OrderDate)
MIN(Salary)

-- Spark SQL
max(OrderDate)
min(Salary)
```

**WHY it matters:** Both work on any comparable type (numeric, string, date). Both ignore `NULL` values. For strings, comparison is lexicographic.

---

## FIRST / LAST

**Syntax:** `FIRST( column )` / `LAST( column )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Any | Yes | Column to return |

**Return Type:** Same as input

**Spark SQL Equivalent:** `first(column)` / `last(column)` with ordered window

**Example:**
```sql
-- Informatica (requires sorted input in Aggregator)
FIRST(OrderDate)     -- first row in sorted group
LAST(OrderDate)      -- last row in sorted group

-- Spark SQL (requires explicit window ordering)
first(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate)
last(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

-- As aggregate with window frame
SELECT
    CustomerID,
    first(OrderDate) OVER (
        PARTITION BY CustomerID
        ORDER BY OrderDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as FirstOrder,
    last(OrderDate) OVER (
        PARTITION BY CustomerID
        ORDER BY OrderDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as LastOrder
FROM Orders
```

**WHY it matters:** Informatica's `FIRST`/`LAST` depend on sorted input in the Aggregator transformation. In Spark, you **must** specify an `ORDER BY` in the window definition. Without an explicit frame, `last()` may not return the expected row -- use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` to get the true last row.

---

## MEDIAN / PERCENTILE

**Syntax:** `MEDIAN( column )` / `PERCENTILE( column, value )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Numeric | Yes | Column to compute percentile of |
| `value` | Double | Yes (PERCENTILE only) | Percentile to compute (0.0 to 1.0) |

**Return Type:** Double

**Spark SQL Equivalent:** `percentile_approx(column, 0.5)` / `percentile_approx(column, value [, accuracy])`

**Example:**
```sql
-- Informatica
MEDIAN(Salary)                    -- median salary
PERCENTILE(Salary, 0.9)           -- 90th percentile
PERCENTILE(Salary, 0.25)          -- 25th percentile (Q1)

-- Spark SQL
percentile_approx(Salary, 0.5)            -- median
percentile_approx(Salary, 0.9)            -- 90th percentile
percentile_approx(Salary, 0.25)           -- Q1
percentile_approx(Salary, 0.5, 10000)     -- higher accuracy (default 10000)
```

**WHY it matters:** Spark's `percentile_approx` uses an approximate algorithm (T-Digest) for performance. For exact percentiles, use `percentile(column, 0.5)` which requires shuffling all data and is much slower. Informatica's `MEDIAN` may compute an exact value; verify accuracy requirements before migration.

---

## STDDEV / VARIANCE

**Syntax:** `STDDEV( column )` / `VARIANCE( column )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `column` | Numeric | Yes | Column to compute statistics on |

**Return Type:** Double

**Spark SQL Equivalent:** `stddev(column)` / `variance(column)` (sample) or `stddev_pop()` / `var_pop()` (population)

**Example:**
```sql
-- Informatica
STDDEV(Salary)
VARIANCE(Salary)

-- Spark SQL (sample statistics - default)
stddev(Salary)        -- sample standard deviation
variance(Salary)      -- sample variance

-- Population statistics
stddev_pop(Salary)    -- population standard deviation
var_pop(Salary)       -- population variance
```

**WHY it matters:** Informatica `STDDEV` computes sample standard deviation (divides by N-1). Spark's `stddev` also computes sample stddev. Use `stddev_pop`/`var_pop` for population statistics (divides by N).

---

## collect_list (LISTAGG Equivalent)

Informatica does not have a native LISTAGG function (requires Custom transformation or Java). Spark's `collect_list`/`collect_set` provide similar functionality.

**Spark SQL Equivalent:**
```sql
-- Spark SQL: collect values into an array
collect_list(ProductName)           -- array with duplicates
collect_set(ProductName)            -- array with unique values only

-- Convert array to concatenated string
concat_ws(', ', collect_list(ProductName))   -- comma-separated string
```

**Example:**
```sql
-- Spark SQL: list of products per customer
SELECT
    CustomerID,
    concat_ws(', ', collect_list(ProductName)) as Products,
    size(collect_list(ProductName)) as ProductCount
FROM Orders
GROUP BY CustomerID
```

---

## Critical Rules

### Nested Aggregates
Informatica does NOT support nested aggregate functions: `MAX(SUM(column))` is invalid. Spark has the same restriction.

```sql
-- INVALID in both Informatica and Spark
MAX(SUM(Revenue))

-- Workaround in both: use subquery/CTE
SELECT MAX(TotalRevenue) FROM (
    SELECT Region, SUM(Revenue) as TotalRevenue
    FROM Sales
    GROUP BY Region
) subq
```

### NULL Handling
| Function | NULL Behavior |
|---|---|
| `SUM` | Ignores NULLs; returns NULL if all NULL |
| `AVG` | Ignores NULLs; returns NULL if all NULL |
| `COUNT(*)` | Counts all rows including NULLs |
| `COUNT(column)` | Excludes NULLs |
| `MAX`/`MIN` | Ignores NULLs; returns NULL if all NULL |
| `FIRST`/`LAST` | Depends on window frame |

### Aggregator Transformation vs SQL GROUP BY
Informatica's Aggregator transformation processes sorted or unsorted data. Spark's `GROUP BY` is a SQL construct. Key differences:
- Informatica Aggregator can output multiple aggregate rows per group with `FIRST`/`LAST`
- Spark `GROUP BY` produces exactly one row per group
- Use Spark window functions for per-row aggregates within a group

---

## Key Migration Patterns

### Row Count with NULL Handling
```sql
-- Informatica: count all rows
COUNT(*)

-- Spark SQL
count(*)
```

### Distinct Count of Multiple Columns
```sql
-- Informatica (concatenate then count distinct)
COUNT(DISTINCT FirstName || '|' || LastName)

-- Spark SQL (concatenate then count distinct, or use struct)
count(DISTINCT concat(FirstName, '|', LastName))
count(DISTINCT named_struct('f', FirstName, 'l', LastName))
```

### Running Total (Cumulative Sum)
```sql
-- Informatica: CUME() in sorted Aggregator
CUME()

-- Spark SQL: window function
sum(Revenue) OVER (
    PARTITION BY Region
    ORDER BY OrderDate
    ROWS UNBOUNDED PRECEDING
) as RunningTotal
```

### First and Last Order per Customer
```sql
-- Informatica: Aggregator with FIRST/LAST, sorted by OrderDate
FIRST(OrderDate) as FirstOrder
LAST(OrderDate) as LastOrder

-- Spark SQL: window functions
SELECT DISTINCT
    CustomerID,
    first(OrderDate) OVER w as FirstOrder,
    last(OrderDate) OVER w as LastOrder
FROM Orders
WINDOW w AS (PARTITION BY CustomerID ORDER BY OrderDate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

### Aggregate with FILTER (Conditional Aggregation)
```sql
-- Informatica: IIF inside aggregate
SUM(IIF(Status = 'Active', Amount, 0))

-- Spark SQL
sum(CASE WHEN Status = 'Active' THEN Amount ELSE 0 END)
-- or Spark 3.0+:
sum(Amount) FILTER(WHERE Status = 'Active')
```
