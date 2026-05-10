---
name: informatica-to-databricks-function-mapping
description: "Use when mapping Informatica PowerCenter functions to Spark SQL equivalents. Covers complete function equivalence table across date, string, numeric, conversion, conditional, and aggregate categories with syntax differences and common migration patterns. Do NOT use for general SQL function reference or transformation-level mapping."
---

# Function Mapping: Informatica PowerCenter → Spark SQL

## Conditional Functions

| Informatica | Spark SQL | Syntax Example | Notes |
|---|---|---|---|
| `IIF(cond, true_val, false_val)` | `CASE WHEN` / `if()` | `if(col > 0, "pos", "neg")` | `if()` only supports 2 branches; nested for 3+ |
| `IIF(cond, val, NULL)` | `when().otherwise()` | `when(col > 0, "pos")` | Chain `.when()` for multi-branch logic |
| `DECODE(col, a, 1, b, 2, 0)` | `CASE WHEN` chain | `CASE WHEN col='a' THEN 1 WHEN col='b' THEN 2 ELSE 0 END` | No direct `DECODE`; always rewrite to `CASE` |
| `ISNULL(col)` | `col IS NULL` | `col.isNull()` | Or `isnull(col)` in SQL mode |
| `IS_SPACES(col)` | `length(trim(col)) = 0` | No built-in equivalent | Must trim and check length |
| `IS_DATE(col, format)` | `try_cast(col as date)` | `to_date(col, 'yyyy-MM-dd')` | Use `try_to_date()` (DBR 13+) for null-on-fail |
| `IS_NUMBER(col)` | `try_cast(col as decimal)` | `rlike(col, '^[0-9]+\\.?[0-9]*$')` | Regex or try_cast approach |
| `NVL(col, 0)` | `coalesce(col, 0)` | `coalesce(col, lit(0))` | Same semantics; use `lit()` for constants |
| `NVL2(col, not_null_val, null_val)` | `CASE WHEN col IS NOT NULL` | `when(col.isNotNull(), val1).otherwise(val2)` | No direct equivalent |

## Date Functions

| Informatica | Spark SQL | Syntax Example | Notes |
|---|---|---|---|
| `SYSDATE` | `current_date()` | `current_date()` | Use `current_timestamp()` for full datetime |
| `TRUNC(date)` | `trunc(date, 'MONTH')` | `date_trunc('MONTH', col)` | Specify truncation unit |
| `ADD_TO_DATE(date, 'MM', 3)` | `date_add()` / `add_months()` | `add_months(col, 3)` | Use `date_add(col, n)` for days |
| `DATE_DIFF(date1, date2)` | `datediff(date1, date2)` | `datediff(end_date, start_date)` | Returns int days |
| `GET_DATE_PART(date, 'MONTH')` | `month()` / `date_format()` | `month(col)`, `date_format(col, 'MM')` | Multiple extract functions available |
| `LAST_DAY(date)` | `last_day(date)` | `last_day(col)` | Direct equivalent |
| `NEXT_DAY(date, 'MONDAY')` | `next_day(date, 'Mon')` | `next_day(col, 'Mon')` | Abbreviated day name |
| `ROUND(date, 'MM')` | `date_trunc('MONTH', date)` | `date_trunc('MONTH', col)` | Truncation, not rounding |
| `TO_CHAR(date, 'YYYY-MM-DD')` | `date_format(date, 'yyyy-MM-dd')` | `date_format(col, 'yyyy-MM-dd')` | Java SimpleDateFormat patterns |
| `TO_DATE(string, 'YYYY-MM-DD')` | `to_date(string, 'yyyy-MM-dd')` | `to_date(col, 'yyyy-MM-dd')` | Returns null on bad format |
| `GET_TIMEZONE` | No direct equivalent | Use session timezone | Spark has `current_timezone()` (DBR 12+) |

## String Functions

| Informatica | Spark SQL | Syntax Example | Notes |
|---|---|---|---|
| `SUBSTR(col, 1, 5)` | `substring(col, 1, 5)` | `substring(col, 1, 5)` | Both are 1-based indexing |
| `INSTR(col, 'A')` | `instr(col, 'A')` | `instr(col, 'A')` | Returns position or 0. No occurrence param |
| `INSTR(col, 'A', 1, 2)` | No direct equivalent | `locate('A', col, 2)` then chain | For nth occurrence, use regex or UDF |
| `LPAD(col, 10, '0')` | `lpad(col, 10, '0')` | `lpad(col, 10, '0')` | Direct equivalent |
| `RPAD(col, 10, '0')` | `rpad(col, 10, '0')` | `rpad(col, 10, '0')` | Direct equivalent |
| `LTRIM(col)` | `ltrim(col)` | `ltrim(col)` | Default trims spaces |
| `LTRIM(col, 'x')` | `regexp_replace(col, '^x+', '')` | No built-in char set trim | Must use regex for custom trim set |
| `RTRIM(col, 'x')` | `regexp_replace(col, 'x+$', '')` | No built-in char set trim | Must use regex for custom trim set |
| `TRIM(col)` | `trim(col)` | `trim(col)` | Direct equivalent |
| `REPLACECHR(1, col, 'A', 'B')` | `replace(col, 'A', 'B')` | `replace(col, 'A', 'B')` | Direct equivalent |
| `REPLACESTR(1, col, 'ABC', 'XYZ')` | `replace(col, 'ABC', 'XYZ')` | `replace(col, 'ABC', 'XYZ')` | Direct equivalent |
| `LENGTH(col)` | `length(col)` | `length(col)` | Direct equivalent |
| `UPPER(col)` | `upper(col)` | `upper(col)` | Direct equivalent |
| `LOWER(col)` | `lower(col)` | `lower(col)` | Direct equivalent |
| `INITCAP(col)` | `initcap(col)` | `initcap(col)` | Direct equivalent |
| `LTRIM(RTRIM(col))` | `trim(col)` | `trim(col)` | Prefer single `trim()` |
| `CHR(65)` | `char(65)` | `char(65)` | Direct equivalent |
| `ASCII('A')` | `ascii('A')` | `ascii(col)` | Direct equivalent |
| `REVERSE(col)` | `reverse(col)` | `reverse(col)` | Direct equivalent |
| `CONCAT(col1, col2, col3)` | `concat(col1, col2, col3)` | `concat(col1, col2, col3)` | Or `col1 || col2` operator |
| `SOUNDEX(col)` | `soundex(col)` | `soundex(col)` | Direct equivalent |
| `METAPHONE(col)` | No built-in | Use third-party library | Phonetic matching not native |
| `REG_EXTRACT(col, '([0-9]+)', 1)` | `regexp_extract(col, '([0-9]+)', 1)` | `regexp_extract(col, r'([0-9]+)', 1)` | 0 = full match, 1+ = groups |
| `REG_MATCH(col, '[0-9]+')` | `rlike(col, '[0-9]+')` | `col.rlike('[0-9]+')` | Returns boolean |
| `REG_REPLACE(col, '[0-9]', '#')` | `regexp_replace(col, '[0-9]', '#')` | `regexp_replace(col, r'[0-9]', '#')` | Direct equivalent |

## Numeric Functions

| Informatica | Spark SQL | Syntax Example | Notes |
|---|---|---|---|
| `ABS(col)` | `abs(col)` | `abs(col)` | Direct equivalent |
| `CEIL(col)` | `ceil(col)` | `ceil(col)` | Direct equivalent |
| `FLOOR(col)` | `floor(col)` | `floor(col)` | Direct equivalent |
| `ROUND(col, 2)` | `round(col, 2)` | `round(col, 2)` | Direct equivalent |
| `TRUNC(col, 2)` | `trunc(col, 2)` | `trunc(col, 2)` | Truncate toward zero |
| `POWER(col, 2)` | `pow(col, 2)` | `pow(col, 2)` | Direct equivalent |
| `MOD(col, 10)` | `col % 10` | `col % 10` | Or `pmod(col, 10)` for positive mod |
| `SQRT(col)` | `sqrt(col)` | `sqrt(col)` | Direct equivalent |
| `SIGN(col)` | `signum(col)` | `signum(col)` | Returns -1, 0, or 1 |
| `GREATEST(col1, col2)` | `greatest(col1, col2)` | `greatest(col1, col2)` | Direct equivalent |
| `LEAST(col1, col2)` | `least(col1, col2)` | `least(col1, col2)` | Direct equivalent |
| `LN(col)` | `ln(col)` | `ln(col)` | Direct equivalent |
| `LOG(col)` | `log10(col)` | `log10(col)` | Log base 10 |
| `EXP(col)` | `exp(col)` | `exp(col)` | Direct equivalent |
| `SIN/COS/TAN(col)` | `sin(col)` / `cos(col)` / `tan(col)` | `sin(col)` | Direct equivalents |
| `PI()` | `pi()` | `pi()` | Direct equivalent |

## Conversion Functions

| Informatica | Spark SQL | Syntax Example | Notes |
|---|---|---|---|
| `TO_CHAR(number)` | `cast(col as string)` | `col.cast("string")` | Or `format_number()` for formatting |
| `TO_DATE(string, format)` | `to_date(string, format)` | `to_date(col, 'yyyy-MM-dd')` | Returns null on failure |
| `TO_DECIMAL(string)` | `cast(col as decimal(10,2))` | `col.cast("decimal(10,2)")` | Specify precision/scale |
| `TO_FLOAT(string)` | `cast(col as float)` | `col.cast("float")` | Or `col.cast("double")` |
| `TO_INTEGER(string)` | `cast(col as int)` | `col.cast("int")` | Truncates decimal |
| `HEX_TO_BINARY(col)` | `unhex(col)` | `unhex(col)` | Converts hex string to binary |
| `BINARY_TO_HEX(col)` | `hex(col)` | `hex(col)` | Converts binary to hex string |
| `TO_BIG_ENDIAN(col)` | No direct equivalent | Use `conv()` + string padding | Manual byte manipulation |
| `MBCS_TO_CHAR(col)` | `decode(col, 'UTF-8')` | `decode(col, 'UTF-8')` | Encoding conversion |

## Aggregate Functions

| Informatica | Spark SQL | Syntax Example | Notes |
|---|---|---|---|
| `SUM(col)` | `sum(col)` | `sum(col)` | Direct equivalent |
| `AVG(col)` | `avg(col)` | `avg(col)` | Returns double; cast for precision |
| `COUNT(col)` | `count(col)` | `count(col)` | Use `count(*)` for all rows |
| `COUNT(DISTINCT col)` | `countDistinct(col)` | `countDistinct(col)` | Or `approx_count_distinct()` for speed |
| `MAX(col)` | `max(col)` | `max(col)` | Direct equivalent |
| `MIN(col)` | `min(col)` | `min(col)` | Direct equivalent |
| `FIRST(col)` | `first(col)` | `first(col, ignorenulls=True)` | `TRUE` in Informatica = ignore nulls |
| `LAST(col)` | `last(col)` | `last(col, ignorenulls=True)` | Non-deterministic without ORDER BY |
| `MEDIAN(col)` | `percentile_approx(col, 0.5)` | `percentile_approx(col, 0.5)` | Approximate; exact is expensive |
| `STDDEV(col)` | `stddev(col)` | `stddev(col)` | Sample stddev |
| `VARIANCE(col)` | `variance(col)` | `variance(col)` | Sample variance |
| `PERCENTILE(col, 0.9)` | `percentile_approx(col, 0.9)` | `percentile_approx(col, 0.9, 10000)` | Accuracy vs performance tradeoff |

---

## Key Migration Patterns

### Nested IIF → CASE WHEN

```python
# Informatica:
# IIF(SAL > 5000, 'HIGH', IIF(SAL > 2000, 'MEDIUM', 'LOW'))

# Spark SQL:
df.withColumn("SAL_BAND",
    when(col("SAL") > 5000, "HIGH")
    .when(col("SAL") > 2000, "MEDIUM")
    .otherwise("LOW")
)
```

### DECODE → CASE WHEN

```python
# Informatica:
# DECODE(DEPTNO, 10, 'ACCOUNTING', 20, 'RESEARCH', 30, 'SALES', 'UNKNOWN')

# Spark SQL:
df.withColumn("DNAME",
    when(col("DEPTNO") == 10, "ACCOUNTING")
    .when(col("DEPTNO") == 20, "RESEARCH")
    .when(col("DEPTNO") == 30, "SALES")
    .otherwise("UNKNOWN")
)
```

### FIRST/LAST with ORDER BY

```python
# Informatica:
# FIRST(SAL) with ORDER BY HIREDATE within DEPTNO group

# Spark SQL:
from pyspark.sql.window import Window
from pyspark.sql.functions import first, last

window_spec = Window.partitionBy("DEPTNO").orderBy("HIREDATE")

df.withColumn("FIRST_SAL", first("SAL").over(window_spec)) \
  .withColumn("LAST_SAL", last("SAL").over(window_spec))

# WHY: first()/last() without window spec are non-deterministic.
# Always provide an explicit Window with orderBy for Informatica parity.
```

### TO_CHAR Date Formatting

```python
# Informatica:
# TO_CHAR(HIREDATE, 'YYYY/MM/DD HH24:MI:SS')

# Spark SQL:
df.withColumn("HIREDATE_FMT",
    date_format(col("HIREDATE"), "yyyy/MM/dd HH:mm:ss")
)

# WHY: Spark uses Java SimpleDateFormat patterns, not Oracle format masks.
# HH24 → HH, MI → mm, SS → ss, DDD → D, WW → w
```

### Nested Aggregates → Subquery or CTE

```python
# Informatica:
# MAX(AVG(SAL)) — aggregate of aggregate

# Spark SQL (subquery approach):
avg_sal = df.groupBy("DEPTNO").agg(avg("SAL").alias("AVG_SAL"))
max_avg = avg_sal.agg(max("AVG_SAL").alias("MAX_AVG_SAL"))

# Or using window function:
from pyspark.sql.functions import avg, max
from pyspark.sql.window import Window

window_spec = Window.partitionBy()
df.groupBy("DEPTNO").agg(avg("SAL").alias("AVG_SAL")) \
  .withColumn("MAX_AVG_SAL", max("AVG_SAL").over(window_spec))

# WHY: Spark SQL does not allow nested aggregates without a subquery.
# Use CTE, subquery, or window function for nested aggregations.
```

### LTRIM/RTRIM with Custom Character Set

```python
# Informatica:
# LTRIM(ENAME, '0') — trim leading zeros

# Spark SQL:
df.withColumn("ENAME_CLEAN",
    regexp_replace(col("ENAME"), r'^[0]+', '')
)

# WHY: Spark's ltrim() only trims spaces. For custom character sets,
# use regexp_replace with ^ (leading) or $ (trailing) anchors.
```

---

## When This Skill Should NOT Fire

- Do NOT use for transformation-level architecture (see `informatica-to-databricks-transformation-mapping`).
- Do NOT use for workflow orchestration migration (see `informatica-to-databricks-workflow-mapping`).
- Do NOT use for Spark functions unrelated to Informatica equivalents.
- Do NOT use for performance optimization (see `informatica-to-databricks-performance-patterns`).
- Do NOT use for identifying anti-patterns (see `informatica-to-databricks-anti-patterns`).
