---
name: informatica-to-databricks-anti-patterns
description: "Use when identifying Informatica PowerCenter patterns that cause issues in Databricks/Spark migrations. Covers row-by-row processing, blocking transformations, implicit type conversions, precision differences, and Spark-specific failure modes with fixes. Do NOT use for general anti-pattern discussion or non-migration scenarios."
---

# Anti-Patterns: Informatica Patterns That Fail in Spark

## 1. Row-by-Row Processing (RBAR → Set-Based)

```
Informatica pattern:
  Expression with variable ports:
    V_RUNNING_TOTAL = V_RUNNING_TOTAL + SAL
    O_RUNNING_TOTAL = V_RUNNING_TOTAL

Problem:
  Variable ports accumulate sequentially row-by-row.
  Spark processes data in partitions, not sequential rows.
  No guaranteed row order within a partition.

Fix: Use window functions
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("DEPTNO").orderBy("EMPNO") \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("RUNNING_TOTAL", sum("SAL").over(window_spec))

# WHY: sum() OVER with rowsBetween produces cumulative sums.
# Always specify orderBy for deterministic results.
# Without order, the running total is non-deterministic.
```

---

## 2. Unconnected Lookups in Row Expressions

```
Informatica pattern:
  Expression port: O_DNAME = :LKP.LKP_DEPT(DEPTNO)
  Called once per row to look up department name.

Problem:
  Spark UDFs calling external lookups cause N+1 queries.
  Each executor row triggers a separate database round-trip.
  Performance degrades from minutes to hours.

Fix: Broadcast join or lateral join
```python
from pyspark.sql.functions import broadcast

emp = spark.table("source.emp")
dept = spark.table("source.dept").select("DEPTNO", "DNAME")

# Broadcast the lookup table and join
result = emp.join(broadcast(dept), "DEPTNO", "left") \
  .select("EMPNO", "ENAME", "SAL", "DNAME")

# WHY: A single broadcast join replaces N lookup calls with one
# data movement. The dept table is sent once to all executors.
# Never implement lookups as per-row UDF calls.
```

---

## 3. Blocking Transformation Chains

```
Informatica pattern:
  Joiner → Aggregator → Rank (all "blocking" in Informatica)

Problem:
  In Informatica, each blocking stage writes all rows to cache
  before passing to the next stage.
  Spark handles this naturally via pipelining, BUT memory may
  be insufficient if shuffle output is huge.

Fix: Add explicit repartition checkpoints
```python
# Informatica: Join → Aggregate → Rank with 16 MB cache each

# Databricks:
joined = large_df.join(small_df, "KEY", "inner")

# Checkpoint between heavy stages to spill and recover
spark.conf.set("spark.sql.shuffle.partitions", 400)

aggregated = joined.repartition(200, "GROUP_KEY") \
  .groupBy("GROUP_KEY") \
  .agg(sum("AMT").alias("TOTAL_AMT"))

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

window_spec = Window.partitionBy("GROUP_KEY").orderBy(desc("TOTAL_AMT"))
ranked = aggregated.withColumn("RANK", rank().over(window_spec))

# WHY: repartition() before groupBy reduces skew and controls
# partition count. AQE (Adaptive Query Execution) coalesces
# small partitions automatically when enabled.
```

---

## 4. Implicit String Padding (CHAR Columns)

```
Informatica pattern:
  CHAR(10) column 'ABC' is auto-padded to 'ABC       '
  Join: EMP.CHAR_DEPTNO = DEPT.CHAR_DEPTNO works in Informatica

Problem:
  Spark does not auto-pad string columns.
  'ABC' != 'ABC       ' in Spark string comparison.
  Joins silently return zero rows.

Fix: Explicitly pad or trim both sides
```python
from pyspark.sql.functions import rpad, lpad, trim

# Option 1: Pad to fixed width before join
df.withColumn("DEPTNO_PADDED", rpad(col("DEPTNO"), 10, " "))

# Option 2: Trim both sides (safer, handles mixed data)
emp_trimmed = emp.withColumn("DEPTNO", trim(col("DEPTNO")))
dept_trimmed = dept.withColumn("DEPTNO", trim(col("DEPTNO")))
result = emp_trimmed.join(dept_trimmed, "DEPTNO")

# WHY: Spark treats strings as variable-length. Always normalize
# fixed-width columns after migration. Prefer trim() over pad()
# unless the downstream system requires fixed-width.
```

---

## 5. SYSDATE in SQL Override

```
Informatica pattern:
  Source Qualifier SQL Override:
    SELECT * FROM EMP WHERE HIREDATE > SYSDATE - 30

Problem:
  Spark JDBC may not push SYSDATE to the source database.
  SYSDATE evaluated in Spark may use a different timezone.
  Predicate pushdown fails, fetching all rows.

Fix: Compute date in Spark before JDBC read
```python
from pyspark.sql.functions import current_date, date_sub, lit
from datetime import datetime, timedelta

# Compute the cutoff date in Spark
cutoff_date = datetime.now() - timedelta(days=30)
cutoff_str = cutoff_date.strftime("%Y-%m-%d")

# Pass as literal in JDBC query
df = spark.read.jdbc(
    url,
    f"(SELECT * FROM EMP WHERE HIREDATE > '{cutoff_str}') AS emp_filtered",
    properties=props
)

# WHY: Computing the date client-side ensures the source database
# receives a literal value, enabling index usage and predicate
# pushdown. Never rely on source database functions in JDBC
# SQL overrides.
```

---

## 6. Session-Level Error Handling

```
Informatica pattern:
  Stop on 10 errors
  Write rejected rows to .bad file
  Continue processing valid rows

Problem:
  Spark fails fast on the first error (e.g., bad cast).
  No built-in error threshold or bad file mechanism.
  One bad row kills a 2-hour job.

Fix: Use try_cast + quarantine pattern
```python
from pyspark.sql.functions import col, when, isnan, isnull

# Identify bad rows BEFORE processing
def validate_and_split(df):
    bad_condition = (
        isnull("EMPNO") |
        isnull("SAL") |
        (col("SAL") < 0) |
        isnan("SAL")
    )

    good = df.filter(~bad_condition)
    bad = df.filter(bad_condition).withColumn("REJECT_REASON", lit("VALIDATION_FAILED"))
    return good, bad

good_df, bad_df = validate_and_split(spark.table("staging.emp_raw"))

# Process good rows
good_df.write.mode("overwrite").saveAsTable("target.emp_clean")

# Write bad rows to quarantine for manual inspection
bad_df.write.mode("append").saveAsTable("quarantine.emp_rejected")

# WHY: Spark has no "error threshold" concept. The quarantine pattern
# separates bad rows before they cause failures. Use try_cast() (DBR 13+)
# for null-on-error type conversions.
```

```python
# DBR 13+ try_cast for null-on-error conversions
from pyspark.sql.functions import try_cast, lit

df.withColumn("SAL_AS_INT", try_cast("SAL", "int")) \
  .filter(col("SAL_AS_INT").isNotNull())

# WHY: try_cast returns NULL on conversion failure instead of
# aborting the job. Filter out NULLs or route to quarantine.
```

---

## 7. Transaction Control Per Row

```
Informatica pattern:
  Transaction Control: TC_COMMIT_BEFORE when DEPTNO changes
  Groups rows by department and commits each group separately.

Problem:
  Spark does not support fine-grained transactions.
  Delta Lake writes are atomic at the table level.
  No "commit every N rows" mechanism.

Fix: Use Delta Lake time travel, or batch by key
```python
# Option 1: Accept Delta's atomic write semantics (recommended)
df.write.format("delta").mode("append").saveAsTable("target.emp")

# Option 2: If separate commit per key is truly required
for dept in df.select("DEPTNO").distinct().collect():
    dept_df = df.filter(col("DEPTNO") == dept.DEPTNO)
    dept_df.write.format("delta").mode("append").saveAsTable("target.emp")

# Option 3: Partitioned write achieves logical separation
df.write \
  .format("delta") \
  .partitionBy("DEPTNO") \
  .mode("overwrite") \
  .saveAsTable("target.emp")

# WHY: Option 1 is preferred — Delta's atomicity is a feature, not
# a limitation. Option 2 has massive overhead (N separate jobs).
# Option 3 provides logical partitioning without separate commits.
```

---

## 8. Custom C Procedures

```
Informatica pattern:
  Custom Transformation calling a C DLL
  Complex business logic in native C code.

Problem:
  Cannot run native C code in Spark.
  JNI is not available in Python/Scala Spark UDFs.
  Complete rewrite required.

Fix: Rewrite as Python UDF or pandas UDF
```python
# Informatica: Custom C function compute_tax(income, state_code)

# Databricks (Python UDF for simple logic):
from pyspark.sql.functions import udf
from pyspark.sql.types import DecimalType

@udf(DecimalType(10, 2))
def compute_tax(income, state_code):
    tax_brackets = {
        "CA": 0.093,
        "NY": 0.0685,
        "TX": 0.0
    }
    rate = tax_brackets.get(state_code, 0.05)
    return float(income) * rate

df.withColumn("TAX", compute_tax(col("INCOME"), col("STATE")))

# For complex logic or DataFrame operations, use pandas UDF:
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("decimal(10,2)")
def compute_tax_pandas(income: pd.Series, state_code: pd.Series) -> pd.Series:
    rates = {"CA": 0.093, "NY": 0.0685, "TX": 0.0}
    return income * state_code.map(rates).fillna(0.05)

df.withColumn("TAX", compute_tax_pandas(col("INCOME"), col("STATE")))

# WHY: pandas UDFs are 10-100x faster than row-at-a-time UDFs
# because they process batches of rows using Arrow serialization.
# Always prefer pandas UDFs for non-trivial logic.
```

---

## 9. File-Based Lookup Cache

```
Informatica pattern:
  Lookup with persistent cache file (.idx + .dat)
  Shared across multiple sessions via $PMLookupFileDir

Problem:
  Spark executors run on separate nodes.
  No shared local filesystem between executors.
  Cache file on one node is invisible to others.

Fix: Use Delta table or broadcast variable
```python
from pyspark.sql.functions import broadcast

# Option 1: Delta table (for large, slowly changing lookup)
lookup_df = spark.table("reference.dept_lookup").cache()

result = emp_df.join(
    broadcast(lookup_df),
    emp_df.DEPTNO == lookup_df.DEPTNO,
    "left"
).select("EMP.*", "DNAME", "LOC")

# Option 2: Broadcast variable (for small, static lookup)
lookup_dict = {
    row.DEPTNO: (row.DNAME, row.LOC)
    for row in spark.table("reference.dept").collect()
}
lookup_bc = spark.sparkContext.broadcast(lookup_dict)

@udf("struct<DNAME:string,LOC:string>")
def lookup_dept(deptno):
    dname, loc = lookup_bc.value.get(deptno, (None, None))
    return (dname, loc)

df.withColumn("lookup", lookup_dept(col("DEPTNO")))

# WHY: broadcast() sends the DataFrame to all executors.
# broadcast() variable sends a Python dict to all executors.
# Delta table is best for lookups > 100 MB or that change frequently.
```

---

## 10. Precision/Scale Assumptions

```
Informatica pattern:
  DECIMAL(10,2) port automatically enforces scale
  SAL * 1.1 → rounded to 2 decimal places automatically

Problem:
  Spark allows decimal overflow (results may have more scale).
  SAL(10,2) * 1.1 → result may be DECIMAL(12,3) or double.
  Results differ from Informatica rounding.

Fix: Explicitly cast and round
```python
from pyspark.sql.functions import col, round, bround, lit

# Informatica: SAL * 1.1 automatically rounds to DECIMAL(10,2)
# Spark: Must explicitly round and cast

df.withColumn(
    "SAL_WITH_RAISE",
    round(col("SAL") * lit(1.1), 2).cast("decimal(10,2)")
)

# For banking precision (HALF_EVEN rounding):
df.withColumn(
    "SAL_BANKING",
    bround(col("SAL") * lit(1.1), 2).cast("decimal(10,2)")
)

# WHY: Spark's round() uses HALF_UP. bround() uses HALF_EVEN
# (banker's rounding). Always cast back to the target decimal
# type to ensure precision matches the target schema.
```

---

## Quick Reference: Fix Summary

| Anti-Pattern | Spark Failure Mode | Fix |
|---|---|---|
| Row-by-row variables | Non-deterministic results | Window functions with `rowsBetween` |
| Unconnected lookups | N+1 queries, hours-long jobs | Broadcast join |
| Blocking chains | OOM on large shuffles | `repartition()` + AQE tuning |
| CHAR padding | Joins return zero rows | `trim()` or `rpad()` both sides |
| SYSDATE in SQL | Pushdown failure, full table scan | Compute date client-side |
| Error thresholds | Job fails on first bad row | Quarantine pattern + `try_cast` |
| Row-level commits | Not supported | Delta atomic writes / partition-by-key |
| C DLL procedures | Cannot execute | Rewrite as pandas UDF |
| File-based cache | Executors can't share files | Delta table or broadcast |
| Implicit decimal rounding | Precision loss / overflow | Explicit `round()` + `cast()` |

---

## When This Skill Should NOT Fire

- Do NOT use for transformation mapping (see `informatica-to-databricks-transformation-mapping`).
- Do NOT use for function syntax mapping (see `informatica-to-databricks-function-mapping`).
- Do NOT use for workflow orchestration (see `informatica-to-databricks-workflow-mapping`).
- Do NOT use for performance optimization (see `informatica-to-databricks-performance-patterns`).
- Do NOT use for general Spark programming without Informatica context.
