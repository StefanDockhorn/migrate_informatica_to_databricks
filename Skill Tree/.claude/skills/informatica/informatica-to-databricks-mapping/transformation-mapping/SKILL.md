---
name: informatica-to-databricks-transformation-mapping
description: "Use when mapping Informatica PowerCenter transformations to Databricks/Spark equivalents. Covers the complete transformation mapping table, configuration property equivalents, and before/after code examples. Do NOT use for general ETL migration advice, workflow orchestration, or SQL function mapping."
---

# Transformation Mapping: Informatica PowerCenter → Databricks/Spark

## Transformation Mapping Table

| Informatica Transformation | Databricks/Spark Equivalent | Complexity | Notes |
|---|---|---|---|
| Source Qualifier | `spark.read.jdbc()` / `spark.read.format()` | Low | SQL override → `option("query", sql)`. Prefer pushdown. |
| Expression | `withColumn()` / `selectExpr()` | Low | Variable ports → chained `withColumn()`. Order matters. |
| Filter | `df.filter()` / `WHERE` | Low | Direct equivalent. Combine early for predicate pushdown. |
| Sorter | `orderBy()` / `sort()` | Low | `DISTINCT` → `dropDuplicates()`. Sorting large datasets triggers shuffle. |
| Aggregator | `groupBy().agg()` | Low | Sorted input → pre-sort not required; Spark uses hash aggregation. |
| Joiner | `df.join()` with `broadcast()` | Medium | Master = broadcast side. Always broadcast the smaller relation. |
| Lookup (connected, single) | `left join` / `broadcast` join | Medium | Use `broadcast()` hint for dimension tables < 10 MB. |
| Lookup (connected, multiple) | `join` + `row_number()` | High | Row duplication needed when lookup returns multiple matches. |
| Lookup (unconnected) | UDF with broadcast lookup | High | Requires lateral join or UDF. See negative cases below. |
| Lookup (dynamic cache) | `MERGE INTO` (Delta Lake) | High | Delta Lake upsert pattern. Cannot use static join. |
| Router | Multiple `df.filter()` / `CASE WHEN` | Low | One-to-many routing. Evaluate once, branch to separate DataFrames. |
| Union | `union()` / `unionByName()` | Low | `union()` requires identical schema order; `unionByName()` is safer. |
| Rank | `rank().over(Window)` | Medium | Window functions required. Dense rank uses `dense_rank()`. |
| Sequence Generator | `monotonically_increasing_id()` | Low | Gaps acceptable. For gap-free, use `zipWithIndex()` after coalesce. |
| Update Strategy | `write.mode()` / `MERGE INTO` | High | Delta `MERGE` for SCD/upserts. No direct row-level update. |
| Transaction Control | Auto-commit / Delta transactions | High | No direct Spark equivalent. Delta provides atomic writes. |
| Normalizer | `explode()` / `unpivot` | Medium | `explode()` for nested arrays. `stack()` for unpivot. |
| Stored Procedure | JDBC callable / Spark UDF | Medium | Prefer Spark-native rewrite; JDBC call adds driver bottleneck. |
| Data Masking | `sha()` / `md5()` / UDF | Medium | Use deterministic hashing for consistent masking. |
| Custom Transformation | UDF / pandas UDF | High | Rewrite logic in Python/Scala. Avoid for simple operations. |
| External Procedure | External JAR / UDF | High | Platform dependency; requires complete rewrite. |
| XML Source Qualifier | `spark.read.format("xml")` | Medium | Use Databricks XML format package. |
| SQL Transformation | `spark.sql()` | Low | Direct equivalent. Register temp views first. |
| HTTP Transformation | HTTP requests via UDF | Medium | Use external library (requests, urllib). Add retry logic. |
| Java Transformation | Python/Scala UDF | High | Rewrite in Spark-native language. No JVM interop for custom logic. |

---

## Before/After Code Examples

### Source Qualifier → Expression → Filter → Target

```python
# Informatica chain:
# Source Qualifier (SQL Override: SELECT EMPNO, ENAME, SAL, COMM, DEPTNO FROM EMP WHERE SAL > 0)
#   → Expression (TOTAL_COMP = SAL + NVL(COMM, 0))
#   → Filter (DEPTNO IN 10, 20, 30)
#   → Target

# Databricks equivalent:
df = (
    spark.read
    .jdbc(url, "(SELECT EMPNO, ENAME, SAL, COMM, DEPTNO FROM EMP WHERE SAL > 0) AS emp_sql", properties=props)
    .withColumn("TOTAL_COMP", col("SAL") + coalesce(col("COMM"), lit(0)))
    .filter(col("DEPTNO").isin([10, 20, 30]))
)
df.write.mode("overwrite").saveAsTable("target.emp_clean")

# WHY: Push the WHERE into JDBC SQL override to reduce data transfer.
# Expression becomes withColumn; Filter becomes filter().
```

### Joiner (Master/Detail Normal Join)

```python
# Informatica:
# Joiner: EMP [Master] + DEPT [Detail], Normal Join, DEPTNO = DEPTNO

# Databricks equivalent:
emp = spark.table("source.emp")
dept = spark.table("source.dept")

from pyspark.sql.functions import broadcast
# Always broadcast the smaller side (dept) to avoid shuffle
result = emp.join(broadcast(dept), "DEPTNO", "inner")

# WHY: broadcast() keeps the dept table in memory on each executor,
# eliminating a shuffle. Only broadcast when lookup < 10 MB.
```

### Aggregator (GROUP BY with SUM, AVG)

```python
# Informatica:
# Aggregator: SUM(SAL), AVG(SAL) GROUP BY DEPTNO
# Sorted input checked → no pre-sort needed in Spark

# Databricks equivalent:
from pyspark.sql.functions import sum, avg, round

df.groupBy("DEPTNO").agg(
    sum("SAL").alias("TOTAL_SAL"),
    round(avg("SAL"), 2).alias("AVG_SAL")
)

# WHY: Spark's Tungsten engine uses hash aggregation by default.
# Pre-sorting is unnecessary and wastes CPU.
```

### Rank (Top N per Group)

```python
# Informatica:
# Rank: Top 3 salaries per DEPTNO, rank by SAL descending

# Databricks equivalent:
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, col

window_spec = Window.partitionBy("DEPTNO").orderBy(desc("SAL"))

df.withColumn("RANK", rank().over(window_spec)) \
  .filter(col("RANK") <= 3)

# WHY: rank() assigns same rank to ties. Use row_number() for deterministic
# ranking or dense_rank() for no gaps.
```

### Router (One-to-Many Split)

```python
# Informatica:
# Router: DEPTNO=10 → TGT_10, DEPTNO=20 → TGT_20, DEPTNO=30 → TGT_30, default → TGT_OTHER

# Databricks equivalent:
all_rows = spark.table("source.emp")

tgt_10 = all_rows.filter(col("DEPTNO") == 10)
tgt_20 = all_rows.filter(col("DEPTNO") == 20)
tgt_30 = all_rows.filter(col("DEPTNO") == 30)
tgt_other = all_rows.filter(~col("DEPTNO").isin([10, 20, 30]))

tgt_10.write.mode("overwrite").saveAsTable("target.dept_10")
tgt_20.write.mode("overwrite").saveAsTable("target.dept_20")
tgt_30.write.mode("overwrite").saveAsTable("target.dept_30")
tgt_other.write.mode("overwrite").saveAsTable("target.dept_other")

# WHY: Each filter is a narrow transformation; Catalyst eliminates re-reading.
# Write each branch separately for true multi-target output.
```

### Sequence Generator

```python
# Informatica:
# Sequence Generator: Start 1, Increment 1, No Cycle

# Databricks equivalent:
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# Option 1: Gap-free IDs (requires single partition or Window)
window_spec = Window.orderBy(monotonically_increasing_id())
df.withColumn("SEQ_ID", row_number().over(window_spec))

# Option 2: Distributed IDs with possible gaps
df.withColumn("SEQ_ID", monotonically_increasing_id())

# WHY: monotonically_increasing_id() is distributed and fast but may have gaps.
# Use row_number() over a window only when gap-free is strictly required.
```

### Update Strategy (SCD Type 1)

```python
# Informatica:
# Update Strategy: DD_INSERT for new, DD_UPDATE for existing

# Databricks equivalent (Delta Lake MERGE):
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "target.emp_scd1")

source = spark.table("staging.emp_updates")

target.alias("t").merge(
    source.alias("s"),
    "t.EMPNO = s.EMPNO"
).whenMatchedUpdate(set={
    "ENAME": "s.ENAME",
    "SAL": "s.SAL",
    "DEPTNO": "s.DEPTNO",
    "MODIFIED_DATE": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "EMPNO": "s.EMPNO",
    "ENAME": "s.ENAME",
    "SAL": "s.SAL",
    "DEPTNO": "s.DEPTNO",
    "MODIFIED_DATE": "current_timestamp()"
}).execute()

# WHY: Spark does not support row-level UPDATE. Delta MERGE is the only
# pattern for upserts. Always use mergeSchema=true for schema evolution.
```

### Normalizer (VSAM/Array Unroll)

```python
# Informatica:
# Normalizer: 5 OCCURS columns → 5 output rows per input row

# Databricks equivalent:
from pyspark.sql.functions import explode, arrays_zip, col, lit

df.withColumn("combined", arrays_zip(
    col("SAL_JAN"), col("SAL_FEB"), col("SAL_MAR"),
    col("SAL_APR"), col("SAL_MAY")
)).withColumn("zipped", explode("combined")) \
  .select(
      "EMPNO",
      col("zipped.SAL_JAN").alias("SALARY"),
      lit("JAN").alias("MONTH")
  )

# WHY: explode() creates multiple rows from an array.
# For simple unpivot, prefer select + union or stack() function.
```

---

## Patterns Requiring Manual Intervention

These patterns cannot be auto-migrated and require architectural redesign:

| Pattern | Complexity | Reason | Fix Strategy |
|---|---|---|---|
| **Unconnected Lookups** | High | No DataFrame equivalent for per-row lookup function | Rewrite as broadcast join or lateral view |
| **Dynamic Lookup Cache** | High | Cache mutates during mapping run | Use Delta `MERGE INTO` or idempotent reprocessing |
| **Blocking transformations** | High | Affect Spark's pipelined execution | Add explicit `repartition()` checkpoints |
| **Transaction Control** | High | Spark has no fine-grained transactions | Delta Lake auto-commit + time travel |
| **Custom/External Procedures** | High | Platform-native compiled code | Complete rewrite as UDF or external service |
| **Java Transformations** | High | JVM interop not available for custom logic | Port to Python/Scala UDF or pandas UDF |
| **Post-session commands** | Medium | Imperative shell commands | Move to Databricks Workflow task dependencies |

---

## When This Skill Should NOT Fire

- Do NOT use for general Spark programming questions unrelated to migration.
- Do NOT use for greenfield Databricks design (no Informatica source).
- Do NOT use for workflow orchestration mapping (see `informatica-to-databricks-workflow-mapping`).
- Do NOT use for function-level syntax mapping (see `informatica-to-databricks-function-mapping`).
- Do NOT use for performance optimization post-migration (see `informatica-to-databricks-performance-patterns`).
