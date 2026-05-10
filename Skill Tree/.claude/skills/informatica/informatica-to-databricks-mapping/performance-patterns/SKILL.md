---
name: informatica-to-databricks-performance-patterns
description: "Use when optimizing migrated Informatica PowerCenter mappings for Databricks performance. Covers cache optimization, partition tuning, broadcast hints, shuffle management, and Spark-specific optimizations with Informatica context. Do NOT use for general Spark performance tuning or non-migration scenarios."
---

# Performance Patterns: Informatica → Databricks Optimization

## Cache Optimization Mapping

| Informatica Pattern | Spark Equivalent | When to Use | Why |
|---|---|---|---|
| Lookup cache (small table, < 10 MB) | `broadcast()` hint | Dimension tables, code tables | Keeps lookup in memory per executor; eliminates shuffle |
| Lookup cache (small table, 10-100 MB) | `broadcast()` with care | Larger dimensions | Monitor executor memory; spill to disk if exceeded |
| Lookup cache (large table, > 100 MB) | Regular shuffle join | Fact-to-fact joins | Avoid OOM; let Spark choose sort-merge join |
| Persistent lookup cache | Delta table with `ZORDER` | Slowly changing lookups | Fast lookups via optimized file skipping on Z-ordered columns |
| Aggregator sorted input | No pre-sort needed | All aggregations | Spark hash aggregation is efficient; pre-sorting wastes CPU |
| Joiner sorted input | No pre-sort needed | All joins | Spark sort-merge join handles sorting internally |
| Sorter before join | Remove explicit sort | Pre-join sorting | Catalyst optimizer adds sorts only when necessary |
| Sequence generator gaps | `monotonically_increasing_id()` | Surrogate keys | Gaps acceptable in Spark; gap-free requires `zipWithIndex()` |
| Source Qualifier SQL override | Pushdown via JDBC | All database reads | Enable `pushDownPredicate=true` to filter at source |
| Persistent cache file | Delta table or broadcast | Shared reference data | Spark executors don't share local files |

### Broadcast Hint Examples

```python
# Informatica:
# Lookup: DEPT table (100 rows) cached, joined with EMP (1M rows)

# Databricks:
from pyspark.sql.functions import broadcast

emp = spark.table("source.emp")          # 1M rows — fact
dept = spark.table("source.dept")        # 100 rows — dimension

# Always broadcast the smaller side
result = emp.join(broadcast(dept), "DEPTNO", "inner")

# WHY: broadcast() sends a copy of dept to every executor.
# Eliminates shuffle of the large emp table. No SortMergeJoin needed.
# Spark will auto-broadcast if spark.sql.autoBroadcastJoinThreshold
# is set (default 10 MB), but explicit hint is clearer.
```

```python
# Anti-pattern: Broadcasting a large table
big_table = spark.table("source.transactions")  # 50 GB
small_table = spark.table("source.accounts")     # 5 GB

# WRONG — broadcasting 5 GB will cause OOM
result = big_table.join(broadcast(small_table), "ACCT_ID")

# CORRECT — let Spark use SortMergeJoin
result = big_table.join(small_table, "ACCT_ID")

# Or if accounts is still under threshold, rely on auto-broadcast:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB default
```

---

## Partition Tuning

### Informatica → Spark Partition Mapping

| Informatica Partition Type | Spark Equivalent | API | Use Case |
|---|---|---|---|
| Hash auto-keys | Hash partitioning | `df.repartition(N, "key_col")` | Even distribution by key |
| Key range | Range partitioning | `df.repartitionByRange(N, "key_col")` | Sorted output by key range |
| Round-robin | Round-robin partitioning | `df.repartition(N)` | Even distribution regardless of key |
| Pass-through | Single partition per task | `df.coalesce(N)` | Reduce partition count before write |
| Database partitioning | JDBC parallel read | `option("partitionColumn", "id")` | Parallel DB reads by range |

### Partition Tuning Examples

```python
# Informatica:
# EMP table partitioned by hash on DEPTNO, 4 partitions

# Databricks:
emp = spark.table("source.emp").repartition(4, "DEPTNO")

# WHY: repartition() with a column triggers hash partitioning.
# All rows with the same DEPTNO hash to the same partition.
# Critical before groupBy or join on that key to minimize shuffle.
```

```python
# Skewed data handling (common migration issue)
# Informatica: Even partition sizes guaranteed by round-robin
# Spark: Skewed key causes one partition to be huge

# Detect skew:
emp.groupBy("DEPTNO").count().orderBy(desc("count")).show()

# Fix: Salt the skewed key
def add_salt(df, key_col, num_salts=10):
    from pyspark.sql.functions import rand, lit, concat, col, lpad
    return df.withColumn(
        f"{key_col}_salted",
        concat(col(key_col), lit("_"), lpad((rand() * num_salts).cast("int"), 2, "0"))
    )

# Repartition on salted key, then groupBy original key
salted = add_salt(emp, "DEPTNO", 10)
salted.repartition(40, "DEPTNO_salted").groupBy("DEPTNO").agg(sum("SAL"))

# WHY: Salting distributes a hot key across multiple partitions.
# Essential when one key value represents > 20% of dataset.
```

---

## Shuffle Management

```python
# Informatica:
# Sorter (16 MB cache) → Joiner → Aggregator
# Each stage writes to disk cache

# Databricks:
# Control shuffle with these configs:
spark.conf.set("spark.sql.shuffle.partitions", 200)  # Default; increase for large joins
spark.conf.set("spark.sql.adaptive.enabled", "true")           # AQE - auto-optimize
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # Auto handle skew

# WHY: spark.sql.shuffle.partitions controls shuffle output partitions.
# Default 200 is often too low for large datasets.
# Rule of thumb: target 100-200 MB per partition after shuffle.
# AQE (Adaptive Query Execution) dynamically optimizes at runtime.
```

```python
# Informatica:
# Sorter transformation with 16 MB work memory

# Databricks:
# No explicit sorter memory tuning needed.
# Spark spills to disk automatically when execution memory is exhausted.
spark.conf.set("spark.memory.fraction", 0.8)        # Default 0.6
spark.conf.set("spark.memory.storageFraction", 0.3) # Default 0.5

# WHY: Spark manages memory automatically. Tuning is rarely needed.
# If spills are excessive, increase cluster memory or reduce shuffle partitions.
```

---

## Write Optimization

| Informatica Write Pattern | Spark/Databricks Equivalent | Notes |
|---|---|---|
| Target flat file (fixed width) | `df.write.option("delimiter", "")` | Use `concat()` to format fixed-width columns |
| Target flat file (delimited) | `df.write.csv("/path")` | Direct equivalent |
| Target table (truncate) | `df.write.mode("overwrite").saveAsTable()` | `overwrite` drops and recreates |
| Target table (append) | `df.write.mode("append").saveAsTable()` | Direct equivalent |
| Target table (update) | `MERGE INTO` (Delta) | No direct row-level update in Spark |
| Target table (delete) | `MERGE INTO ... WHEN MATCHED DELETE` | Delta MERGE for deletes |
| Target table (reject file) | Quarantine table | Write bad rows to separate Delta table |
| Pre-SQL (truncate target) | `spark.sql("TRUNCATE TABLE target")` | Run as separate command before write |
| Post-SQL (gather stats) | `ANALYZE TABLE target COMPUTE STATISTICS` | Spark auto-gathers; explicit for accuracy |

### Optimized Write Pattern

```python
# Informatica:
# Session: Target → ORACLE_EMP, pre-SQL = "TRUNCATE TABLE EMP"
#                   Target → BAD_FILE, on error

# Databricks (Delta Lake):
from delta.tables import DeltaTable

# 1. Truncate target (pre-SQL)
spark.sql("TRUNCATE TABLE target.emp")

# 2. Read and validate
source = spark.table("staging.emp_raw")
valid = source.filter(col("EMPNO").isNotNull() & col("SAL").isNotNull())
bad = source.filter(col("EMPNO").isNull() | col("SAL").isNull())

# 3. Write good records
def optimize_write(df, table):
    # Optimize for large writes
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    df.write.format("delta").mode("append").saveAsTable(table)

optimize_write(valid, "target.emp")

# 4. Write bad records to quarantine
bad.write.mode("append").saveAsTable("quarantine.emp_bad")

# WHY: Delta optimizeWrite coalesces small files during write.
# autoCompact runs background compaction. Both reduce small file problems.
# Always separate bad records for debugging and reprocessing.
```

---

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Chaining many `withColumn` without caching | Catalyst handles this fine, but repeated use causes long plans | Cache at checkpoint boundaries |
| Using `coalesce(1)` for large datasets | Single partition causes OOM and skew | Use `coalesce(N)` with reasonable N |
| Calling UDFs for simple operations | UDFs are black boxes to Catalyst; no optimization | Use built-in functions |
| `collect()` to driver for large data | Driver OOM | Use `write()` to persist |
| Repartitioning without need | Unnecessary shuffle | Let AQE handle it |
| Sorting before every join | Redundant; Spark sorts during sort-merge | Remove explicit sorts |
| `persist()` on everything | Memory pressure; eviction thrashing | Only cache reused DataFrames |
| Ignoring data skew | One task takes 10x longer | Salt skewed keys or isolate |

### Caching Strategy

```python
# Informatica:
# Lookup cache persists for entire session
# Aggregator cache auto-managed

# Databricks:
# Only cache DataFrames that are used more than once
emp = spark.table("source.emp").filter(col("DEPTNO").isin([10, 20, 30]))
emp_cached = emp.cache()  # Used in 2 downstream branches

# Branch 1: Join with dept
departments = emp_cached.join(broadcast(dept), "DEPTNO")

# Branch 2: Aggregate salaries
totals = emp_cached.groupBy("DEPTNO").agg(sum("SAL"))

emp_cached.unpersist()  # Release when done

# WHY: cache() materializes in memory. Only use when a DataFrame
# is referenced multiple times. Always unpersist when done.
```

---

## When This Skill Should NOT Fire

- Do NOT use for transformation mapping (see `informatica-to-databricks-transformation-mapping`).
- Do NOT use for function syntax mapping (see `informatica-to-databricks-function-mapping`).
- Do NOT use for workflow orchestration (see `informatica-to-databricks-workflow-mapping`).
- Do NOT use for general Spark tuning without Informatica migration context.
- Do NOT use for identifying anti-patterns (see `informatica-to-databricks-anti-patterns`).
