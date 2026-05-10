---
name: informatica-lookup
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Lookup transformations. Covers connected/unconnected, static/dynamic cache, relational/flat file/pipeline sources, SQL override, and cache sharing. Includes Databricks broadcast join and left outer join equivalents. Do NOT use for general join optimization or non-Informatica lookup patterns."
---

# Lookup Transformation

## Purpose

Look up data in a relational table, flat file, or pipeline source.

## Source Types

| Source Type | Description |
|-------------|-------------|
| Relational | Database table or view |
| Flat File | CSV, delimited, or fixed-width file |
| Pipeline | Output from another transformation in the mapping |

## Connected vs Unconnected

| Aspect | Connected | Unconnected |
|--------|-----------|-------------|
| Input | Direct pipeline connection | Called from expression via `:LKP.lookup_name(port, port)` |
| Return columns | Multiple columns | Single return value |
| Rows per input | One row per input row | One value per call |
| Use case | Multi-column enrichment | Single-value lookup in expressions |

## Cache Types

| Cache Type | Description |
|------------|-------------|
| Uncached | Queries database for every lookup |
| Static Cache | Builds cache once at first lookup row; read-only |
| Dynamic Cache | Updates cache during session (insert/update rows) |
| Persistent Cache | Reuses cache file across session runs |
| Shared Cache | Named or unnamed cache shared between lookups |

## Key Properties

| Property | Description |
|----------|-------------|
| Lookup table | Source table, file, or pipeline for lookup data |
| Source Type | Relational / Flat File / Pipeline |
| Lookup Policy on Multiple Match | Use First / Use Last / Report Error / Report All |
| Connection Information | Database connection for relational lookups |
| Cache File Name Prefix | Prefix for persistent cache files |
| Pre-build Lookup Cache | Build cache before first lookup row arrives |

## Dynamic Cache

- **NewLookupRow port**: Indicates `Insert` (1), `Update` (2), or `NoChange` (0)
- **Insert Else Update**: If row not in cache, insert; if found, update
- **Update Else Insert**: If row found, update; if not, insert
- Commonly used for slowly changing dimensions and target table lookups

## Behavior Rules

- Always make the smallest table/source the lookup (master in join semantics)
- For connected lookups, all input ports must participate in the lookup condition
- Dynamic cache requires matching all lookup condition ports exactly
- Cache is built at first lookup row unless pre-built; use persistent cache for slowly-changing lookup data
- Unconnected lookups must have exactly one return port
- NULL in any lookup condition port returns no match
- Multiple match behavior is policy-dependent (Use First / Use Last / Error / Report)

## Spark Equivalent

```python
from pyspark.sql.functions import broadcast, col, row_number, collect_list
from pyspark.sql.window import Window

# Connected single match: broadcast join (small lookup)
result = detail_df.join(
    broadcast(lookup_df),
    detail_df.deptno == lookup_df.deptno,
    "left"
).select(detail_df["*"], lookup_df["dname"])

# Multiple match: join with row_number()
joined = detail_df.join(lookup_df, "deptno", "left")
window_spec = Window.partitionBy("deptno").orderBy(lookup_df["version"].desc())
result = joined.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1)

# Unconnected lookup: create lookup DataFrame, broadcast, use UDF or left join
def lookup_value(key):
    # Broadcast lookup_df to executors
    row = lookup_broadcast.value.get(key)
    return row[1] if row else None

# Dynamic cache (SCD): Delta Lake MERGE INTO
spark.sql("""
MERGE INTO target_table t
USING source_updates s ON t.key = s.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

## Edge Cases

- NULL in lookup condition returns no match -- use `ISNULL()` in expression to handle
- Multiple matches behavior is policy-dependent; verify Lookup Policy on Multiple Match setting
- Dynamic cache with Insert Else Update requires unique lookup condition
- Pipeline source lookups cannot use persistent cache
- Flat file lookups build entire file into memory cache

## Example

```
-- Connected lookup: DEPTNO -> DNAME
-- Lookup condition: IN_DEPTNO = LOOKUP_DEPTNO
-- Return port: DNAME

-- Unconnected lookup call in Expression:
:LKP.lkp_dept(EMP.DEPTNO)
```

See also:
- [references/cache-mechanics.md](references/cache-mechanics.md)
- [references/dynamic-lookup-examples.md](references/dynamic-lookup-examples.md)
- [references/unconnected-lookup-patterns.md](references/unconnected-lookup-patterns.md)