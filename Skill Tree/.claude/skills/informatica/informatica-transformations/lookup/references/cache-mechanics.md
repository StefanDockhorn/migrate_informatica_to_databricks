# Lookup Cache Mechanics

## Cache Architecture

The Integration Service builds lookup caches to avoid repeated database queries. Understanding cache mechanics is essential for performance tuning and troubleshooting.

## Cache Build Modes

| Mode | When Cache Builds | Use Case |
|------|-------------------|----------|
| Auto | At first lookup row (default) | Most scenarios |
| Pre-build | Before first lookup row arrives | Large caches that need early loading |

## Cache File Types

| File Type | Extension | Content |
|-----------|-----------|---------|
| Data Cache | `.dat` | Lookup table row data |
| Index Cache | `.idx` | Key-to-row pointers |

## Cache Sizing

- **Index Cache**: Stores key values and row pointers
- **Data Cache**: Stores actual row data
- When cache exceeds configured size, Integration Service pages to disk

## Persistent Cache

- Cache files saved to disk after session completion
- Reused across session runs
- Identified by Cache File Name Prefix
- Invalidated when source data changes

### Persistent Cache Invalidation

```
-- When source table changes:
-- 1. Delete .dat and .idx cache files
-- 2. Or disable persistent cache in session
-- 3. Or use $PMCacheDir to manage cache files
```

## Shared Cache

### Named Shared Cache

Multiple lookups share a single cache by name:
```
-- Lookup 1: Cache Name = "dept_cache"
-- Lookup 2: Cache Name = "dept_cache" (reuses same cache)
```

### Unnamed Shared Cache

Lookups with identical source and condition share cache automatically.

## Cache Performance Tips

- Always cache small lookup tables (under 1M rows)
- Use persistent cache for slowly-changing reference data
- Use shared cache when multiple lookups query the same table
- Pre-build cache for large lookups to avoid first-row latency
- Uncached mode only for real-time lookups that must see latest data
- Monitor $PMCacheDir disk space for persistent cache files

## Spark Equivalent: Broadcast Variables

```python
from pyspark.sql.functions import broadcast

# Small DataFrame broadcast (equivalent to lookup cache)
lookup_df = spark.read.parquet("/path/to/dept")
result = large_df.join(broadcast(lookup_df), "deptno", "left")

# Manual broadcast variable (equivalent to shared cache)
lookup_dict = {row.id: row.name for row in lookup_df.collect()}
lookup_bcast = spark.sparkContext.broadcast(lookup_dict)

@udf("string")
def cached_lookup(key):
    return lookup_bcast.value.get(key)
```