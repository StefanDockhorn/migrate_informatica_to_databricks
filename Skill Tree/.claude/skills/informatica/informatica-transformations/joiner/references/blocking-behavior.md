# Blocking Behavior Reference

> **When to use this file:** When the main `informatica-joiner` skill needs deeper detail on blocking transformation mechanics, pipeline implications, and Spark equivalents for managing blocking operations.

## What Is a Blocking Transformation?

A **blocking transformation** must receive **all input rows** from at least one input group before it can output any rows. This breaks pipelined execution and can cause memory pressure.

### Blocking Transformations in Informatica

| Transformation | Blocks | Memory Impact |
|---|---|---|
| Joiner (unsorted) | Detail pipeline until master fully cached | Master table size |
| Aggregator (no sorted input) | All input until groups complete | Group key cardinality |
| Rank | All input until ranking determined | Top N rows per group |
| Sorter | All input until sort complete | Full dataset |
| Lookup (static cache) | First lookup row until cache built | Lookup source size |
| Custom Transformation | Configurable | Depends on implementation |
| Normalizer (VSAM) | All input until pivot complete | Full dataset |
| XML Generator | All input until XML built | Full dataset |

### Non-Blocking (Pipeline-Friendly) Transformations

| Transformation | Behavior |
|---|---|
| Expression | Row-by-row, immediate output |
| Filter | Row-by-row, immediate output (or drop) |
| Router | Row-by-row, routes to matching groups immediately |
| Sequence Generator | Row-by-row, generates value immediately |
| Union | Row-by-row from each input group |
| Source Qualifier | Row-by-row from source |

## Pipeline Architecture Impact

```
Non-blocking pipeline:
Source → Expression → Filter → Target (all rows flow through simultaneously)

Blocking pipeline:
Source → Sorter → Aggregator → Target
         ↑ blocks       ↑ blocks
         all rows       all rows
         until sort     until groups
         complete       complete
```

When multiple blocking transformations are chained, memory accumulates:

```
Source → Sorter (dataset in memory) → Aggregator (groups in memory) → Target
              ↑ 2GB                       ↑ 500MB groups
         Sorter cache                   Aggregate cache
```

## Spark Equivalent: No Explicit Blocking

Spark handles blocking semantics internally:

| Informatica Blocking | Spark Mechanism |
|---|---|
| Unsorted Joiner (cache master) | Shuffle hash join or broadcast join |
| Aggregator | Hash aggregation or sort-based aggregation |
| Sorter | External sort (spills to disk if needed) |
| Rank | Window function with full shuffle |
| Lookup cache | Broadcast variable or broadcast hash join |

Spark automatically spills to disk when memory is exhausted — Informatica does not (it fails).

```python
# Spark handles large aggregations gracefully via disk spill
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.memory.fraction", "0.8")

# External sort enabled by default — no manual configuration needed
# But you can tune:
spark.conf.set("spark.sql.sortMergeJoinExec.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

## Detecting and Mitigating Blocking Chains

### Detection Pattern
```python
def detect_blocking_chain(transformations):
    """Analyze a chain of transformations for blocking behavior."""
    blocking_types = {'JOINER', 'AGGREGATOR', 'RANK', 'SORTER', 
                      'LOOKUP', 'CUSTOM', 'NORMALIZER'}
    blocking_chain = []
    for tx in transformations:
        if tx['TYPE'] in blocking_types:
            blocking_chain.append(tx['NAME'])
    if len(blocking_chain) > 2:
        return f"WARNING: {len(blocking_chain)} blocking transformations chained: {blocking_chain}"
    return "OK"
```

### Mitigation Strategies

1. **Replace with sorted input** (Joiner, Aggregator) — eliminates blocking
2. **Add Sorter + enable sorted input** on downstream blocking transformations
3. **Reduce DTM buffer size** — forces earlier spill (Informatica only)
4. **Increase Integration Service memory** — temporary fix, not scalable
5. **In Spark**: Tune `spark.sql.shuffle.partitions` to avoid skew

## Blocking + Session Recovery

Blocking transformations prevent session recovery from functioning correctly for upstream transformations. If a session fails inside a blocking transformation, recovery can only resume from the beginning of the blocking operation — not from the last successful row.

In Spark: checkpoint/save intermediate results before expensive blocking operations for fault tolerance.

```python
# Checkpoint before expensive operation
df.checkpoint()
df.write.format("delta").mode("overwrite").save("/tmp/intermediate_checkpoint")
```
