---
name: informatica-joiner
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Joiner transformations. Covers normal/master outer/detail outer/full outer joins, sorted/unsorted input, master/detail designation, and blocking behavior. Includes Spark SQL join equivalents. Do NOT use for general SQL join writing."
---

# Joiner Transformation

## Purpose

Joins two heterogeneous sources (can be any source type -- relational, flat file, XML, etc.).

## Join Types

| Join Type | Behavior |
|-----------|----------|
| Normal Join | Inner join -- only matching rows from both sources |
| Master Outer Join | Left outer -- all detail rows + matching master rows |
| Detail Outer Join | Right outer -- all master rows + matching detail rows |
| Full Outer Join | Full outer -- all rows from both sources |

## Master/Detail Designation

- **Master**: Smaller source -- loaded entirely into cache/memory
- **Detail**: Larger source -- streamed through join
- Always designate the smaller source as Master for optimal cache performance

## Sorted Input

- Both sources must be sorted on join ports in the same order
- Eliminates blocking behavior -- detail source streams without pause
- Required sort order: all join key columns, ascending

## Unsorted Joiner (Blocking)

- Blocks detail source until all master rows are cached
- Memory intensive for large master sources
- Detail pipeline stalls until master is fully loaded

## Blocking Behavior

| Input Type | Blocking | Why |
|------------|----------|-----|
| Unsorted | Yes | Must cache all master rows before processing detail |
| Sorted | No | Streams both sources simultaneously |

## Join Condition

- One or more port pairs compared with equality only
- All conditions use AND logic
- Cannot use inequality (<, >, !=) in join conditions

## Transaction Boundaries

Can preserve, drop, or preserve detail pipeline boundaries.

## Behavior Rules

- Always designate the smaller source as Master for optimal cache performance
- Sorted input requires matching sort order on all join ports
- Cannot join more than two sources directly -- chain joiners for 3+ sources
- Joiner is a **blocking transformation** when unsorted
- Join conditions support equality only (no range joins)

## Spark Equivalent

```python
from pyspark.sql.functions import broadcast

# Normal join with broadcast hint for small table
result = large_df.join(
    broadcast(small_df),
    large_df.deptno == small_df.deptno,
    "inner"   # "left", "right", "full", "leftsemi", "leftanti"
)

# Without broadcast (shuffle join)
result = df1.join(df2, [df1.key == df2.key, df1.region == df2.region], "inner")

# Chain for 3+ sources
result = df1.join(df2, "key", "inner") \
    .join(df3, "key", "left") \
    .join(df4, "key", "inner")
```

## Edge Cases

- Blocking can cause pipeline stalls when master source is slow or large
- Self-join requires separate source instances (same table, different pipeline branches)
- Sorted joiner requires exact sort order match -- any mismatch causes incorrect results
- Large master sources in unsorted joiner may exceed memory and cause session failure

## Example

```
-- Joiner configuration:
-- Master: DEPT (small lookup table)
-- Detail: EMP (large fact table)
-- Join Condition: DEPT.DEPTNO = EMP.DEPTNO
-- Join Type: Normal Join (Inner)

-- Spark equivalent:
emp_df.join(broadcast(dept_df), "deptno", "inner")
```