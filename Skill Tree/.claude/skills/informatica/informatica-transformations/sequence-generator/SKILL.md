---
name: informatica-sequence-generator
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Sequence Generator transformations. Covers NEXTVAL, CURRVAL, cycle behavior, and cached values. Includes Spark monotonically_increasing_id() and row_number() equivalents. Do NOT use for general sequence generation patterns."
---

# Sequence Generator Transformation

## Purpose

Passive transformation that generates sequential numbers.

## Ports

| Port | Type | Description |
|------|------|-------------|
| CURRVAL | Output | Current value (last generated NEXTVAL) |
| NEXTVAL | Output (required) | Next sequential number; advances sequence |

## Properties

| Property | Description |
|----------|-------------|
| Start Value | First value in sequence |
| Increment By | Step size between values |
| End Value | Maximum value before cycle or error |
| Cycle | When enabled, wraps from End Value back to Start Value |
| Current Value | Starting point for non-reusable sequences |
| Number of Cached Values | Values cached in memory; higher = fewer DB round trips |

## NEXTVAL

Returns next sequential number each time called. Primary port for sequence generation. Advances the sequence by Increment By.

## CURRVAL

Returns current value (last generated NEXTVAL). Does **not** advance sequence. `CURRVAL = NEXTVAL - Increment By`.

## Cycle

When enabled, wraps from End Value back to Start Value. Must be enabled if data volume exceeds the sequence range.

## Cached Values

Integration Service caches sequence values. Higher cache reduces database round trips but may leave gaps on session abort.

## Reusable vs Non-Reusable

| Type | Scope | Behavior |
|------|-------|----------|
| Reusable | Shared across mappings | Single sequence shared by all sessions |
| Non-Reusable | Per mapping instance | Independent sequence per session |

## Behavior Rules

- CURRVAL is always NEXTVAL - Increment By
- Sequences may have gaps due to cached values or session aborts
- Cycle must be enabled if data volume exceeds (End Value - Start Value) / Increment By
- Non-reusable sequence generators start at Current Value for each session
- Reusable sequences shared across mappings may create gaps between sessions
- Number of Cached Values = 0 means no caching (fetches from database each time)

## Spark Equivalent

```python
from pyspark.sql.functions import monotonically_increasing_id, row_number, lit
from pyspark.sql.window import Window

# Simple surrogate key (not gap-free, not deterministic across runs)
df = df.withColumn("SURROGATE_KEY", monotonically_increasing_id())

# Gap-free row number (requires ordering)
window_spec = Window.orderBy("EMPNO")
df = df.withColumn("SEQ", row_number().over(window_spec))

# Deterministic sequence with start and increment
from pyspark.sql.functions import expr
df = df.withColumn("SEQ", expr("id + 1"))  # Requires id column

# Using zipWithIndex (RDD-based, gap-free)
rdd = df.rdd.zipWithIndex().map(lambda row, idx: (*row, idx + 1))
```

## Example

```
-- Start Value: 1
-- Increment By: 1
-- End Value: 999999
-- Cycle: No
-- Cached Values: 1000
-- NEXTVAL connected to target surrogate key

# Spark equivalent (gap-free surrogate key):
from pyspark.sql.window import Window
window_spec = Window.orderBy("EMPNO")
df.withColumn("SURROGATE_KEY", row_number().over(window_spec))
```