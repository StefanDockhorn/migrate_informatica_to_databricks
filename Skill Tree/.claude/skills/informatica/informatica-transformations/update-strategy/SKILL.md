---
name: informatica-update-strategy
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Update Strategy transformations. Covers DD_INSERT, DD_UPDATE, DD_DELETE, DD_REJECT flags and their interaction with session properties. Includes Delta Lake MERGE and Spark write mode equivalents. Do NOT use for general CDC patterns."
---

# Update Strategy Transformation

## Purpose

Active transformation that flags rows for insert, update, delete, or reject.

## Constants

| Constant | Value | Action |
|----------|-------|--------|
| DD_INSERT | 0 | Insert row into target |
| DD_UPDATE | 1 | Update existing row in target |
| DD_DELETE | 2 | Delete row from target |
| DD_REJECT | 3 | Drop row (or forward to reject target) |

## Expression

Sets strategy for each row based on expression evaluation. Output is one of the four constants above.

## Session Integration

Session "Treat Source Rows As" property must align with Update Strategy output:

| Session Setting | Behavior |
|-----------------|----------|
| Data Driven | Honors Update Strategy flags |
| Insert | All rows treated as insert (Update/Delete rejected) |
| Update | All rows treated as update |
| Delete | All rows treated as delete |

## Forwarding Rejected Rows

Rejected rows can be forwarded to a separate target for audit/review instead of being dropped.

## Behavior Rules

- DD_REJECT drops row unless "Forward Rejected Rows" is enabled
- Session property "Treat Source Rows As" must be "Data Driven" to honor Update Strategy
- If session is set to Insert but Update Strategy outputs Update, row is rejected
- Always use with Target "Update else Insert" or "Insert else Update" for SCD Type 2
- Combined with Lookup for slowly changing dimension detection
- Aggregator outputs single row that can be flagged for update/delete

## Spark Equivalent

```python
from delta.tables import DeltaTable

# INSERT: append mode
df.write.format("delta").mode("append").save("/path/to/target")

# UPDATE/DELETE: Delta Lake MERGE INTO
delta_table = DeltaTable.forPath(spark, "/path/to/target")
delta_table.alias("t").merge(
    updates_df.alias("s"),
    "t.key = s.key"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# REJECT: filter to separate DataFrame
rejected_df = df.filter(col("REJECT_FLAG") == True)
valid_df = df.filter(col("REJECT_FLAG") == False)
```

## Example

```
-- IIF(UPDATED_DATE > LAST_RUN_DATE, DD_UPDATE, DD_INSERT)

# Spark equivalent (Delta Lake MERGE):
spark.sql("""
    MERGE INTO target t
    USING source s ON t.key = s.key
    WHEN MATCHED AND s.updated_date > s.last_run_date THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```