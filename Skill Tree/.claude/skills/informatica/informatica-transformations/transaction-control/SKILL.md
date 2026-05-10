---
name: informatica-transaction-control
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Transaction Control transformations. Covers commit/rollback boundaries, transaction control expressions, and scoped transactions. Includes Spark batch write and Delta Lake transaction boundaries. Do NOT use for general transaction management."
---

# Transaction Control Transformation

## Purpose

Active transformation that defines commit and rollback boundaries within a session.

## Transaction Control Expression

Evaluated per row; returns one of these constants:

| Constant | Behavior |
|----------|----------|
| TC_COMMIT_BEFORE | Commit current transaction, start new one BEFORE processing current row |
| TC_COMMIT_AFTER | Commit AFTER processing current row |
| TC_ROLLBACK_BEFORE | Rollback current transaction, start new one BEFORE current row |
| TC_ROLLBACK_AFTER | Rollback AFTER processing current row |
| TC_CONTINUE_TRANSACTION | Continue current transaction (default) |

## Transaction Scope

Determines which targets participate in the transaction:

| Scope | Behavior |
|-------|----------|
| Transaction | Only transformations within scope participate |
| All Input | All targets in mapping commit/rollback together |

## Multiple Targets

All targets in the transaction scope commit or rollback together based on the control expression.

## Behavior Rules

- `TC_COMMIT_BEFORE` commits current transaction and starts new one BEFORE processing current row
- `TC_COMMIT_AFTER` commits AFTER processing current row (row is included in committed transaction)
- If no transaction control expression set, default is `TC_CONTINUE_TRANSACTION`
- Target connection settings must allow transaction control (not all database types support it)
- Rollback applies to all targets in the transaction scope
- Frequent commits can degrade performance -- balance between data integrity and throughput

## Spark Equivalent

```python
# Spark manages transactions at write level
# Each batch write is implicitly a transaction

# Append (implicit commit per write)
df.write.format("delta").mode("append").save("/path/to/target")

# Delta Lake auto-commit per batch
# For explicit transactions, use JDBC:
conn = jdbc_connection()
conn.setAutoCommit(False)
try:
    cursor.execute("INSERT INTO ...")
    cursor.execute("UPDATE ...")
    conn.commit()
except:
    conn.rollback()
finally:
    conn.close()

# Micro-batch transaction boundaries in Structured Streaming
(df.writeStream
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.mode("append").save("..."))
    .start())
```

## Example

```
-- IIF(CUSTOMER_ID != PREV_CUSTOMER_ID, TC_COMMIT_BEFORE, TC_CONTINUE_TRANSACTION)
-- Commits transaction and starts new one when CUSTOMER_ID changes

# Spark equivalent: repartition by customer + batch processing
(df.repartition("CUSTOMER_ID")
   .write
   .partitionBy("CUSTOMER_ID")
   .mode("append")
   .save("..."))
```