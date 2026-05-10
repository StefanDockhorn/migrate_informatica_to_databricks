# Dynamic Lookup Examples

## Dynamic Cache Behavior

Dynamic lookup maintains a cache that is updated during the session. Used for slowly changing dimensions (SCD) and target table lookups.

## NewLookupRow Port

| Value | Meaning |
|-------|---------|
| 0 | No change (row found, no update needed) |
| 1 | Insert (row not in cache) |
| 2 | Update (row found, needs update) |

## SCD Type 1 Example (Overwrite)

```
-- Dynamic Lookup on target dimension table
-- Lookup Condition: CUSTOMER_ID = IN_CUSTOMER_ID
-- Insert Else Update policy

-- Mapping logic:
-- 1. Source -> Expression (calculate hash/compare columns)
-- 2. Dynamic Lookup on target (check if customer exists)
-- 3. Router based on NewLookupRow:
--    - NewLookupRow = 1 (Insert) -> Insert target
--    - NewLookupRow = 2 (Update) -> Update target
--    - NewLookupRow = 0 (NoChange) -> Drop
```

## SCD Type 2 Example (Track History)

```
-- Dynamic Lookup on current dimension record
-- Additional ports: EFFECTIVE_DATE, EXPIRY_DATE, IS_CURRENT

-- Mapping logic:
-- 1. Source -> Dynamic Lookup (find current record)
-- 2. If NewLookupRow = 2 (Update):
--    a. Update existing record: SET EXPIRY_DATE = current, IS_CURRENT = 'N'
--    b. Insert new record: EFFECTIVE_DATE = current, IS_CURRENT = 'Y'
-- 3. If NewLookupRow = 1 (Insert): Insert with IS_CURRENT = 'Y'
```

## Delta Lake MERGE Equivalent

```python
# SCD Type 1 (overwrite)
spark.sql("""
    MERGE INTO customers t
    USING source_updates s ON t.customer_id = s.customer_id
    WHEN MATCHED AND s.hash != t.hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# SCD Type 2 (track history)
spark.sql("""
    MERGE INTO customers t
    USING source_updates s ON t.customer_id = s.customer_id AND t.is_current = 'Y'
    WHEN MATCHED THEN UPDATE SET expiry_date = current_date(), is_current = 'N'
""")

# Then insert new current records
new_current = spark.sql("""
    SELECT s.*, current_date() as effective_date, '9999-12-31' as expiry_date, 'Y' as is_current
    FROM source_updates s
""")
new_current.write.mode("append").saveAsTable("customers")
```

## Dynamic Lookup vs Static Lookup

| Aspect | Static Cache | Dynamic Cache |
|--------|-------------|---------------|
| Cache updates | Read-only during session | Inserted/updated during session |
| Use case | Reference data lookup | SCD, target table lookup |
| NewLookupRow | N/A | Indicates Insert/Update/NoChange |
| Performance | Faster (no cache writes) | Slower (cache modifications) |
| Source | Any source type | Relational or pipeline only |