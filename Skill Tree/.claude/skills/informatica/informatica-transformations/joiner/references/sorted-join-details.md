# Sorted Join Details

> **When to use this file:** When the main `informatica-joiner` skill does not provide enough depth on sorted input configuration, sort-merge mechanics, and integration with other sorted transformations.

## Sorted Join Requirements

For a sorted join to function correctly, both inputs must be sorted on **all join key columns in the same order** as specified in the join condition.

```
Join Condition: EMP.DEPTNO = DEPT.DEPTNO

Required sort on EMP:  DEPTNO (Ascending)
Required sort on DEPT: DEPTNO (Ascending)

Required sort on EMP:  DEPTNO (Ascending), EMPNO (Ascending)
Required sort on DEPT: DEPTNO (Ascending), DNAME (Ascending)
```

The Integration Service verifies sort order at runtime. A mismatch produces either incorrect results or a session failure (depending on session configuration).

## Sort-Merge Join Mechanics

1. Both sorted streams are read in key order
2. The joiner advances through both streams without caching either entirely
3. Memory usage is O(1) per key group — only matching key groups are held
4. Output is produced in sort-key order

This is equivalent to Spark's **SortMergeJoin** — the default join strategy when both sides are large.

```python
# Spark: pre-sorting not required — Catalyst optimizer handles it
# But if data is already sorted, preserve it:
emp_sorted = emp.repartition("DEPTNO").sortWithinPartitions("DEPTNO")
dept_sorted = dept.repartition("DEPTNO").sortWithinPartitions("DEPTNO")
result = emp_sorted.join(dept_sorted, "DEPTNO", "inner")
```

## Sorted Input Chain

When chaining sorted transformations, all intermediate sorts must be compatible:

```
Source → Sorter (DEPTNO ASC) → Joiner (DEPTNO = DEPTNO) → Aggregator (Group By DEPTNO)
                                                                    ↑
                                          Aggregator sorted input also requires DEPTNO sorted
```

If the joiner output is not sorted on DEPTNO (some join types reorder), add a Sorter before the Aggregator.

## Sorted Join vs Unsorted Join Decision Matrix

| Scenario | Use Sorted? | Why |
|---|---|---|
| Both sources already sorted | Yes | Avoids full master cache, O(1) memory |
| Master source < 25% of executor memory | No (unsorted + broadcast) | Broadcast is faster |
| Master source very large | Yes | Unsorted would OOM |
| Join followed by Aggregator (same key) | Yes | Chain sorted inputs |
| Self-join (same table) | Yes | Preserves sort from initial read |

## Common Sorted Join Mistakes

- Sorting on **output ports** instead of **input ports** — sort must happen before the joiner
- Mismatched sort direction (ASC vs DESC) on the two inputs
- Sorting on a subset of join keys — all join condition ports must be in the sort
- Assuming join output preserves sort order — only inner sorted join does; outer joins may not
