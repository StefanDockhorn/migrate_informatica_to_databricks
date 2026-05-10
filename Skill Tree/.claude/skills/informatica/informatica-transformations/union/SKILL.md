---
name: informatica-union
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Union transformations. Covers input groups, output ports, and data type matching rules. Includes Spark union() and unionByName() equivalents. Do NOT use for general set operations."
---

# Union Transformation

## Purpose

Active transformation that merges multiple pipelines with matching structures.

## Input Groups

Multiple input groups (2 or more). All must have matching port names and compatible data types.

## Output

Single output group containing all rows from all input groups. Union does **not** eliminate duplicates.

## Port Matching

Ports merged by name; data types must be compatible. Output port precision derives from the widest input port.

## Behavior Rules

- All input groups must have identical port names and compatible types
- Union does **not** eliminate duplicates (use Sorter with Distinct for that)
- Output ports derive precision from widest input port
- Union is **active** -- generates multiple output rows per input set
- Number of output rows = sum of rows from all input groups
- Cannot use Union with mismatched port names -- rename ports upstream if needed

## Spark Equivalent

```python
# Union by position (column order must match)
result = df1.union(df2).union(df3)

# Union by name (column names must match, order doesn't matter)
result = df1.unionByName(df2).unionByName(df3)

# Union with allowMissingColumns (fills nulls for missing columns)
result = df1.unionByName(df2, allowMissingColumns=True)

# Union then distinct (union + deduplication)
result = df1.union(df2).dropDuplicates()

# Note: Spark union preserves duplicates (same as Informatica Union)
# Use dropDuplicates() or distinct() to remove duplicates
```

## Example

```
-- Input Group 1: EMP_CURRENT (EMPNO, ENAME, SAL)
-- Input Group 2: EMP_ARCHIVE (EMPNO, ENAME, SAL)
-- Output: All rows from both tables (duplicates preserved)

# Spark equivalent:
result = emp_current_df.union(emp_archive_df)

# With deduplication (add Sorter + Distinct):
result = emp_current_df.union(emp_archive_df).dropDuplicates()
```