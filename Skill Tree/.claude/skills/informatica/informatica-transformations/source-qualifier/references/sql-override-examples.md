# SQL Override Reference

> **When to use this file:** When the main `informatica-source-qualifier` skill does not provide enough detail for complex SQL override scenarios. Reference when migrating SQL overrides to Spark SQL.

## SQL Override Fundamentals

Always list SELECT fields in the same order as Source Qualifier output ports. The Integration Service expects port-to-column positional alignment, not name matching.

```sql
-- CORRECT: Columns match port order (EMPNO, ENAME, SAL, DEPTNO)
SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE SAL > 0

-- WRONG: Columns out of order — causes data misalignment
SELECT ENAME, EMPNO, DEPTNO, SAL FROM EMP WHERE SAL > 0
```

## Common Override Patterns

### Incremental Extract with Parameter
```sql
-- Informatica
SELECT * FROM ORDERS WHERE LAST_MODIFIED > TO_DATE('$$LAST_EXTRACT_DATE', 'YYYY-MM-DD HH24:MI:SS')

-- Spark equivalent
spark.read.jdbc(url, "(SELECT * FROM orders WHERE last_modified > '{}') AS incr".format(last_extract_date), props)
```

### Join Override for Relational Sources
```sql
-- Informatica: Heterogeneous join via database link or SQL override
SELECT A.EMPNO, A.ENAME, B.DNAME, A.SAL
FROM EMP A, DEPT B
WHERE A.DEPTNO = B.DEPTNO(+)
AND A.HIRE_DATE > SYSDATE - 90

-- Spark equivalent
emp = spark.table("emp").filter(col("hire_date") > current_date() - 90)
dept = spark.table("dept")
result = emp.join(dept, "deptno", "left")
```

### Sorted Ports with Override
When using sorted ports, the ORDER BY must match the sort key order exactly:
```sql
-- If sorted ports are DEPTNO (ascending), SAL (descending)
SELECT * FROM EMP ORDER BY DEPTNO ASC, SAL DESC
```

### Override with Pre/Post SQL
```sql
-- Pre-SQL: Create temp index for session
CREATE INDEX IDX_EMP_SESSION ON EMP(HIRE_DATE) NOLOGGING

-- Main SQL
SELECT * FROM EMP WHERE HIRE_DATE BETWEEN $$START_DATE AND $$END_DATE

-- Post-SQL: Drop temp index
DROP INDEX IDX_EMP_SESSION
```

## Spark SQL Equivalent Patterns

| Informatica SQL Feature | Spark SQL Approach |
|---|---|
| `$$Parameter` | String interpolation or `spark.conf.get()` |
| `SYSDATE` | `current_date()` or `current_timestamp()` |
| `TO_DATE` | `to_date(string, format)` |
| `NVL(expr, 0)` | `coalesce(expr, lit(0))` |
| `ROWNUM` | `row_number().over(Window.orderBy())` |
| `DUAL` | No equivalent — use `SELECT current_date()` directly |
| Database link joins | Read both sources into DataFrames, then join |

## Error-Prone Patterns

- **Trailing semicolons**: Remove `;` from SQL override — Informatica sometimes rejects it
- `SELECT *`: Never use — always list columns explicitly
- **Format mismatches**: Informatica `YYYY-MM-DD HH24:MI:SS` vs Spark `yyyy-MM-dd HH:mm:ss`
- **Bind variables**: `?` placeholders not supported in SQL override — use $$parameters instead
