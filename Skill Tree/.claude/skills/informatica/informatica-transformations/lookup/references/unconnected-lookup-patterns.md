# Unconnected Lookup Patterns

## Syntax

```
:LKP.lookup_transformation_name(argument1, argument2, ...)
```

- Called from Expression transformation
- Arguments map to lookup condition ports in order
- Returns single return port value
- Returns NULL if no match found

## Pattern 1: Single Value Lookup

```
-- Scenario: Get department name from department number
-- Lookup: lkp_dept (DEPTNO -> DNAME)
-- Expression port: OUT_DNAME = :LKP.lkp_dept(IN_DEPTNO)

# Spark equivalent:
@udf("string")
def lookup_dept_name(deptno):
    return dept_broadcast.value.get(deptno)
```

## Pattern 2: Conditional Lookup

```
-- Scenario: Use different lookups based on record type
-- Expression:
OUT_MANAGER = IIF(IN_RECORD_TYPE = 'EMP',
    :LKP.lkp_emp_manager(IN_EMP_ID),
    :LKP.lkp_temp_manager(IN_TEMP_ID)
)

# Spark equivalent:
df.withColumn("MANAGER",
    when(col("RECORD_TYPE") == "EMP",
         lookup_udf(col("EMP_ID")))
    .otherwise(lookup_temp_udf(col("TEMP_ID")))
)
```

## Pattern 3: Cascading Lookups

```
-- Scenario: Look up intermediate value, then use it for next lookup
-- Variable port: V_REGION = :LKP.lkp_office_region(IN_OFFICE_ID)
-- Output port: OUT_MANAGER = :LKP.lkp_region_manager(V_REGION)

# Spark equivalent:
df1 = df.join(office_df, "office_id", "left").select("*", office_df["region"].alias("V_REGION"))
result = df1.join(region_mgr_df, "region", "left").select("*", region_mgr_df["manager"].alias("OUT_MANAGER"))
```

## Pattern 4: Error Handling on No Match

```
-- Scenario: Default value when lookup fails
-- Expression:
OUT_COUNTRY = IIF(ISNULL(:LKP.lkp_country(IN_COUNTRY_CODE)),
    'UNKNOWN',
    :LKP.lkp_country(IN_COUNTRY_CODE)
)

# Better approach: single lookup call in variable
-- V_COUNTRY = :LKP.lkp_country(IN_COUNTRY_CODE)
-- OUT_COUNTRY = IIF(ISNULL(V_COUNTRY), 'UNKNOWN', V_COUNTRY)

# Spark equivalent:
df = df.join(country_df, df.country_code == country_df.code, "left") \
    .withColumn("COUNTRY", coalesce(country_df["name"], lit("UNKNOWN")))
```

## Pattern 5: Lookup in Update Strategy

```
-- Scenario: Determine insert vs update based on lookup
-- Lookup: lkp_target_key (BUSINESS_KEY -> SURROGATE_KEY)
-- Expression in Update Strategy:
IIF(ISNULL(:LKP.lkp_target_key(IN_BUSINESS_KEY)),
    DD_INSERT,
    DD_UPDATE
)

# Spark equivalent (Delta Lake MERGE):
spark.sql("""
    MERGE INTO target t
    USING source s ON t.business_key = s.business_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

## Limitations

- Single return value only
- Cannot return multiple columns in one call
- Each call rebuilds lookup context (less efficient than connected for repeated lookups)
- Must have exactly one return port defined
- Lookup condition ports must match argument count and order