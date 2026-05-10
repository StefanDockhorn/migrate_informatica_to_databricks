---
name: informatica-xml-source-qualifier
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter XML Source Qualifier transformations. Covers XML parsing, schema validation, XPath queries, hierarchical data flattening, and XML views. Includes Spark read.format('xml') and explode() equivalents. Do NOT use for general XML processing."
---

# XML Source Qualifier Transformation

## Purpose

Reads XML data from sources; flattens hierarchical XML into relational rows.

## XML Views

Defines relational view of hierarchical XML:
- **Entity Relationships**: Parent-child element relationships
- **Hierarchy Relationships**: Defines the XML nesting structure
- All XML views must have a primary key (generated if no natural key exists)

## XML Parser Midstream

Parses XML data from pipeline (not just source). Receives XML as string/varchar from upstream transformation.

## XML Generator

Creates XML output from relational data. Converts rows back to XML hierarchy.

## Schema Support

| Schema Type | Support |
|-------------|---------|
| DTD | Yes |
| XML Schema (XSD) | Yes |
| Inline Schema | Yes |

## Datatype Mapping

Maps XML schema types to Informatica transformation datatypes:

| XML Schema Type | Informatica Type |
|-----------------|------------------|
| xs:string | String |
| xs:integer | Integer |
| xs:decimal | Decimal |
| xs:date | DateTime |
| xs:boolean | Integer (0/1) |

## XPath Query

Supports XPath predicates for filtering XML data within the source qualifier.

## Cardinality

Handles:
- One-to-one relationships
- One-to-many relationships
- Many-to-many relationships (via intermediate elements)

## Behavior Rules

- XML source qualifier always paired with XML source definition
- All XML views must have a primary key (generated if not natural)
- Pivoting columns converts multiple-occurring elements to separate columns
- Schema synchronization required when XML structure changes
- XML Parser (midstream) requires XML string input from upstream transformation
- XML Generator creates hierarchical output; must define output schema

## Spark Equivalent

```python
from pyspark.sql.functions import explode, col, xpath, xpath_string

# Read XML file
df = spark.read.format("xml") \
    .option("rowTag", "Employee") \
    .load("/path/to/employees.xml")

# Handle nested elements with explode
df = spark.read.format("xml").option("rowTag", "Department").load("/path/to/dept.xml")
df_flat = df.select(
    col("_id").alias("DEPT_ID"),
    col("DeptName"),
    explode("Employees.Employee").alias("emp")
)
df_flat.select(
    col("DEPT_ID"),
    col("DeptName"),
    col("emp.EmpNo").alias("EMPNO"),
    col("emp.Name").alias("ENAME")
)

# XML from string column (midstream parser)
from pyspark.sql.functions import from_xml, schema_of_xml
df = df.withColumn("parsed",
    from_xml(col("xml_string_column"), schema_of_xml(lit("<Employee><Name>John</Name></Employee>")))
)

# Explode arrays for one-to-many relationships
df.select(explode(col("Items.Item")).alias("item")).select("item.*")
```

## Example

```xml
<!-- XML Source -->
<Employees>
  <Employee>
    <EmpNo>1</EmpNo>
    <Name>John</Name>
    <Department>Sales</Department>
  </Employee>
  <Employee>
    <EmpNo>2</EmpNo>
    <Name>Jane</Name>
    <Department>Marketing</Department>
  </Employee>
</Employees>
```

```python
# Spark equivalent:
df = spark.read.format("xml") \
    .option("rowTag", "Employee") \
    .load("/path/to/employees.xml")

# Result schema: [EmpNo: bigint, Name: string, Department: string]
```