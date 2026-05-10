---
name: informatica-repository-lineage
description: "Use when tracing data lineage in Informatica PowerCenter repository XML. Covers CONNECTOR elements, FROMFIELD/TOFIELD attributes, THROUGHPORT tracing, and end-to-end column-level lineage. Includes Spark column-level lineage via transformation tracing. Do NOT use for general data lineage questions."
---

# Informatica PowerCenter Repository Lineage Tracing

## When to Use

- Tracing a target column back to its source origin
- Understanding how a source column flows through transformations
- Impact analysis: identifying all targets affected by a source column change
- Debugging data quality issues by following data flow path
- Generating column-level lineage documentation for compliance

## When NOT to Use

- General data lineage questions about external systems (use data-lineage skills)
- File-level or table-level lineage only (use asset-lineage skills)
- Runtime data profiling or data quality assessment (use data-quality skills)
- Business glossary or semantic lineage (use business-glossary skills)

## CONNECTOR Elements: The Lineage Graph

Every field-to-field link in a mapping is a `CONNECTOR` element:

```xml
<CONNECTOR FROMINSTANCE="SQ_SALES_TRANSACTIONS" FROMFIELD="TXN_ID"
           TOINSTANCE="EXP_CALCULATE_KEYS" TOFIELD="TXN_ID_IN"/>
```

| Attribute | Meaning | Lineage Direction |
|-----------|---------|-------------------|
| `FROMINSTANCE` | Transformation producing the field | Upstream |
| `FROMFIELD` | Output field name from source transformation | Upstream |
| `TOINSTANCE` | Transformation consuming the field | Downstream |
| `TOFIELD` | Input field name in target transformation | Downstream |

The set of all CONNECTOR elements in a mapping forms a directed acyclic graph of data flow.

## Lineage Tracing Method

### Forward Tracing: Source → Target

Follow `FROMFIELD` → `TOFIELD` to see where a source column is used.

```
Source: RAW_CUSTOMERS.CUST_ID
    → CONNECTOR → SQ_CUSTOMERS.CUST_ID (pass-through)
    → CONNECTOR → EXP_STD.CUST_ID_IN (input)
    → EXP_STD.CUST_KEY_OUT (expression output: LKP_CUST_DIM(CUST_ID_IN))
    → CONNECTOR → TGT_CUSTOMERS.CUST_KEY (target field)
```

### Reverse Tracing: Target → Source

Follow `TOFIELD` → `FROMFIELD` to find the origin of a target column.

```python
def trace_lineage_reverse(mapping_xml, target_instance, target_field):
    """Trace lineage from target field back to source origin.

    Returns ordered list of lineage steps from target back to source.
    """
    lineage = []
    current_instance = target_instance
    current_field = target_field
    visited = set()

    while True:
        key = (current_instance, current_field)
        if key in visited:
            lineage.append({'warning': 'Cycle detected', 'instance': current_instance,
                          'field': current_field})
            break
        visited.add(key)

        # Find connector feeding this field
        xpath = f'.//CONNECTOR[@TOINSTANCE="{current_instance}"][@TOFIELD="{current_field}"]'
        connector = mapping_xml.find(xpath)

        if connector is None:
            # No upstream connector = this is a source (or generated field)
            lineage.append({
                'instance': current_instance,
                'field': current_field,
                'is_source': True
            })
            break

        step = {
            'to_instance': current_instance,
            'to_field': current_field,
            'from_instance': connector.get('FROMINSTANCE'),
            'from_field': connector.get('FROMFIELD'),
            'transformation_type': get_transformation_type(
                mapping_xml, connector.get('FROMINSTANCE')
            )
        }
        lineage.append(step)

        # Continue tracing from upstream
        current_instance = connector.get('FROMINSTANCE')
        current_field = connector.get('FROMFIELD')

    return list(reversed(lineage))


def get_transformation_type(mapping_xml, instance_name):
    """Get the TYPE of a transformation by its instance name."""
    instance = mapping_xml.find(f'.//INSTANCE[@NAME="{instance_name}"]')
    if instance is not None:
        return instance.get('TRANSFORMATIONTYPE', 'Unknown')
    # Check for embedded transformation
    tx = mapping_xml.find(f'.//TRANSFORMATION[@NAME="{instance_name}"]')
    return tx.get('TYPE', 'Unknown') if tx is not None else 'Unknown'
```

## Transformation-Specific Lineage

Each transformation type transforms data differently. Lineage logic must account for the transformation's behavior.

### Expression Transformation

```xml
<TRANSFORMATION NAME="EXP_CALC_KEYS" TYPE="Expression">
  <TRANSFORMFIELD NAME="TXN_DATE_IN" DATATYPE="date" PORTTYPE="INPUT"/>
  <TRANSFORMFIELD NAME="TXN_DATE_KEY" DATATYPE="number" PORTTYPE="OUTPUT"
                  EXPRESSION="TO_CHAR(TXN_DATE_IN,'YYYYMMDD')"/>
  <TRANSFORMFIELD NAME="V_CUST_EXISTS" DATATYPE="integer" PORTTYPE="VARIABLE"
                  EXPRESSION="IIF(ISNULL(LKP_CUST_DIM.CUST_KEY), 0, 1)"/>
</TRANSFORMATION>
```

Lineage rules for Expression:
- Each OUTPUT field lineage depends on fields referenced in its `EXPRESSION`
- VARIABLE ports are internal-only; they do not appear in CONNECTOR lineage
- An output may depend on multiple input fields (parse the expression string)
- Pass-through outputs (`EXPRESSION = INPUT_FIELD`) have direct lineage

```python
def expression_dependencies(tx_xml, output_field_name):
    """Extract input field dependencies from an expression."""
    output_field = tx_xml.find(f'.//TRANSFORMFIELD[@NAME="{output_field_name}"]')
    if output_field is None:
        return []

    expression = output_field.get('EXPRESSION', '')
    # Simple regex extraction of field references
    import re
    # Match field names (alphanumeric + underscore) that are likely references
    refs = re.findall(r'\b([A-Z_][A-Z0-9_]*)\b', expression)

    # Filter to actual input fields in this transformation
    input_fields = {
        f.get('NAME') for f in tx_xml.findall('TRANSFORMFIELD')
        if f.get('PORTTYPE') in ('INPUT', 'INPUT/OUTPUT')
    }
    return [r for r in refs if r in input_fields]
```

### Lookup Transformation

```xml
<TRANSFORMATION NAME="LKP_CUST_DIM" TYPE="Lookup Procedure">
  <TRANSFORMFIELD NAME="CUST_ID" DATATYPE="number" PRECISION="10" PORTTYPE="INPUT"/>
  <TRANSFORMFIELD NAME="CUST_KEY" DATATYPE="number" PRECISION="10" PORTTYPE="OUTPUT"/>
  <TRANSFORMFIELD NAME="CUST_NAME" DATATYPE="string" PRECISION="100" PORTTYPE="OUTPUT"/>
  <TABLEATTRIBUTE NAME="Lookup Table Name" VALUE="DWH.DIM_CUSTOMER"/>
  <TABLEATTRIBUTE NAME="Lookup Condition" VALUE="CUST_ID = CUST_ID"/>
</TRANSFORMATION>
```

Lineage rules for Lookup:
- INPUT ports are lookup conditions (e.g., `CUST_ID = CUST_ID`)
- OUTPUT ports come from the lookup table columns, not from input
- Lineage splits: input → lookup condition; output → lookup table column
- Dynamic lookup adds `NewLookupRow` output (generated, no source lineage)

```python
def lookup_lineage(tx_xml, output_field_name):
    """Determine source of a lookup output field."""
    field = tx_xml.find(f'.//TRANSFORMFIELD[@NAME="{output_field_name}"]')
    if field is None:
        return None

    port_type = field.get('PORTTYPE', '')
    if port_type == 'INPUT':
        return {'type': 'lookup_condition_input'}
    elif port_type == 'OUTPUT':
        table_name = tx_xml.find("TABLEATTRIBUTE[@NAME='Lookup Table Name']")
        return {
            'type': 'lookup_table_column',
            'table': table_name.get('VALUE') if table_name is not None else 'unknown',
            'column': output_field_name
        }
    elif port_type == 'LOOKUP':
        return {'type': 'lookup_return_port'}
    return {'type': 'unknown'}
```

### Aggregator Transformation

```xml
<TRANSFORMATION NAME="AGG_SALES_BY_DATE" TYPE="Aggregator">
  <TRANSFORMFIELD NAME="TXN_DATE_KEY" DATATYPE="number" PORTTYPE="GROUPBY"/>
  <TRANSFORMFIELD NAME="TOTAL_AMOUNT" DATATYPE="number" PORTTYPE="OUTPUT"
                  EXPRESSION="SUM(AMOUNT_IN)"/>
  <TRANSFORMFIELD NAME="TXN_COUNT" DATATYPE="integer" PORTTYPE="OUTPUT"
                  EXPRESSION="COUNT(*)"/>
</TRANSFORMATION>
```

Lineage rules for Aggregator:
- `GROUPBY` ports pass through to output (direct lineage)
- `OUTPUT` ports contain aggregate expressions (may reference multiple input fields)
- `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`, `FIRST`, `LAST` define the aggregation
- Group By fields determine the granularity of the output

### Joiner Transformation

```xml
<TRANSFORMATION NAME="JNR_SALES_CUST" TYPE="Joiner">
  <TRANSFORMFIELD NAME="M_CUST_ID" DATATYPE="number" PORTTYPE="MASTER"/>
  <TRANSFORMFIELD NAME="D_TXN_ID" DATATYPE="number" PORTTYPE="DETAIL"/>
  <TRANSFORMFIELD NAME="CUST_NAME" DATATYPE="string" PORTTYPE="OUTPUT"/>
  <TABLEATTRIBUTE NAME="Join Type" VALUE="Normal Join"/>
  <TABLEATTRIBUTE NAME="Join Condition" VALUE="M_CUST_ID = D_CUST_ID"/>
</TRANSFORMATION>
```

Lineage rules for Joiner:
- MASTER and DETAIL ports come from separate input pipelines
- OUTPUT ports select from either master or detail fields
- Join condition fields may not appear in output but affect which rows pass
- Null handling in outer joins can produce null outputs without upstream lineage

### Router Transformation

```xml
<TRANSFORMATION NAME="RTR_SALES_SPLIT" TYPE="Router">
  <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="number" PORTTYPE="INPUT/OUTPUT"/>
  <TRANSFORMFIELD NAME="TXN_TYPE" DATATYPE="string" PORTTYPE="INPUT/OUTPUT"/>
  <GROUP NAME="HIGH_VALUE" DESCRIPTION=""
         EXPRESSION="AMOUNT > 10000"/>
  <GROUP NAME="STANDARD" DESCRIPTION=""
         EXPRESSION="AMOUNT <= 10000"/>
  <GROUP NAME="DEFAULT" DESCRIPTION=""
         EXPRESSION=""/>
</TRANSFORMATION>
```

Lineage rules for Router:
- Same input fields appear in ALL output groups (one-to-many lineage)
- Group expressions filter which rows go to which output group
- DEFAULT group catches rows not matching any named group
- Always trace all output groups when doing impact analysis

### Filter Transformation

```xml
<TRANSFORMATION NAME="FIL_VALID_RECORDS" TYPE="Filter">
  <TRANSFORMFIELD NAME="CUST_ID" DATATYPE="number" PORTTYPE="INPUT/OUTPUT"/>
  <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="number" PORTTYPE="INPUT/OUTPUT"/>
  <TABLEATTRIBUTE NAME="Filter Condition"
                  VALUE="ISNULL(CUST_ID)=FALSE AND AMOUNT > 0"/>
</TRANSFORMATION>
```

Lineage rules for Filter:
- Pass-through transformation (input fields = output fields)
- Filter condition determines row routing, not column lineage
- Fields referenced in filter condition may affect row existence but do not change column values

### Sorter Transformation

Lineage rules for Sorter:
- Pure pass-through; output fields mirror input fields exactly
- Sort order does not affect column values or lineage
- `DISTINCT` option eliminates duplicate rows but preserves column lineage

### Sequence Generator Transformation

Lineage rules for Sequence Generator:
- `NEXTVAL` and `CURRVAL` are generated -- NO upstream lineage
- Mark as `generated` in lineage documentation

### Normalizer Transformation

Lineage rules for Normalizer:
- Input row is pivoted into multiple output rows (`OCCURS` attribute)
- `GK_` (Generated Key) and `GCID_` (Generated Column ID) are system-generated
- Generated columns have no source lineage -- mark as `generated`
- Multiple input fields may map to a single output field with `OCCURS`

### Union Transformation

```xml
<TRANSFORMATION NAME="UN_ALL_REGIONS" TYPE="Union">
  <TRANSFORMFIELD NAME="REGION_CODE" DATATYPE="string" PORTTYPE="INPUT/OUTPUT" GROUP="G1"/>
  <TRANSFORMFIELD NAME="REGION_CODE" DATATYPE="string" PORTTYPE="INPUT/OUTPUT" GROUP="G2"/>
  <TRANSFORMFIELD NAME="REGION_CODE" DATATYPE="string" PORTTYPE="INPUT/OUTPUT" GROUP="G3"/>
</TRANSFORMATION>
```

Lineage rules for Union:
- Output fields merge multiple input groups by field name
- All input groups must have matching field names and data types
- Lineage is many-to-one: multiple source instances feed one output field

## THROUGHPORT Attribute

Multi-group transformations use `THROUGHPORT` to identify which output group a field belongs to.

```xml
<!-- Router with multiple output groups -->
<CONNECTOR FROMINSTANCE="RTR_SALES_SPLIT" FROMFIELD="AMOUNT"
           TOINSTANCE="TGT_HIGH_VALUE" TOFIELD="AMOUNT"
           THROUGHPORT="HIGH_VALUE"/>
<CONNECTOR FROMINSTANCE="RTR_SALES_SPLIT" FROMFIELD="AMOUNT"
           TOINSTANCE="TGT_STANDARD" TOFIELD="AMOUNT"
           THROUGHPORT="STANDARD"/>
```

Always include `THROUGHPORT` when tracing lineage through Router, Union, or custom multi-group transformations. Without it, you cannot distinguish which output group a field came from.

## Special Fields Without Source Lineage

| Field Type | Generated By | Lineage Status |
|------------|-------------|----------------|
| `GK_` (Generated Key) | Normalizer | Generated -- no source |
| `GCID_` (Generated Column ID) | Normalizer | Generated -- no source |
| Variable ports (`V_`) | Expression | Internal -- no external lineage |
| `NEXTVAL` / `CURRVAL` | Sequence Generator | Generated -- no source |
| `RANKINDEX` | Rank transformation | Generated -- no source |
| `NewLookupRow` | Dynamic Lookup | Generated -- indicates insert/update |
| `SYSDATE`, `SESSSTARTTIME` | Built-in variables | Session runtime -- no source lineage |
| `MAPPINGNAME`, `FOLDERNAME` | Built-in variables | Metadata -- no source lineage |

Always identify these fields during lineage tracing and mark them with `source_type: generated` rather than leaving lineage incomplete.

## Complete Lineage Query Pattern

```python
def full_column_lineage(mapping_xml, target_instance, target_field):
    """Full reverse lineage from target field to all source origins.

    Returns a tree structure showing all paths from target back to sources,
    including transformation-specific metadata at each step.
    """
    def trace_step(instance, field, visited):
        if (instance, field) in visited:
            return {'instance': instance, 'field': field, 'cycle': True}
        visited = visited | {(instance, field)}

        # Find upstream connectors
        xpath = f'.//CONNECTOR[@TOINSTANCE="{instance}"][@TOFIELD="{field}"]'
        connectors = mapping_xml.findall(xpath)

        if not connectors:
            # Source field (no upstream)
            return {
                'instance': instance,
                'field': field,
                'is_source': True,
                'source_type': 'source_definition'
            }

        paths = []
        for conn in connectors:
            from_inst = conn.get('FROMINSTANCE')
            from_field = conn.get('FROMFIELD')
            through_port = conn.get('THROUGHPORT', '')

            # Get transformation details
            tx_type = get_transformation_type(mapping_xml, from_inst)
            tx_xml = get_transformation_xml(mapping_xml, from_inst)

            step = {
                'from_instance': from_inst,
                'from_field': from_field,
                'to_instance': instance,
                'to_field': field,
                'transformation_type': tx_type,
                'through_port': through_port,
                'upstream': trace_step(from_inst, from_field, visited)
            }

            # Add transformation-specific lineage details
            if tx_type == 'Expression' and tx_xml is not None:
                step['expression_dependencies'] = expression_dependencies(
                    tx_xml, from_field
                )
            elif tx_type in ('Sequence Generator', 'Rank', 'Normalizer'):
                step['generated_field'] = True

            paths.append(step)

        return paths[0] if len(paths) == 1 else {'branches': paths}

    return trace_step(target_instance, target_field, set())


def get_transformation_xml(mapping_xml, instance_name):
    """Get the TRANSFORMATION element for an instance."""
    instance = mapping_xml.find(f'.//INSTANCE[@NAME="{instance_name}"]')
    if instance is None:
        return mapping_xml.find(f'.//TRANSFORMATION[@NAME="{instance_name}"]')
    tx_name = instance.get('TRANSFORMATIONNAME', instance_name)
    return mapping_xml.find(f'.//TRANSFORMATION[@NAME="{tx_name}"]')
```

## Impact Analysis: Reverse Lineage

Forward tracing finds all targets affected by a source change.

```python
def forward_impact(mapping_xml, source_instance, source_field):
    """Find all downstream targets affected by changing a source field."""
    affected = []
    to_process = [(source_instance, source_field)]
    visited = set()

    while to_process:
        current_inst, current_field = to_process.pop(0)
        key = (current_inst, current_field)
        if key in visited:
            continue
        visited.add(key)

        # Find downstream connectors
        xpath = f'.//CONNECTOR[@FROMINSTANCE="{current_inst}"][@FROMFIELD="{current_field}"]'
        for conn in mapping_xml.findall(xpath):
            to_inst = conn.get('TOINSTANCE')
            to_field = conn.get('TOFIELD')

            # Check if this is a target
            instance = mapping_xml.find(f'.//INSTANCE[@NAME="{to_inst}"]')
            if instance is not None and instance.get('TYPE') == 'Target':
                affected.append({
                    'target_instance': to_inst,
                    'target_field': to_field,
                    'via_instance': current_inst,
                    'via_field': current_field
                })
            else:
                to_process.append((to_inst, to_field))

    return affected
```

## Lineage Output Format

Always produce structured lineage output for downstream consumption:

```json
{
  "mapping_name": "m_SALES_FACT_LOAD",
  "target_field": "TGT_SALES_FACT.AMOUNT",
  "lineage_path": [
    {
      "instance": "TGT_SALES_FACT",
      "field": "AMOUNT",
      "type": "Target",
      "step": 0
    },
    {
      "instance": "EXP_CALC_KEYS",
      "field": "AMOUNT",
      "type": "Expression",
      "expression": "AMOUNT_IN",
      "step": 1
    },
    {
      "instance": "SQ_SALES_TRANSACTIONS",
      "field": "AMOUNT",
      "type": "Source Qualifier",
      "step": 2
    },
    {
      "instance": "SRC_TRANSACTIONS",
      "field": "AMOUNT",
      "type": "Source",
      "is_source": true,
      "step": 3
    }
  ],
  "source_type": "source_definition",
  "transformations_count": 1
}
```

This format is consumable by data catalogs, compliance tools, and impact analysis systems.
