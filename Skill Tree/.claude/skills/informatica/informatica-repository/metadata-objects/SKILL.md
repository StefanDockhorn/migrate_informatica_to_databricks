---
name: informatica-repository-metadata
description: "Use when analyzing Informatica PowerCenter repository metadata objects. Covers folders, mappings, mapplets, shortcuts, reusable transformations, and their relationships. Includes metadata lineage and impact analysis patterns. Do NOT use for general metadata management."
---

# Informatica PowerCenter Repository Metadata Objects

## When to Use

- Understanding repository object hierarchy and relationships
- Planning reuse strategy for transformations and mapplets
- Organizing folders for multi-team development
- Performing impact analysis before changes
- Migrating objects between repositories or environments

## When NOT to Use

- General data catalog or metadata management (use data-catalog skills)
- Database schema design (use database-design skills)
- Code review of individual transformation logic (use transformation-logic skills)

## Folders

Folders are the top-level logical containers. Every object lives in exactly one folder.

```xml
<FOLDER NAME="FN_SALES" DESCRIPTION="Sales ETL processes"
        SHARED="NO" OWNER="etl_admin" GROUP="etl_devs"
        PERMISSIONS="rwxr-xr-x">
  <!-- All objects scoped to this folder -->
</FOLDER>
```

### Folder Organization Rules

Always organize folders by functional domain, not by environment:

| Pattern | Example | Rationale |
|---------|---------|-----------|
| By subject area | `FN_SALES`, `FN_FINANCE`, `FN_HR` | Teams own domains |
| By layer | `FN_STAGING`, `FN_ODS`, `FN_DWH` | Pipeline stages |
| By project | `FN_PRJ_UPGRADE_2025` | Temporary isolation |
| By source system | `FN_SAP`, `FN_SALESFORCE`, `FN_ORACLE` | Source-team alignment |

Avoid:
- Environment-based folders (`FN_DEV`, `FN_PROD`) -- use repository separation instead
- Single-object folders -- creates maintenance overhead
- Deeply nested folder hierarchies -- PowerCenter has no subfolder concept

### Shared Folders

```xml
<FOLDER NAME="FN_SHARED" SHARED="YES" OWNER="admin" GROUP="all_devs">
  <SOURCE NAME="DIM_DATE" DESCRIPTION="Shared date dimension"/>
  <TRANSFORMATION NAME="EXP_STANDARDIZE_DATES" TYPE="Expression" REUSABLE="YES"/>
</FOLDER>
```

A shared folder exposes its objects to other folders via shortcuts. Always place enterprise-standard objects (date dimensions, common lookups, standardization expressions) in shared folders.

## Mappings

A mapping is a complete ETL specification: sources, targets, transformations, and data flow connectors.

```
Mapping: m_SALES_FACT_LOAD
├── INSTANCE: SRC_TRANSACTIONS (Source)
├── INSTANCE: SQ_TRANSACTIONS (Source Qualifier)
├── TRANSFORMATION: SQ_TRANSACTIONS [non-reusable]
│   ├── TRANSFORMFIELD: TXN_ID
│   ├── TRANSFORMFIELD: TXN_DATE
│   └── TABLEATTRIBUTE: Sql Query = "SELECT * FROM STG.TRANSACTIONS"
├── INSTANCE: EXP_CALC_KEYS (Expression)
├── TRANSFORMATION: EXP_CALC_KEYS [non-reusable]
│   ├── TRANSFORMFIELD: TXN_ID_IN (INPUT)
│   ├── TRANSFORMFIELD: TXN_DATE_KEY (OUTPUT)
│   │   └── EXPRESSION = "TO_CHAR(TXN_DATE_IN,'YYYYMMDD')"
│   └── TRANSFORMFIELD: CUST_KEY (OUTPUT)
│       └── EXPRESSION = "LKP_CUST_DIM(CUST_ID_IN)"
├── INSTANCE: LKP_CUST_DIM (Lookup) → references reusable LKP_CUST_DIM
├── INSTANCE: TGT_SALES_FACT (Target)
├── CONNECTOR: SRC_TRANSACTIONS.TXN_ID → SQ_TRANSACTIONS.TXN_ID
├── CONNECTOR: SQ_TRANSACTIONS.TXN_ID → EXP_CALC_KEYS.TXN_ID_IN
├── CONNECTOR: EXP_CALC_KEYS.TXN_DATE_KEY → TGT_SALES_FACT.TXN_DATE_KEY
└── CONNECTOR: EXP_CALC_KEYS.CUST_KEY → TGT_SALES_FACT.CUST_KEY
```

### Mapping Design Rules

- Always name mappings with `m_` prefix for quick identification
- Source Qualifier must immediately follow each source -- no transformations between them
- Target definitions must be the terminal nodes in the data flow graph
- Every source field must have at least one outgoing CONNECTOR (or be intentionally unused)
- Every target field must have at least one incoming CONNECTOR

## Mapplets

A mapplet is a reusable subset of a mapping: a collection of transformations with defined input and output interfaces.

```
Mapplet: mplt_STANDARDIZE_CUSTOMER
├── INPUT transformation: IN_CUST_DATA
│   ├── CUST_ID (number)
│   ├── CUST_NAME (string)
│   ├── CUST_EMAIL (string)
│   └── CUST_PHONE (string)
├── TRANSFORMATION: EXP_STANDARDIZE
│   ├── CUST_NAME_IN (INPUT)
│   ├── CUST_EMAIL_IN (INPUT)
│   ├── CUST_PHONE_IN (INPUT)
│   ├── STD_NAME (OUTPUT) → UPPER(LTRIM(RTRIM(CUST_NAME_IN)))
│   ├── STD_EMAIL (OUTPUT) → LOWER(LTRIM(RTRIM(CUST_EMAIL_IN)))
│   └── STD_PHONE (OUTPUT) → REG_REPLACE(CUST_PHONE_IN,'[^0-9]','')
├── TRANSFORMATION: LKP_CUST_TYPE
│   ├── DOMAIN (INPUT) → from email extension
│   └── CUST_TYPE_KEY (OUTPUT)
├── OUTPUT transformation: OUT_STD_CUST
│   ├── CUST_ID
│   ├── STD_NAME
│   ├── STD_EMAIL
│   ├── STD_PHONE
│   └── CUST_TYPE_KEY
```

### Mapplet Usage in Mapping

```
Mapping: m_LOAD_CUSTOMERS
├── SOURCE: RAW_CUSTOMERS
├── INSTANCE: mplt_STANDARDIZE_CUSTOMER (Mapplet)
│   ├── IN_CUST_DATA.CUST_NAME ← RAW_CUSTOMERS.CUST_NAME
│   └── OUT_STD_CUST.STD_NAME → TGT_CUSTOMERS.CUST_NAME
└── TARGET: TGT_CUSTOMERS
```

### Mapplet Rules

Always encapsulate complex, repeated logic in mapplets:
- Data standardization (name, address, email formatting)
- Surrogate key assignment via lookup
- Common validation patterns (null checks, range validation)
- Slowly Changing Dimension Type 2 logic

Mapplet constraints (PowerCenter enforces these):
- Cannot contain Target definitions
- Cannot contain Source Qualifier transformations (use Input transformation instead)
- Cannot contain certain transformation types (Joiner with multiple pipelines)
- Must have exactly one Input and one Output transformation (or passive mapplet with only Output)
- Active mapplets (with aggregators, filters, etc.) behave as a single active transformation in the parent mapping

## Shortcuts

Shortcuts are references to objects in shared folders. They enable single-source-of-truth.

```xml
<!-- In FN_SALES folder -->
<SHORTCUT NAME="DIM_DATE" SHORTCUTTYPE="Source" SHORTCUTFOLDER="FN_SHARED"
          SHORTCUTREPOSITORY="REPO_PROD" SHORTCUTTO="DIM_DATE"
          REFERENCETYPE="Local" OBJECTSUBTYPE="Source Definition"/>

<!-- In FN_FINANCE folder (same shared source) -->
<SHORTCUT NAME="DIM_DATE" SHORTCUTTYPE="Source" SHORTCUTFOLDER="FN_SHARED"
          SHORTCUTTO="DIM_DATE" REFERENCETYPE="Local"/>
```

### Shortcut Types

| `REFERENCETYPE` | Scope | Use Case |
|---------------|-------|----------|
| `Local` | Same repository | Standard pattern for shared objects |
| `Global` | Cross-repository | Multi-repository federated architecture |

### Shortcut Rules

Always use shortcuts for:
- Shared dimension tables (date, customer, product)
- Common lookup transformations
- Reusable standardization expressions
- Source definitions referenced by multiple teams

Never use shortcuts for:
- Object-specific or one-off transformations
- Temporary or experimental mappings
- Objects that require local customization (use a copy instead)

Shortcut resolution: when the repository loads, shortcuts are resolved to their target objects. If the target changes, all shortcuts reflect the change immediately.

## Reusable Transformations

Reusable transformations are defined once in a folder and instanced in multiple mappings.

### Transformations That Can Be Reusable

| Type | Reusable | Typical Use |
|------|----------|-------------|
| Expression | Yes | Standardization, calculation logic |
| Lookup | Yes | Surrogate key resolution |
| Sequence Generator | Yes | Generate surrogate keys |
| Stored Procedure | Yes | Database operations |
| External Procedure | Yes | Custom C/C++ logic |
| Aggregator | No | Mapping-specific grouping |
| Filter | No | Mapping-specific filtering |
| Joiner | No | Mapping-specific joins |
| Router | No | Mapping-specific routing |
| Source Qualifier | No | Bound to specific source |
| Target | No | Bound to specific target |
| Rank | No | Mapping-specific ranking |
| Update Strategy | No | Mapping-specific CDC logic |

### Reusable Transformation Definition

```xml
<TRANSFORMATION NAME="LKP_CUST_DIM" TYPE="Lookup Procedure" REUSABLE="YES"
                DESCRIPTION="Lookup customer surrogate key by CUST_ID">
  <TRANSFORMFIELD NAME="CUST_ID" DATATYPE="number" PRECISION="10" PORTTYPE="INPUT"/>
  <TRANSFORMFIELD NAME="CUST_KEY" DATATYPE="number" PRECISION="10" PORTTYPE="OUTPUT"/>
  <TRANSFORMFIELD NAME="CUST_NAME" DATATYPE="string" PRECISION="100" PORTTYPE="OUTPUT"/>
  <TRANSFORMFIELD NAME="CUST_SEGMENT" DATATYPE="string" PRECISION="20" PORTTYPE="OUTPUT"/>
  <TABLEATTRIBUTE NAME="Lookup Table Name" VALUE="DWH.DIM_CUSTOMER"/>
  <TABLEATTRIBUTE NAME="Lookup Condition" VALUE="CUST_ID = CUST_ID"/>
  <TABLEATTRIBUTE NAME="Lookup cache enabled" VALUE="YES"/>
  <TABLEATTRIBUTE NAME="Lookup cache persistent" VALUE="NO"/>
  <TABLEATTRIBUTE NAME="Dynamic Lookup Cache" VALUE="NO"/>
  <TABLEATTRIBUTE NAME="Output Old Value On Update" VALUE="NO"/>
</TRANSFORMATION>
```

### Instance with Local Overrides

```xml
<!-- In mapping m_SALES_FACT_LOAD -->
<INSTANCE NAME="LKP_CUST_DIM" TYPE="Lookup Procedure"
          TRANSFORMATIONNAME="LKP_CUST_DIM" TRANSFORMATIONTYPE="Lookup Procedure"/>

<!-- No local overrides = use reusable definition as-is -->

<!-- In mapping m_CUSTOMER_360 (with override) -->
<INSTANCE NAME="LKP_CUST_DIM_360" TYPE="Lookup Procedure"
          TRANSFORMATIONNAME="LKP_CUST_DIM" TRANSFORMATIONTYPE="Lookup Procedure"/>
```

Instance overrides are limited in PowerCenter. Most properties come from the reusable definition. This is a design constraint -- plan reusable transformations for the common case, not the edge case.

### Reusable Transformation Caution

Changes to a reusable transformation propagate to ALL instances immediately (with save confirmation). Before modifying:

1. Export the current definition as backup
2. Run impact analysis to list all affected mappings
3. Validate all affected mappings after the change
4. Test at least one session per affected mapping in non-production

## Instances vs Definitions

| Aspect | Definition | Instance |
|--------|-----------|----------|
| Location | Folder level (reusable) or inside MAPPING (non-reusable) | Inside MAPPING as INSTANCE element |
| Reusability | Single definition, referenced many times | One per mapping usage |
| Overrides | Not applicable | Limited local property overrides |
| XML element | `TRANSFORMATION` | `INSTANCE` |
| Referencing | Reusable: `TRANSFORMATIONNAME` points to folder-level definition | `INSTANCE` + embedded `TRANSFORMATION` for non-reusable |

### Reading the Pattern

```python
def resolve_instance(mapping, instance_name, folder):
    """Resolve an INSTANCE to its full TRANSFORMATION definition."""
    instance = mapping.find(f'.//INSTANCE[@NAME="{instance_name}"]')
    tx_name = instance.get('TRANSFORMATIONNAME')
    tx_type = instance.get('TRANSFORMATIONTYPE')

    # Check for embedded (non-reusable) transformation in mapping
    embedded = mapping.find(f'.//TRANSFORMATION[@NAME="{tx_name}"]')
    if embedded is not None:
        return {'type': 'non-reusable', 'definition': embedded}

    # Check for reusable transformation at folder level
    reusable = folder.find(f'.//TRANSFORMATION[@NAME="{tx_name}"][@REUSABLE="YES"]')
    if reusable is not None:
        return {'type': 'reusable', 'definition': reusable}

    # Check shortcut to shared folder
    shortcut = folder.find(f'.//SHORTCUT[@NAME="{tx_name}"]')
    if shortcut is not None:
        return {'type': 'shortcut', 'shortcut': shortcut}

    return {'type': 'unresolved'}
```

## Impact Analysis

Impact analysis traces dependencies across the repository object graph.

### Common Queries

**Which mappings use a given source?**

```python
def mappings_using_source(folder, source_name):
    return [
        m.get('NAME') for m in folder.findall('MAPPING')
        if m.find(f'.//INSTANCE[@TRANSFORMATIONNAME="{source_name}"][@TYPE="Source"]')
           is not None
    ]
```

**Which sessions reference a given mapping?**

```python
def sessions_for_mapping(folder, mapping_name):
    return [
        {
            'session_name': s.get('NAME'),
            'workflow_name': s.getparent().get('NAME')
            if s.getparent().tag == 'WORKFLOW' else 'standalone'
        }
        for s in folder.findall(f'.//SESSION[@MAPPINGNAME="{mapping_name}"]')
    ]
```

**Which workflows contain a given session?**

```python
def workflows_containing_session(folder, session_name):
    return [
        {
            'workflow_name': wf.get('NAME'),
            'is_enabled': wf.get('ISENABLED'),
            'scheduler': sched.get('NAME') if (sched := wf.find('SCHEDULER')) else None
        }
        for wf in folder.findall('WORKFLOW')
        if wf.find(f'.//SESSION[@NAME="{session_name}"]')
           or wf.find(f'.//TASKINSTANCE[@TASKNAME="{session_name}"]')
    ]
```

**Full dependency chain: Source → Mappings → Sessions → Workflows**

```python
def full_impact_chain(folder, source_name):
    chain = {'source': source_name, 'mappings': [], 'sessions': [], 'workflows': []}

    for mapping in folder.findall('MAPPING'):
        if mapping.find(f'.//INSTANCE[@TRANSFORMATIONNAME="{source_name}"]') is not None:
            m_name = mapping.get('NAME')
            chain['mappings'].append(m_name)

            for session in folder.findall(f'.//SESSION[@MAPPINGNAME="{m_name}"]'):
                s_name = session.get('NAME')
                chain['sessions'].append(s_name)

                for workflow in folder.findall('WORKFLOW'):
                    if workflow.find(f'.//TASKINSTANCE[@TASKNAME="{s_name}"]') is not None:
                        chain['workflows'].append(workflow.get('NAME'))

    # Deduplicate
    chain['workflows'] = list(set(chain['workflows']))
    return chain
```

## Repository Object Lifecycle

```
Development workflow:
1. Create/modify objects in shared or private folder
2. Validate mapping (checks connectivity, expression syntax)
3. Create session (binds mapping to runtime connections)
4. Add session to workflow
5. Test in non-production repository
6. Export as XML, migrate to production via pmrep or XML import
```

Always version-control repository exports. The XML export is the authoritative representation of repository state at a point in time.

## Databricks/Spark Metadata Equivalents

| Informatica Concept | Databricks Equivalent |
|---|---|
| Repository Folder | Unity Catalog Schema or Database |
| Mapping | Delta Live Tables Pipeline or Notebook |
| Mapplet | Python function / Notebook include |
| Reusable Transformation | Python function / Scala UDF |
| Shortcut | Unity Catalog VIEW |
| Session | Job Task (Notebook/JAR/Python) |
| Workflow | Databricks Workflow (multi-task job) |
| Source Definition | External Location + Schema |
| Target Definition | Managed or External Delta Table |

```python
# List all tables in a schema (equivalent to folder browse)
spark.catalog.listTables("my_schema")

# Reusable transformation as Python function
def standardize_dates(df, date_col):
    """Equivalent to a reusable Expression mapplet."""
    return df.withColumn(f"{date_col}_clean",
        to_date(col(date_col), "yyyy-MM-dd"))
```
