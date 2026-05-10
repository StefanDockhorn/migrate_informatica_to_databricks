---
name: informatica-repository-xml-schema
description: "Use when analyzing or parsing Informatica PowerCenter repository XML export files. Covers MAPPING, WORKFLOW, SESSION, TASK, CONNECTION, TRANSFORMATION node structures and attributes. Includes PySpark XML parsing and schema inference patterns. Do NOT use for general XML parsing tasks."
---

# Informatica PowerCenter Repository XML Schema

## When to Use

- Parsing `.xml` repository exports from PowerCenter Repository Manager
- Extracting metadata from MAPPING, WORKFLOW, SESSION, or CONNECTION nodes
- Building automated repository analysis tools
- Migrating or comparing repository objects across environments

## When NOT to Use

- General XML parsing tasks (use standard XML libraries instead)
- Real-time session log analysis (use session log parsing skills instead)
- Parameter file management (use parameter-management skills instead)

## Root Element: POWERMART

Every repository XML export begins with the `POWERMART` root element:

```xml
<POWERMART CREATION_DATE="01/15/2025 08:30:00" REPOSITORY_VERSION="187.0" CODEPAGE="UTF-8">
  <REPOSITORY NAME="REPO_PROD" VERSION="187" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
    <!-- FOLDER elements here -->
  </REPOSITORY>
</POWERMART>
```

Key attributes on `POWERMART`:
- `CREATION_DATE`: Export timestamp (format varies by locale)
- `REPOSITORY_VERSION`: Repository schema version
- `CODEPAGE`: Character encoding of the export

## Folder Structure

The `REPOSITORY` contains one or more `FOLDER` elements. All objects live inside a folder.

```xml
<FOLDER NAME="FN_SALES" DESCRIPTION="Sales ETL folder" PERMISSIONS="rwx---r--"
        UUID="abc-123" SHARED="NO" OWNER="etl_admin" GROUP="etl_devs">
  <SOURCE>...</SOURCE>
  <TARGET>...</TARGET>
  <MAPPING>...</MAPPING>
  <TRANSFORMATION>...</TRANSFORMATION>
  <SESSION>...</SESSION>
  <WORKFLOW>...</WORKFLOW>
  <CONFIG>...</CONFIG>
  <SHORTCUT>...</SHORTCUT>
  <TASK>...</TASK>
  <SESSIONCONFIG>...</SESSIONCONFIG>
</FOLDER>
```

| Attribute | Meaning |
|-----------|---------|
| `NAME` | Unique folder name within repository |
| `DESCRIPTION` | Human-readable folder purpose |
| `PERMISSIONS` | Unix-style permission string |
| `SHARED` | `YES` if shared across repositories |
| `OWNER` / `GROUP` | Security domain assignment |

Always scope queries to a single `FOLDER` first. Cross-folder references use `SHORTCUT` elements.

## Source Definitions

```xml
<SOURCE NAME="SALES_TRANSACTIONS" DESCRIPTION="Daily sales data"
        DATABASETYPE="Oracle" DBDNAME="ORCL_SALES" OWNERNAME="SALES_STG">
  <SOURCEFIELD NAME="TXN_ID" DATATYPE="number" PRECISION="15" SCALE="0"
               KEYTYPE="PRIMARY KEY" NULLABLE="NOTNULL"/>
  <SOURCEFIELD NAME="TXN_DATE" DATATYPE="date" PRECISION="19" SCALE="0"/>
  <SOURCEFIELD NAME="AMOUNT" DATATYPE="number" PRECISION="18" SCALE="2"/>
  <SOURCEFIELD NAME="CUST_ID" DATATYPE="number" PRECISION="10" SCALE="0"
               KEYTYPE="FOREIGN KEY" NULLABLE="NOTNULL"/>
  <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM SALES_STG.TRANSACTIONS"/>
  <TABLEATTRIBUTE NAME="Pre SQL" VALUE=""/>
  <METADATAEXTENSION NAME="BusinessDomain" VALUE="Sales"/>
</SOURCE>
```

Key children:
- `SOURCEFIELD`: Column definition with `NAME`, `DATATYPE`, `PRECISION`, `SCALE`, `KEYTYPE`, `NULLABLE`
- `TABLEATTRIBUTE`: Named properties including SQL overrides
- `CONNECTOR` (within MAPPING context): Links source field to transformation input
- `METADATAEXTENSION`: Custom metadata tags

## Target Definitions

```xml
<TARGET NAME="DWH_SALES_FACT" DESCRIPTION="Sales fact table"
        DATABASETYPE="Oracle" DBDNAME="ORCL_DWH" OWNERNAME="DWH_ODS">
  <TARGETFIELD NAME="SALES_KEY" DATATYPE="number" PRECISION="15" SCALE="0"
               KEYTYPE="PRIMARY KEY" NULLABLE="NOTNULL"/>
  <TARGETFIELD NAME="TXN_ID" DATATYPE="number" PRECISION="15" SCALE="0"/>
  <TARGETFIELD NAME="TXN_DATE_KEY" DATATYPE="number" PRECISION="10" SCALE="0"/>
  <TARGETFIELD NAME="AMOUNT" DATATYPE="number" PRECISION="18" SCALE="2"/>
  <TARGETFIELD NAME="CUST_KEY" DATATYPE="number" PRECISION="10" SCALE="0"/>
  <TABLEATTRIBUTE NAME="Pre SQL" VALUE="TRUNCATE TABLE DWH_ODS.SALES_FACT"/>
  <TABLEATTRIBUTE NAME="Post SQL" VALUE="ANALYZE TABLE DWH_ODS.SALES_FACT COMPUTE STATISTICS"/>
</TARGET>
```

Target `TABLEATTRIBUTE` commonly holds `Pre SQL`, `Post SQL`, `Update Override`, and `Insert`/`Update`/`Delete` strategy flags.

## Mappings

The `MAPPING` element is the core ETL definition. It contains transformations, instances, and connectors that define data flow.

```xml
<MAPPING NAME="m_SALES_FACT_LOAD" DESCRIPTION="Load sales fact from staging"
         ISVALID="YES" OBJECTVERSION="1" VERSIONNUMBER="1">
  <INSTANCE NAME="SALES_TRANSACTIONS" TYPE="Source"
            TRANSFORMATIONNAME="SALES_TRANSACTIONS" TRANSFORMATIONTYPE="Source Definition"/>
  <INSTANCE NAME="SQ_SALES_TRANSACTIONS" TYPE="Source Qualifier"
            TRANSFORMATIONNAME="SQ_SALES_TRANSACTIONS" TRANSFORMATIONTYPE="Source Qualifier"/>
  <INSTANCE NAME="EXP_CALCULATE_KEYS" TYPE="Expression"
            TRANSFORMATIONNAME="EXP_CALCULATE_KEYS" TRANSFORMATIONTYPE="Expression"/>
  <INSTANCE NAME="DWH_SALES_FACT" TYPE="Target"
            TRANSFORMATIONNAME="DWH_SALES_FACT" TRANSFORMATIONTYPE="Target Definition"/>

  <TRANSFORMATION NAME="SQ_SALES_TRANSACTIONS" TYPE="Source Qualifier">
    <TRANSFORMFIELD NAME="TXN_ID" DATATYPE="number" PRECISION="15" SCALE="0" PORTTYPE="INPUT/OUTPUT"/>
    <TRANSFORMFIELD NAME="TXN_DATE" DATATYPE="date" PRECISION="19" SCALE="0" PORTTYPE="INPUT/OUTPUT"/>
    <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="number" PRECISION="18" SCALE="2" PORTTYPE="INPUT/OUTPUT"/>
    <TRANSFORMFIELD NAME="CUST_ID" DATATYPE="number" PRECISION="10" SCALE="0" PORTTYPE="INPUT/OUTPUT"/>
    <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT TXN_ID, TXN_DATE, AMOUNT, CUST_ID FROM SALES_STG.TRANSACTIONS"/>
  </TRANSFORMATION>

  <TRANSFORMATION NAME="EXP_CALCULATE_KEYS" TYPE="Expression">
    <TRANSFORMFIELD NAME="TXN_ID_IN" DATATYPE="number" PRECISION="15" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="TXN_DATE_IN" DATATYPE="date" PRECISION="19" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="AMOUNT_IN" DATATYPE="number" PRECISION="18" SCALE="2" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="CUST_ID_IN" DATATYPE="number" PRECISION="10" PORTTYPE="INPUT"/>
    <TRANSFORMFIELD NAME="TXN_DATE_KEY" DATATYPE="number" PRECISION="10" PORTTYPE="OUTPUT"
                    EXPRESSION="TO_CHAR(TXN_DATE_IN,'YYYYMMDD')"/>
    <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="number" PRECISION="18" SCALE="2" PORTTYPE="OUTPUT"
                    EXPRESSION="AMOUNT_IN"/>
    <TABLEATTRIBUTE NAME="Tracing Level" VALUE="Normal"/>
  </TRANSFORMATION>

  <CONNECTOR FROMINSTANCE="SALES_TRANSACTIONS" FROMFIELD="TXN_ID"
             TOINSTANCE="SQ_SALES_TRANSACTIONS" TOFIELD="TXN_ID"/>
  <CONNECTOR FROMINSTANCE="SQ_SALES_TRANSACTIONS" FROMFIELD="TXN_ID"
             TOINSTANCE="EXP_CALCULATE_KEYS" TOFIELD="TXN_ID_IN"/>
  <CONNECTOR FROMINSTANCE="EXP_CALCULATE_KEYS" FROMFIELD="TXN_DATE_KEY"
             TOINSTANCE="DWH_SALES_FACT" TOFIELD="TXN_DATE_KEY"/>
</MAPPING>
```

### Key Structural Rules

- `INSTANCE` references a `TRANSFORMATION` by `TRANSFORMATIONNAME` for reusable transformations
- `TRANSFORMATION` embedded directly in the mapping is non-reusable (local to this mapping)
- `CONNECTOR` elements form a directed acyclic graph of data flow
- Always parse `CONNECTOR` elements first to understand data lineage
- `TRANSFORMATION.TYPE` determines behavior: Source Qualifier, Expression, Lookup, Aggregator, Joiner, Router, Filter, Sorter, Sequence Generator, Normalizer, Union, Rank, Update Strategy, Stored Procedure

## Workflows

```xml
<WORKFLOW NAME="wf_SALES_DAILY_LOAD" DESCRIPTION="Daily sales ETL workflow"
          ISVALID="YES" ISENABLED="YES" ISRUNNABLESERVICE="YES"
          ISARCHIVED="NO" SERVERNAME="INT_SVC" VERSIONNUMBER="3">
  <SCHEDULER NAME="SCHED_DAILY_6AM" DESCRIPTION="" RECOVERYTYPE="Fail task and continue"
             STARTOPTIONS="Start Date" STARTDATE="1/1/2025" STARTTIME="06:00:00"
             ENDOPTIONS="End Date" ENDDATE="12/31/2025" ENDTIME="06:00:00"
             RUNOPTIONS="Run continuously" SCHEDULEOPTIONS="Daily every 1 days"/>

  <SESSION NAME="s_SALES_FACT_LOAD" MAPPINGNAME="m_SALES_FACT_LOAD"
           ISVALID="YES">
    <SESSTRANSFORMATIONINFO TRANSFORMATIONNAME="DWH_SALES_FACT" TRANSFORMATIONTYPE="Target Definition">
      <SESSIONEXTENSION TYPE="Relational Writer" SUBTYPE="Oracle" CONNECTIONREFERENCE="CONN_DWH"/>
    </SESSTRANSFORMATIONINFO>
    <CONFIGREFERENCE CONFIGNAME="default_session_config" TYPE="Session config"/>
    <EXTENSION NAME="General" SUBNAME="Properties" TRANSFORMATIONNAME="s_SALES_FACT_LOAD"
               TYPE="Session" UUID="...">
      <SESSIONATTRIBUTE NAME="Save session log by" VALUE="Session runs"/>
      <SESSIONATTRIBUTE NAME="Stop on errors" VALUE="0"/>
    </EXTENSION>
  </SESSION>

  <TASK NAME="START" TYPE="Start" VERSIONNUMBER="1"/>
  <TASK NAME="DEC_CheckFileArrival" TYPE="Decision" VERSIONNUMBER="1">
    <DECISIONCONDITION NAME="$CheckFileArrival.Status" TYPE="MappingParameter"
                       VALUE="=SUCCEEDED"/>
  </TASK>

  <TASKINSTANCE NAME="START" TYPE="Start" TASKNAME="START" TASKTYPE="Start"/>
  <TASKINSTANCE NAME="s_SALES_FACT_LOAD" TYPE="Session" TASKNAME="s_SALES_FACT_LOAD"
                TASKTYPE="Session"/>
  <TASKINSTANCE NAME="cmd_SendSuccessEmail" TYPE="Command"
                TASKNAME="cmd_SendSuccessEmail" TASKTYPE="Command"/>

  <WORKFLOWLINK FROMTASK="START" TOTASK="s_SALES_FACT_LOAD" CONDITION=""/>
  <WORKFLOWLINK FROMTASK="s_SALES_FACT_LOAD" TOTASK="DEC_CheckFileArrival"
                CONDITION="$s_SALES_FACT_LOAD.Status=SUCCEEDED"/>
  <WORKFLOWLINK FROMTASK="DEC_CheckFileArrival" TOTASK="cmd_SendSuccessEmail"
                CONDITION="$DEC_CheckFileArrival.Status=SUCCEEDED"/>
</WORKFLOW>
```

### Workflow Task Types

| Task Type | Purpose | Key Children |
|-----------|---------|-------------|
| `Session` | Execute a mapping | `SESSTRANSFORMATIONINFO`, `EXTENSION` |
| `Command` | Run OS/shell command | `COMMAND` element with `COMMAND` attribute |
| `Decision` | Conditional branching | `DECISIONCONDITION` |
| `Email` | Send email notification | `EMAIL` element |
| `EventWait` | Wait for event | `EVENT` element with `EVENTNAME` |
| `EventRaise` | Raise event for downstream | `EVENT` element |
| `Timer` | Delay/wait for duration | `TIMER` element |
| `Assignment` | Set variable values | `ASSIGNMENT` element |
| `Worklet` | Nested workflow container | Contains its own tasks and links |
| `Start` | Workflow entry point | No children |

### Workflow Structure Rules

- `TASKINSTANCE` references a `TASK` by `TASKNAME`; reusable tasks are defined once and instanced multiple times
- `WORKFLOWLINK` defines execution order; `CONDITION` attribute controls conditional routing
- `SESSION` elements inside workflows are always non-reusable (bound to one workflow)
- `WORKLET` elements contain nested `TASK`, `TASKINSTANCE`, and `WORKFLOWLINK` structures

## Sessions

```xml
<SESSION NAME="s_SALES_FACT_LOAD" MAPPINGNAME="m_SALES_FACT_LOAD"
         ISVALID="YES" ISDBDIVISIBLE="YES">
  <SESSTRANSFORMATIONINFO TRANSFORMATIONNAME="DWH_SALES_FACT"
                          TRANSFORMATIONTYPE="Target Definition">
    <SESSIONEXTENSION TYPE="Relational Writer" SUBTYPE="Oracle"
                      CONNECTIONREFERENCE="CONN_DWH" DNAME="DWH_SALES_FACT"
                      INSTANCE="DWH_SALES_FACT">
      <CONNECTIONREFERENCE CNXREFNAME="DB Connection" CONNECTIONNAME="ORA_DWH_PROD"
                           CONNECTIONTYPE="Relational" CONNECTIONNUMBER="1"
                           VARIABLE=""/>
      <ATTRIBUTE NAME="Target Table Name" VALUE="DWH_ODS.SALES_FACT"/>
      <ATTRIBUTE NAME="Truncate Target Table" VALUE="YES"/>
      <ATTRIBUTE NAME="Reject File" VALUE="/infa/shared/reject/sales_fact.bad"/>
    </SESSIONEXTENSION>
  </SESSTRANSFORMATIONINFO>

  <CONFIGREFERENCE CONFIGNAME="default_session_config" TYPE="Session config"/>

  <EXTENSION NAME="General" SUBNAME="Properties" TYPE="Session">
    <SESSIONATTRIBUTE NAME="Save session log by" VALUE="Session runs"/>
    <SESSIONATTRIBUTE NAME="Stop on errors" VALUE="0"/>
    <SESSIONATTRIBUTE NAME="DTM buffer size" VALUE="Auto"/>
    <SESSIONATTRIBUTE NAME="Collect performance data" VALUE="Yes"/>
  </EXTENSION>

  <EXTENSION NAME="Partitioning" SUBNAME="" TYPE="Session"
             TRANSFORMATIONNAME="SQ_SALES_TRANSACTIONS">
    <SESSIONEXTENSION TYPE="Partitioning" NAME="Partition Point">
      <ATTRIBUTE NAME="partition type" VALUE="Pass-through"/>
      <ATTRIBUTE NAME="number of partitions" VALUE="2"/>
    </SESSIONEXTENSION>
  </EXTENSION>
</SESSION>
```

Key session structures:
- `SESSTRANSFORMATIONINFO`: One per transformation in the mapping; holds connection and override details
- `SESSIONEXTENSION`: Connection type (`Relational Reader`/`Writer`, `File Reader`/`Writer`, `FTP Writer`)
- `CONFIGREFERENCE`: References a `SESSIONCONFIG` object for default session properties
- `EXTENSION` + `SESSIONATTRIBUTE`: General properties like DTM buffer size, tracing level, error handling
- `CONNECTIONREFERENCE`: Named connection object used for this session
- `EXTENSION` + `Partitioning`: Partition point configuration for parallel execution

Always check `Sql Override` attributes in Source Qualifier sessions for custom SQL that changes the mapping's source data.

## Connections

```xml
<CONNECTION NAME="ORA_SRC_PROD" TYPE="Relational" SUBTYPE="Oracle"
            CONNECTIONSTRING="//dbhost:1521/ORCL" CODEPAGE="UTF-8"
            USERNAME="SALES_STG" PASSWORD="..." CONNECTIONATTRIBUTES="">
  <ATTRIBUTE NAME="Connection Retry Period" VALUE="60"/>
  <ATTRIBUTE NAME="Connection Retry Attempt" VALUE="5"/>
</CONNECTION>

<CONNECTION NAME="FTP_LANDING_ZONE" TYPE="FTP" SUBTYPE="FTP"
            CONNECTIONSTRING="ftp.company.com:21" CODEPAGE="UTF-8"
            USERNAME="infa_ftp" PASSWORD="...">
  <ATTRIBUTE NAME="Remote Filename" VALUE="/data/inbound/sales_*.csv"/>
  <ATTRIBUTE NAME="Is Staged" VALUE="YES"/>
</CONNECTION>
```

Connection types:
- `Relational`: Oracle, SQL Server, DB2, Teradata, Netezza, ODBC
- `FTP`: FTP, SFTP, FTPS
- `Queue`: MQ Series, JMS
- `Application`: SAP, Salesforce, web services
- `Loader`: Teradata, Oracle external table loaders

## Transformation Definitions

Reusable transformations are defined at folder level and referenced by `INSTANCE` elements.

```xml
<!-- Reusable Expression transformation -->
<TRANSFORMATION NAME="EXP_STANDARDIZE_DATES" TYPE="Expression" REUSABLE="YES"
                DESCRIPTION="Standardize date formats across sources">
  <TRANSFORMFIELD NAME="DATE_IN" DATATYPE="string" PRECISION="30" SCALE="0"
                  PORTTYPE="INPUT"/>
  <TRANSFORMFIELD NAME="STD_DATE" DATATYPE="date" PRECISION="19" SCALE="0"
                  PORTTYPE="OUTPUT"
                  EXPRESSION="IIF(ISNULL(DATE_IN) OR LENGTH(DATE_IN)=0,
                            TO_DATE('9999-12-31','YYYY-MM-DD'),
                            IIF(IS_DATE(DATE_IN,'MM/DD/YYYY'),
                                TO_DATE(DATE_IN,'MM/DD/YYYY'),
                                TO_DATE(DATE_IN,'YYYY-MM-DD')))"/>
  <TRANSFORMFIELD NAME="STD_DATE_STR" DATATYPE="string" PRECISION="10" SCALE="0"
                  PORTTYPE="OUTPUT" EXPRESSION="TO_CHAR(STD_DATE,'YYYY-MM-DD')"/>
  <TABLEATTRIBUTE NAME="Tracing Level" VALUE="Normal"/>
</TRANSFORMATION>

<!-- Reusable Lookup transformation -->
<TRANSFORMATION NAME="LKP_CUST_DIM" TYPE="Lookup Procedure" REUSABLE="YES">
  <TRANSFORMFIELD NAME="CUST_ID" DATATYPE="number" PRECISION="10" PORTTYPE="INPUT"/>
  <TRANSFORMFIELD NAME="CUST_KEY" DATATYPE="number" PRECISION="10" PORTTYPE="OUTPUT"/>
  <TRANSFORMFIELD NAME="CUST_NAME" DATATYPE="string" PRECISION="100" PORTTYPE="OUTPUT"/>
  <TABLEATTRIBUTE NAME="Lookup Table Name" VALUE="DWH.DIM_CUSTOMER"/>
  <TABLEATTRIBUTE NAME="Lookup Source Filter" VALUE="IS_CURRENT='Y'"/>
  <TABLEATTRIBUTE NAME="Lookup Policy On Multiple Match" VALUE="Use First Value"/>
  <TABLEATTRIBUTE NAME="Connection Information" VALUE="ORA_DWH_PROD"/>
  <TABLEATTRIBUTE NAME="Lookup cache enabled" VALUE="YES"/>
  <TABLEATTRIBUTE NAME="Lookup cache persistent" VALUE="NO"/>
</TRANSFORMATION>
```

### TRANSFORMFIELD Port Types

| `PORTTYPE` | Meaning |
|-----------|---------|
| `INPUT` | Receives data from upstream |
| `OUTPUT` | Sends data downstream |
| `INPUT/OUTPUT` | Both (pass-through or modified) |
| `VARIABLE` | Internal Expression variable |
| `LOOKUP` | Lookup output port |
| `MASTER` | Joiner master input |
| `DETAIL` | Joiner detail input |

### FIELDATTRIBUTE for Business Metadata

```xml
<TRANSFORMFIELD NAME="SALES_AMOUNT" DATATYPE="number" PRECISION="18" SCALE="2" PORTTYPE="OUTPUT">
  <FIELDATTRIBUTE NAME="BUSINESSNAME" VALUE="Total Sales Amount USD"/>
  <FIELDATTRIBUTE NAME="DESCRIPTION" VALUE="Sum of line item amounts in US Dollars"/>
</TRANSFORMFIELD>
```

Always check `FIELDATTRIBUTE BUSINESSNAME` for business-friendly field descriptions before exposing metadata to non-technical users.

## Parsing Patterns

### Python: ElementTree

```python
import xml.etree.ElementTree as ET
from pathlib import Path

def parse_repository_xml(xml_path: str) -> dict:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    result = {
        'creation_date': root.get('CREATION_DATE'),
        'repository_version': root.get('REPOSITORY_VERSION'),
        'codepage': root.get('CODEPAGE'),
        'folders': []
    }

    for folder in root.findall('.//FOLDER'):
        folder_data = {
            'name': folder.get('NAME'),
            'description': folder.get('DESCRIPTION'),
            'shared': folder.get('SHARED', 'NO'),
            'sources': [s.get('NAME') for s in folder.findall('SOURCE')],
            'targets': [t.get('NAME') for t in folder.findall('TARGET')],
            'mappings': [],
            'workflows': [],
            'reusable_transformations': [
                rt.get('NAME') for rt in folder.findall(
                    "TRANSFORMATION[@REUSABLE='YES']")
            ]
        }

        for mapping in folder.findall('MAPPING'):
            mapping_data = {
                'name': mapping.get('NAME'),
                'description': mapping.get('DESCRIPTION'),
                'is_valid': mapping.get('ISVALID'),
                'connector_count': len(mapping.findall('CONNECTOR')),
                'transformation_count': len(mapping.findall('TRANSFORMATION')),
                'instance_count': len(mapping.findall('INSTANCE')),
                'connectors': [
                    {
                        'from_instance': c.get('FROMINSTANCE'),
                        'from_field': c.get('FROMFIELD'),
                        'to_instance': c.get('TOINSTANCE'),
                        'to_field': c.get('TOFIELD')
                    }
                    for c in mapping.findall('CONNECTOR')
                ]
            }
            folder_data['mappings'].append(mapping_data)

        for wf in folder.findall('WORKFLOW'):
            folder_data['workflows'].append({
                'name': wf.get('NAME'),
                'is_enabled': wf.get('ISENABLED'),
                'session_count': len(wf.findall('SESSION')),
                'task_count': len(wf.findall('TASK')),
                'links': [
                    {
                        'from': l.get('FROMTASK'),
                        'to': l.get('TOTASK'),
                        'condition': l.get('CONDITION', '')
                    }
                    for l in wf.findall('WORKFLOWLINK')
                ]
            })

        result['folders'].append(folder_data)

    return result


# Usage
repo = parse_repository_xml('/path/to/repo_export.xml')
print(f"Folders: {len(repo['folders'])}")
for f in repo['folders']:
    print(f"  {f['name']}: {len(f['mappings'])} mappings, "
          f"{len(f['workflows'])} workflows")
```

### Python: lxml (for large files)

```python
from lxml import etree

def iter_parse_mappings(xml_path: str):
    """Memory-efficient iteration over mappings using iterparse."""
    for event, elem in etree.iterparse(xml_path, events=('end',), tag='MAPPING'):
        yield {
            'name': elem.get('NAME'),
            'connectors': [
                {'from_instance': c.get('FROMINSTANCE'), 'from_field': c.get('FROMFIELD'),
                 'to_instance': c.get('TOINSTANCE'), 'to_field': c.get('TOFIELD')}
                for c in elem.findall('CONNECTOR')
            ]
        }
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
```

### PySpark: XML DataFrame

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RepoParser").getOrCreate()

# Read repository at FOLDER level
df = spark.read.format("xml") \
    .option("rowTag", "FOLDER") \
    .option("ignoreNamespace", "true") \
    .load("/path/to/repo_export.xml")

# Extract mapping metadata
df.selectExpr("explode(MAPPING) as m") \
  .selectExpr("m._NAME as name",
              "size(m.CONNECTOR) as connectors",
              "size(m.TRANSFORMATION) as txs") \
  .show(truncate=False)

# Read at POWERMART root for repository metadata
df_root = spark.read.format("xml") \
    .option("rowTag", "POWERMART") \
    .option("ignoreNamespace", "true") \
    .load("/path/to/repo_export.xml")

df_root.select("_CREATION_DATE", "_REPOSITORY_VERSION").show()
```

## Common Pitfalls

- `REPOSITORY_VERSION` may differ from `REPOSITORY.VERSION` -- use the `POWERMART` attribute for schema compatibility
- Non-reusable transformations lack the `REUSABLE` attribute (defaults to implicit `NO`)
- `INSTANCE.TYPE` and `TRANSFORMATION.TYPE` use different value sets (`Source` vs `Source Definition`)
- Passwords in `CONNECTION` elements may be encrypted -- never log them
- Exports >500MB require streaming parsers (`iterparse`); DOM parsers will OOM
- `CODEPAGE` mismatches cause encoding errors on non-ASCII characters in `DESCRIPTION` and `FIELDATTRIBUTE`