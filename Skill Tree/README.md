# Informatica PowerCenter Skills for Claude Code

A comprehensive structured knowledge base that converts Informatica PowerCenter 10.4.0 documentation into AI-ready skills compatible with the Databricks AI Dev Kit format and Phil Schmid's agent skills best practices.

## What These Skills Cover

These skills enable Claude Code to act as a **5-year PowerCenter veteran** when reading, analyzing, explaining, or migrating Informatica metadata and mappings. The knowledge base covers:

- **19 Transformation Types**: Source Qualifier, Expression, Lookup (connected/unconnected, static/dynamic cache), Joiner, Aggregator, Router, Filter, Sorter, Rank, Sequence Generator, Update Strategy, Transaction Control, Normalizer, Stored Procedure, Data Masking, Custom Transformation, External Procedure, Union, XML Source Qualifier
- **5 Workflow Categories**: Workflow design, session configuration, scheduling, parameter files, error handling
- **3 Repository Topics**: XML schema parsing, metadata objects, column-level lineage tracing
- **6 Function Categories**: Date, string, numeric, conversion, conditional, and aggregate functions
- **5 Databricks Mapping Guides**: Transformation mappings, function mappings, workflow orchestration, performance patterns, and anti-patterns

## Source Documentation

These skills were derived from the following Informatica PowerCenter 10.4.0 PDF documentation:

| Document | Content Extracted |
|----------|------------------|
| `PC_1040_TransformationGuide_en.pdf` | All 32 transformation chapters (Aggregator, Custom, Data Masking, Expression, External Procedure, Filter, HTTP, Java, Joiner, Lookup, Normalizer, Rank, Router, Sequence Generator, Sorter, Source Qualifier, SQL, Stored Procedure, Transaction Control, Union, Update Strategy, XML) |
| `PC_1040_(XML)Guide_en.pdf` | XML source/target handling, XML Source Qualifier, midstream XML Parser/Generator, XML datatypes, XPath query functions |

## Installation

These skills follow the Databricks AI Dev Kit skill system structure. To install:

### Option 1: Direct Copy to Claude Code Project

```bash
# Copy the entire informatica/ directory to your project's .claude/skills/ folder
cp -r informatica/ /path/to/your/project/.claude/skills/
```

### Option 2: Global Installation (Claude Code)

```bash
# Place in your home directory for global access
mkdir -p ~/.claude/skills/
cp -r informatica/ ~/.claude/skills/
```

### Option 3: Databricks AI Dev Kit Install

```bash
# If using the AI Dev Kit CLI
ai-dev-kit skills install --source informatica/ --target ~/.claude/skills/
```

## Directory Structure

```
informatica/
├── README.md                                    # This file
├── informatica-transformations/
│   ├── source-qualifier/SKILL.md                # SQL override, joins, filters, sorted ports
│   ├── expression/SKILL.md                      # Port expressions, variables, defaults
│   ├── lookup/SKILL.md                          # Connected/unconnected, static/dynamic cache
│   │   └── references/
│   │       ├── cache-mechanics.md               # Cache architecture details
│   │       ├── dynamic-lookup-examples.md       # Dynamic cache patterns
│   │       └── unconnected-lookup-patterns.md   # UDF-based lookup calls
│   ├── joiner/SKILL.md                          # Normal/outer joins, sorted input, blocking
│   ├── aggregator/SKILL.md                      # Aggregate functions, group by, sorted input
│   ├── router/SKILL.md                          # Output groups, filter conditions, default
│   ├── filter/SKILL.md                          # Filter conditions, null handling
│   ├── sorter/SKILL.md                          # Sort keys, distinct, case sensitivity
│   ├── rank/SKILL.md                            # Rank index, groups, top/bottom N
│   ├── sequence-generator/SKILL.md              # NEXTVAL, CURRVAL, cycle, cache
│   ├── update-strategy/SKILL.md                 # DD_INSERT/UPDATE/DELETE/REJECT
│   ├── transaction-control/SKILL.md             # Commit/rollback boundaries
│   ├── normalizer/SKILL.md                      # VSAM/pipeline normalizer, GK/GCID
│   ├── stored-procedure/SKILL.md                # Connected/unconnected, pre/post session
│   ├── data-masking/SKILL.md                    # Key, substitution, random, expression masking
│   ├── custom-transformation/SKILL.md           # C/C++ API, blocking, array-based mode
│   ├── external-procedure/SKILL.md              # COM/Informatica procedures
│   ├── union/SKILL.md                           # Input groups, port matching
│   └── xml-source-qualifier/SKILL.md            # XML parsing, schema, XPath
├── informatica-workflows/
│   ├── workflow-design/SKILL.md                 # Tasks, links, events, worklets, decisions
│   ├── session-configuration/SKILL.md           # Connections, partitions, pushdown
│   ├── scheduling/SKILL.md                      # Time-based, event-based, file-watch
│   ├── parameter-files/SKILL.md                 # $$params, $$vars, $PM variables
│   └── error-handling/SKILL.md                  # Fatal/non-fatal, recovery, bad files
├── informatica-repository/
│   ├── xml-schema/SKILL.md                      # Repository XML export format
│   ├── metadata-objects/SKILL.md                # Folders, mappings, mapplets, shortcuts
│   └── lineage-tracing/SKILL.md                 # CONNECTOR-based column lineage
├── informatica-functions/
│   ├── date-functions/SKILL.md                  # TO_DATE, TO_CHAR, ADD_TO_DATE, etc.
│   ├── string-functions/SKILL.md                # SUBSTR, INSTR, LPAD, RPAD, etc.
│   ├── numeric-functions/SKILL.md               # ABS, MOD, POWER, SQRT, etc.
│   ├── conversion-functions/SKILL.md            # TO_INTEGER, TO_DECIMAL, TO_FLOAT, etc.
│   ├── conditional-functions/SKILL.md           # IIF, DECODE, NVL, ISNULL, etc.
│   └── aggregate-functions/SKILL.md             # SUM, AVG, COUNT, MAX, MIN, etc.
└── informatica-to-databricks-mapping/
    ├── transformation-mapping/SKILL.md            # Informatica X -> Databricks Y table
    ├── function-mapping/SKILL.md                  # Function equivalence table
    ├── workflow-orchestration-mapping/SKILL.md    # Workflow -> Databricks Jobs
    ├── performance-patterns/SKILL.md              # Cache -> broadcast, partitions, etc.
    └── anti-patterns/SKILL.md                     # Common migration failures + fixes
```

## How to Use with Claude Code

### Triggering Skills

Claude Code automatically loads relevant skills based on your queries. Each skill has a descriptive trigger that helps Claude identify when to use it:

| Query Pattern | Skills Triggered |
|---------------|-----------------|
| "Explain this Lookup transformation" | `informatica-lookup` |
| "How do I migrate this Source Qualifier to Spark?" | `informatica-source-qualifier`, `transformation-mapping` |
| "What's the Spark equivalent of IIF?" | `informatica-conditional-functions`, `function-mapping` |
| "Parse this repository XML file" | `informatica-repository-xml-schema`, `informatica-repository-lineage` |
| "Why is this Joiner blocking?" | `informatica-joiner`, `anti-patterns` |
| "Convert TO_DATE to Spark SQL" | `informatica-date-functions` |

### Cross-Referenced Skill Usage

Skills are designed to work together. The mapping skills cross-reference transformation and function skills:

```
User: "Migrate this mapping with a Lookup, Joiner, and Aggregator"
Claude uses:
  1. informatica-lookup (cache behavior, connected vs unconnected)
  2. informatica-joiner (master/detail, sorted input)
  3. informatica-aggregator (group by, sorted input)
  4. transformation-mapping (Spark equivalents for all three)
  5. performance-patterns (broadcast hints, partition tuning)
```

## Skill Format

Each skill follows the Databricks AI Dev Kit format:

```yaml
---
name: informatica-skill-name
description: "Use when [specific trigger]. Covers [what]. Includes [Databricks equivalent]. Do NOT use for [negative case]."
---

## Purpose
Direct explanation of what the skill covers.

## Configuration
| Property | Description | Default |
|----------|-------------|---------|
| Property1 | What it does | DefaultValue |

## Behavior Rules
- Always use X when Y
- Never do Z because it causes W
- Explain WHY for important rules

## Spark/Databricks Equivalent
```python
# 5-line code snippet showing the Databricks equivalent
```

## Edge Cases
- Edge case 1 and how to handle it
- Edge case 2 and how to handle it

## Example
```
-- Informatica example
```
```python
# Databricks equivalent
```
```

## Quality Guarantees

- **Accuracy over completeness**: Ambiguous PDF sections are flagged rather than hallucinated
- **Exact terminology preserved**: "blocking transformation", "active vs passive", "connected vs unconnected"
- **Actual Spark APIs referenced**: Real PySpark/Spark SQL APIs, not generic concepts
- **500-line limit**: No SKILL.md exceeds 500 lines; overflow goes to `references/` files
- **Triggerable descriptions**: Every skill has a specific, actionable description with "what" and "when"
- **Negative cases included**: Each skill specifies when it should NOT fire
- **Directives, not essays**: "Always use X", not "X is recommended"
- **Examples lead**: 5-line code snippets over 5-paragraph explanations

## Contributing

To extend these skills with additional Informatica documentation (e.g., Workflow Guide, Designer Guide, Repository Guide):

1. Extract content from the PDF preserving structure
2. Follow the SKILL.md frontmatter template with `name` and `description`
3. Keep the main body under 500 lines
4. Add Databricks/Spark equivalents where applicable
5. Include before/after code examples
6. Place detailed reference material in `references/` subdirectories

## License

These skills are generated from Informatica's publicly available documentation. The skill structure and Databricks mappings are provided as-is for educational and migration purposes. Informatica, PowerCenter, and related marks are trademarks of Informatica LLC.
