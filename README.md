# Informatica PowerCenter → Databricks Migration

![License](https://img.shields.io/github/license/StefanDockhorn/migrate_informatica_to_databricks?style=flat)
![Informatica](https://img.shields.io/badge/Informatica-PowerCenter%2010.4.0-red?style=flat)
![Skills](https://img.shields.io/badge/skills-46-blue?style=flat)
![Databricks](https://img.shields.io/badge/Built%20for-Databricks-FF3621?logo=databricks&style=flat)
![GitHub last commit](https://img.shields.io/github/last-commit/StefanDockhorn/migrate_informatica_to_databricks?style=flat)

> **Hybrid, AI-augmented migration toolkit** combining bulk transpilation (Lakebridge), intelligent gap-filling (AI Coding Assistant + custom skills), and workspace-aware deployment (Databricks AI Dev Kit).

---

## What's in This Repo

| Artifact | Purpose |
|----------|---------|
| [`informatica-to-databricks-migration-plan.md`](informatica-to-databricks-migration-plan.md) | Complete phase-by-phase migration plan with tool stack, risk register, and success criteria |
| [`Skill Tree/`](Skill%20Tree/) | 46 AI-ready skills that teach an AI assistant to read Informatica XML and generate Databricks equivalents |
| `informatica-to-databricks-migration-plan.pdf` | PDF rendering of the migration plan for sharing |

**Stack:** Lakebridge · AI Coding Assistant · Databricks AI Dev Kit · Custom Informatica Skills

---

## Migration Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              INFORMATICA POWERCENTER ESTATE                 │
│  (Mappings, Workflows, Sessions, BDM, DEI, Parameters)     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 0: UNDERSTAND (AI Assistant + Informatica Skills)  │
│  - Read exported repository XML as plain text files         │
│  - Explain business logic per mapping/transformation        │
│  - Identify dependencies, edge cases, anti-patterns       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: CONVERT (Lakebridge + BladeBridge)                │
│  - Profile & assess estate complexity                       │
│  - Bulk transpile 70-80% of standard mappings               │
│  - Generate complexity report & reconciler baseline         │
└──────────────────────┬──────────────────────────────────────┘
                       │ (unconverted artifacts + error logs)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: FILL (AI Assistant + AI Dev Kit + Skills)       │
│  - Read unconverted mapping XML manually via file access    │
│  - Rewrite complex workflows as Databricks Workflows/Airflow│
│  - Convert BDM/DEI gaps to PySpark / Structured Streaming   │
│  - Translate proprietary functions to Spark SQL UDFs        │
│  - Optimize for Delta Lake (Z-ORDER, liquid clustering)     │
│  - Validate against live workspace via AI Dev Kit tools     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: HARDEN (AI Dev Kit + DABs)                      │
│  - Bundle as Databricks Asset Bundles (dev/prod)            │
│  - Enforce Unity Catalog governance                         │
│  - Deploy via CI/CD (GitHub Actions / Azure DevOps)       │
│  - Scaffold post-migration apps (Genie, AI/BI)            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│     DATABRICKS LAKEHOUSE + UNITY CATALOG                  │
│  (Delta Live Tables, Workflows, SQL Warehouses)             │
└─────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
.
├── README.md                                    # This file
├── informatica-to-databricks-migration-plan.md  # Full migration plan
├── informatica-to-databricks-migration-plan.pdf # PDF version
└── Skill Tree/
    ├── README.md                                # Skill tree documentation
    └── .claude/skills/informatica/              # 46 AI skills
        ├── informatica-transformations/         # 19 transformation types
        ├── informatica-workflows/               # 5 workflow categories
        ├── informatica-repository/              # XML schema, metadata, lineage
        ├── informatica-functions/               # 6 function categories
        └── informatica-to-databricks-mapping/   # Rosetta Stone mappings
```

### Skills Breakdown

| Category | Skills | What They Cover |
|----------|--------|-----------------|
| **Transformations** | 19 | Source Qualifier, Expression, Lookup (connected/unconnected, static/dynamic cache), Joiner, Aggregator, Router, Filter, Sorter, Rank, Sequence Generator, Update Strategy, Transaction Control, Normalizer, Stored Procedure, Data Masking, Custom Transformation, External Procedure, Union, XML Source Qualifier |
| **Workflows** | 5 | Workflow design, session configuration, scheduling, parameter files, error handling |
| **Repository** | 3 | XML schema parsing, metadata objects, column-level lineage tracing |
| **Functions** | 6 | Date, string, numeric, conversion, conditional, and aggregate functions |
| **Mapping Guides** | 5 | Transformation mappings, function mappings, workflow orchestration, performance patterns, anti-patterns |

---

## Quick Start

### 1. Read the Migration Plan

Start with [`informatica-to-databricks-migration-plan.md`](informatica-to-databricks-migration-plan.md) for the full strategy, tool installation steps, phase-by-phase execution, and risk register.

### 2. Install the Skills

These skills follow the Databricks AI Dev Kit format and work with Claude Code:

```bash
# Project-scoped (recommended)
cp -r "Skill Tree/.claude/skills/informatica" ./.claude/skills/

# Or global installation
mkdir -p ~/.claude/skills/
cp -r "Skill Tree/.claude/skills/informatica" ~/.claude/skills/
```

### 3. Use the Skills

Once installed, ask your AI assistant questions like:

- *"Explain how this unconnected lookup works in the attached Informatica XML"*
- *"Migrate this Source Qualifier SQL override to Spark SQL"*
- *"What's the Databricks equivalent of this IIF + ISNULL expression?"*
- *"Parse this repository XML and trace column lineage"*

The AI assistant will automatically load the relevant skills and cross-reference them.

---

## Key Migration Metrics

| Metric | Target |
|--------|--------|
| Mappings auto-converted by Lakebridge | ≥ 70% |
| Mappings requiring AI assistant intervention | ≤ 30% |
| Data reconciliation pass rate | 100% (row-level + aggregate) |
| Workflow orchestration migrated | 100% (redesigned, not 1:1) |
| Informatica decommissioned | Domain-by-domain, 100% final |
| Post-migration query performance | ≤ 120% of Informatica baseline |
| Unity Catalog governance coverage | 100% of migrated tables |

---

## Next Actions

1. [ ] Confirm BDM/DEI scope; decide if commercial accelerator needed
2. [ ] Export PowerCenter repository XML + download all PDF guides
3. [ ] Install Databricks AI Dev Kit in migration project directory
4. [ ] Install the Informatica skills (this repo)
5. [ ] Run Lakebridge assessment against exported repository
6. [ ] Establish complexity matrix and gap registry
7. [ ] Begin bulk transpilation (Phase 1)

See the full migration plan for detailed execution steps.

---

## Documentation Sources

Skills are derived from official Informatica PowerCenter 10.4.0 documentation:

- `PC_1040_TransformationGuide_en.pdf`
- `PC_1040_(XML)Guide_en.pdf`

---

## License

These skills are generated from Informatica's publicly available documentation. The skill structure and Databricks mappings are provided as-is for educational and migration purposes. Informatica, PowerCenter, and related marks are trademarks of Informatica LLC.
