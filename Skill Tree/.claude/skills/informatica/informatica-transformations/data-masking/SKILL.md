---
name: informatica-data-masking
description: "Use when analyzing, explaining, or migrating Informatica PowerCenter Data Masking transformations. Covers key masking, substitution, random masking, expression masking, blurring, and special formats (SSN, credit card, phone, email). Includes Spark SQL and Databricks data masking equivalents. Do NOT use for general data anonymization."
---

# Data Masking Transformation

## Purpose

Replaces sensitive data with realistic but fictional data for non-production environments.

## Masking Types

| Type | Deterministic | Description |
|------|--------------|-------------|
| Key Masking | Yes | Same input always produces same output; uses seed |
| Substitution | Yes/No | Replaces values from a dictionary file or table |
| Random Masking | No | Random replacement within constraints |
| Expression Masking | Configurable | User-defined expression for masking logic |
| Blurring | No | Original value +/- random offset within range |

## Key Masking

Same input always produces same masked output. Uses a seed value for determinism across runs. Required for:
- Referential integrity maintenance
- Repeatable test data generation
- Cross-table consistent masking

## Substitution

Replaces values from a dictionary file or relational table. Dictionary must have sufficient unique values.

## Random Masking

Random replacement within datatype constraints. Non-deterministic -- different output on each run.

## Expression Masking

User-defined expression for custom masking logic. Full flexibility using Informatica expression functions.

## Blurring

Adds or subtracts a random offset within a specified range.

```
-- Blur original salary by +/- 10%
-- Range: 0.9 * SAL to 1.1 * SAL
```

## Special Format Masking

| Format | Mask Pattern | Example |
|--------|-------------|---------|
| SSN | AAA-GG-SSSS | 123-45-6789 |
| Credit Card | Preserves last 4 digits | ****-****-****-1234 |
| Phone | (AAA) BBB-CCCC | (555) 123-4567 |
| Email | Preserves domain | xxx@company.com |
| IP Address | DDD.DDD.DDD.DDD | 192.168.1.1 |
| URL | Preserves structure | https://xxx.com |
| SIN | AAA-BBB-CCCC | 123-456-789 |

## Mask Format Characters

| Character | Matches |
|-----------|---------|
| A | Any alphabetic character |
| D | Any digit |
| U | Uppercase alphabetic |
| L | Lowercase alphabetic |
| N | Alphanumeric |

## Behavior Rules

- Key masking requires unique values as input; duplicates produce same masked output
- Substitution dictionaries must have sufficient unique values for all distinct inputs
- Credit card masking preserves last 4 digits and validates Luhn check
- Email masking preserves domain structure (local-part is masked)
- SSN masking generates valid area/group/serial combinations
- Key masking with same seed produces identical output across runs and mappings

## Spark Equivalent

```python
from pyspark.sql.functions import sha2, md5, lit, col, concat, substring, expr, udf
import hashlib

# Key masking (deterministic): hash-based
@udf("string")
def mask_ssn(ssn, seed="42"):
    if ssn is None:
        return None
    h = hashlib.sha256(f"{ssn}{seed}".encode()).hexdigest()
    # Generate valid-format SSN from hash
    area = str(int(h[:4], 16) % 900 + 1).zfill(3)
    group = str(int(h[4:8], 16) % 99 + 1).zfill(2)
    serial = str(int(h[8:12], 16) % 9999 + 1).zfill(4)
    return f"{area}-{group}-{serial}"

df = df.withColumn("MASKED_SSN", mask_ssn(col("SSN"), lit("42")))

# Simple hash masking
df = df.withColumn("MASKED_NAME", sha2(col("NAME"), 256))

# Blurring (add random noise)
df = df.withColumn("MASKED_SAL", col("SAL") * (0.9 + expr("rand() * 0.2")))
```

## Example

```
-- Key masking on SSN with seed=42
-- Input: 123-45-6789 -> Output: 847-29-1563 (deterministic, repeatable)
-- Input: 123-45-6789 (again) -> Output: 847-29-1563 (same output)

# Spark equivalent:
df = df.withColumn("MASKED_SSN", mask_ssn_key(col("SSN"), lit(42)))
```