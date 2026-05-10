---
name: informatica-string-functions
description: "Use when analyzing or migrating Informatica PowerCenter string functions to Spark SQL. Covers SUBSTR, INSTR, LPAD, RPAD, LTRIM, RTRIM, REPLACE, LOWER, UPPER, INITCAP, LENGTH, CONCAT, and more. Includes exact Spark SQL equivalents and padding behavior differences. Do NOT use for general string manipulation."
---

# Informatica PowerCenter String Functions → Spark SQL

## Quick Reference Table

| Informatica Function | Spark SQL Equivalent | Notes |
|---|---|---|
| `SUBSTR(string, start [, len])` | `substring(string, start, length)` | Both are 1-based |
| `INSTR(string, search [, start [, occ]])` | `instr(string, substring)` | Spark lacks start/occ params |
| `LPAD(string, length, pad)` | `lpad(string, length, pad)` | Identical |
| `RPAD(string, length, pad)` | `rpad(string, length, pad)` | Identical |
| `LTRIM(string [, trim_set])` | `ltrim(string)` | Spark trims spaces only |
| `RTRIM(string [, trim_set])` | `rtrim(string)` | Spark trims spaces only |
| `REPLACECHR(flag, str, old, new)` | `translate(str, from, to)` or `regexp_replace` | Character-level replace |
| `REPLACESTR(flag, str, s1, r1...)` | `regexp_replace(str, pattern, rep)` | String-level replace |
| `LOWER(string)` | `lower(string)` | Identical |
| `UPPER(string)` | `upper(string)` | Identical |
| `INITCAP(string)` | `initcap(string)` | Identical |
| `LENGTH(string)` | `length(string)` | Identical |
| `CONCAT(s1, s2)` | `concat(s1, s2)` or `\|\|` | Spark `concat` takes 2+ args |
| `REVERSE(string)` | `reverse(string)` | Identical |
| `CHR(number)` | `char(number)` | Identical |
| `ASCII(char)` | `ascii(string)` | Identical |
| `LTRIM(RTRIM(string))` | `trim(string)` | Both directions |
| `SUBSTR(string, -3)` | `substring(string, -3)` | Negative start supported |

---

## SUBSTR

**Syntax:** `SUBSTR( string, start [, length] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Source string |
| `start` | Integer | Yes | Start position (1-based; negative counts from end) |
| `length` | Integer | No | Number of characters to extract |

**Return Type:** String

**Spark SQL Equivalent:** `substring(string, start, length)`

**Example:**
```sql
-- Informatica
SUBSTR('Hello World', 1, 5)    -- returns 'Hello'
SUBSTR('Hello World', 7)       -- returns 'World'
SUBSTR('Hello World', -5)      -- returns 'World' (negative start)
SUBSTR('ABCDEF', 3, 2)         -- returns 'CD'

-- Spark SQL
substring('Hello World', 1, 5)  -- returns 'Hello'
substring('Hello World', 7)     -- returns 'World'
substring('Hello World', -5)    -- returns 'World'
substring('ABCDEF', 3, 2)       -- returns 'CD'
```

**WHY it matters:** Both Informatica and Spark use 1-based indexing for `SUBSTR`/`substring`. This is one of the few functions where behavior is identical. Negative start positions are supported in both.

**Negative case:** Do NOT assume 0-based indexing. `SUBSTR('ABC', 1, 1)` returns `'A'` in both platforms, not `'B'`.

---

## INSTR

**Syntax:** `INSTR( string, search_string [, start [, occurrence]] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to search within |
| `search_string` | String | Yes | Substring to find |
| `start` | Integer | No | Position to begin search (1-based, negative for reverse) |
| `occurrence` | Integer | No | Which occurrence to find (default: 1) |

**Return Type:** Integer (position, 0 if not found)

**Spark SQL Equivalent:** `instr(string, substring)` -- limited to first occurrence, no start position.

**Example:**
```sql
-- Informatica
INSTR('banana', 'na')           -- returns 3
INSTR('banana', 'na', 4)        -- returns 5 (start at position 4)
INSTR('banana', 'na', 1, 2)     -- returns 5 (2nd occurrence)
INSTR('banana', 'z')            -- returns 0 (not found)
INSTR('banana', 'NA', 1, 1, 1)  -- case-insensitive search

-- Spark SQL (basic)
instr('banana', 'na')           -- returns 3

-- Spark SQL (with start position - workaround)
instr(substring('banana', 4), 'na') + 3 - 1   -- returns 5

-- Spark SQL (case insensitive)
instr(lower('banana'), lower('NA'))           -- returns 3
```

**WHY it matters:** Spark's `instr` only finds the first occurrence starting from position 1. For advanced use cases (start position, nth occurrence, reverse search), use `regexp_extract` or string manipulation workarounds.

---

## LPAD / RPAD

**Syntax:** `LPAD( string, length, pad_string )` / `RPAD( string, length, pad_string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Source string |
| `length` | Integer | Yes | Total desired length |
| `pad_string` | String | Yes | String to pad with (repeated as needed) |

**Return Type:** String

**Spark SQL Equivalent:** `lpad(string, length, pad)` / `rpad(string, length, pad)`

**Example:**
```sql
-- Informatica
LPAD('42', 5, '0')      -- returns '00042'
RPAD('42', 5, '0')      -- returns '42000'
LPAD('hello', 10, 'x')  -- returns 'xxxxxhello'
RPAD('hello', 3, 'x')   -- returns 'hel' (truncates if longer)

-- Spark SQL
lpad('42', 5, '0')      -- returns '00042'
rpad('42', 5, '0')      -- returns '42000'
lpad('hello', 10, 'x')  -- returns 'xxxxxhello'
rpad('hello', 3, 'x')   -- returns 'hel' (truncates if longer)
```

**WHY it matters:** Identical behavior. If source exceeds target length, both truncate. Multi-character pad strings are repeated in full in both platforms.

---

## LTRIM / RTRIM

**Syntax:** `LTRIM( string [, trim_set] )` / `RTRIM( string [, trim_set] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | Source string |
| `trim_set` | String | No | Characters to remove (default: whitespace) |

**Return Type:** String

**Spark SQL Equivalent:** `ltrim(string)` / `rtrim(string)` -- spaces only. `trim(BOTH/LEADING/TRAILING 'chars' FROM string)` for custom sets.

**Example:**
```sql
-- Informatica
LTRIM('  hello  ')            -- returns 'hello  '
RTRIM('  hello  ')            -- returns '  hello'
LTRIM('xxhelloxx', 'x')       -- returns 'helloxx'
RTRIM('xxhelloxx', 'x')       -- returns 'xxhello'
LTRIM(RTRIM('  hello  '))     -- returns 'hello'

-- Spark SQL (spaces only)
ltrim('  hello  ')            -- returns 'hello  '
rtrim('  hello  ')            -- returns '  hello'
trim('  hello  ')             -- returns 'hello' (both sides)

-- Spark SQL (custom trim set)
trim(LEADING 'x' FROM 'xxhelloxx')   -- returns 'helloxx'
trim(TRAILING 'x' FROM 'xxhelloxx')  -- returns 'xxhello'
trim(BOTH 'x' FROM 'xxhelloxx')      -- returns 'hello'
```

**WHY it matters:** Spark's `ltrim`/`rtrim` only remove spaces. For custom character sets, use `trim(FROM ...)` syntax or `regexp_replace`. This is a common migration pitfall.

---

## REPLACECHR

**Syntax:** `REPLACECHR( case_flag, string, old_character_set, new_character_set )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `case_flag` | Integer | Yes | 0 = case-sensitive, 1 = case-insensitive |
| `string` | String | Yes | Source string |
| `old_character_set` | String | Yes | Characters to replace |
| `new_character_set` | String | Yes | Replacement characters (1-to-1 mapping) |

**Return Type:** String

**Spark SQL Equivalent:** `translate(string, from, to)`

**Example:**
```sql
-- Informatica
REPLACECHR(0, 'hello-world_test', '-_', '  ')  -- returns 'hello world test'
REPLACECHR(1, 'Hello', 'h', 'j')               -- returns 'jello' (case-insensitive)

-- Spark SQL
translate('hello-world_test', '-_', '  ')       -- returns 'hello world test'
translate(lower('Hello'), 'h', 'j')              -- case-insensitive workaround
```

**WHY it matters:** `translate` performs a 1-to-1 character mapping, exactly like `REPLACECHR`. For string-level replacement (replacing whole substrings), use `regexp_replace` instead.

---

## REPLACESTR

**Syntax:** `REPLACESTR( case_flag, string, search1, replace1 [, search2, replace2 ...] )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `case_flag` | Integer | Yes | 0 = case-sensitive, 1 = case-insensitive |
| `string` | String | Yes | Source string |
| `searchN` | String | Yes | String to find |
| `replaceN` | String | Yes | Replacement string |

**Return Type:** String

**Spark SQL Equivalent:** `regexp_replace(string, pattern, replacement)` -- chain for multiple replacements.

**Example:**
```sql
-- Informatica
REPLACESTR(0, 'hello world', 'world', 'spark')     -- returns 'hello spark'
REPLACESTR(0, 'a-b-c', '-', '|')                   -- returns 'a|b|c'
REPLACESTR(1, 'Hello', 'h', 'j')                   -- returns 'jello' (case-insensitive)

-- Spark SQL
regexp_replace('hello world', 'world', 'spark')    -- returns 'hello spark'
regexp_replace('a-b-c', '-', '|')                  -- returns 'a|b|c'
regexp_replace('Hello', '(?i)h', 'j')              -- case-insensitive with (?i)
```

---

## LOWER / UPPER / INITCAP

**Syntax:** `LOWER(string)` / `UPPER(string)` / `INITCAP(string)`

**Return Type:** String

**Spark SQL Equivalent:** Identical function names.

**Example:**
```sql
-- Informatica
LOWER('Hello World')    -- returns 'hello world'
UPPER('Hello World')    -- returns 'HELLO WORLD'
INITCAP('hello world')  -- returns 'Hello World'

-- Spark SQL
lower('Hello World')    -- returns 'hello world'
upper('Hello World')    -- returns 'HELLO WORLD'
initcap('hello world')  -- returns 'Hello World'
```

---

## LENGTH

**Syntax:** `LENGTH( string )`

**Arguments:**
| Name | Type | Required | Description |
|---|---|---|---|
| `string` | String | Yes | String to measure |

**Return Type:** Integer

**Spark SQL Equivalent:** `length(string)` or `char_length(string)`

**Example:**
```sql
-- Informatica
LENGTH('hello')         -- returns 5
LENGTH('')              -- returns 0
LENGTH(NULL)            -- returns NULL

-- Spark SQL
length('hello')         -- returns 5
length('')              -- returns 0
length(NULL)            -- returns NULL
```

**WHY it matters:** Identical behavior. Both return `NULL` for `NULL` input. For byte length instead of character length, use `octet_length()` in Spark.

---

## CONCAT

**Syntax:** `CONCAT( string1, string2 )` -- Informatica takes exactly 2 arguments.

**Return Type:** String

**Spark SQL Equivalent:** `concat(string1, string2, ...)` -- Spark takes 2+ arguments.

**Example:**
```sql
-- Informatica
CONCAT('Hello', ' World')                    -- returns 'Hello World'
CONCAT(CONCAT(FirstName, ' '), LastName)     -- nested for 3+ values

-- Spark SQL
concat('Hello', ' World')                    -- returns 'Hello World'
concat(FirstName, ' ', LastName)             -- 3+ args supported directly
'Hello' || ' ' || 'World'                    -- || operator
```

**WHY it matters:** Spark's `concat` accepts any number of arguments, making it more flexible. Use `||` operator for readability in complex expressions.

---

## REVERSE

**Syntax:** `REVERSE( string )`

**Return Type:** String

**Spark SQL Equivalent:** `reverse(string)`

**Example:**
```sql
-- Informatica
REVERSE('hello')        -- returns 'olleh'

-- Spark SQL
reverse('hello')        -- returns 'olleh'
```

---

## CHR / ASCII

**Syntax:** `CHR( number )` / `ASCII( char )`

**Return Type:** String (`CHR`) / Integer (`ASCII`)

**Spark SQL Equivalent:** `char(number)` / `ascii(string)`

**Example:**
```sql
-- Informatica
CHR(65)                 -- returns 'A'
ASCII('A')              -- returns 65

-- Spark SQL
char(65)                -- returns 'A'
ascii('A')              -- returns 65
```

---

## Key Migration Patterns

### Remove All Whitespace
```sql
-- Informatica
REPLACECHR(0, InputString, ' ', '')

-- Spark SQL
regexp_replace(InputString, ' ', '')
-- or remove ALL whitespace types:
regexp_replace(InputString, '\\s', '')
```

### Extract Domain from Email
```sql
-- Informatica
SUBSTR(Email, INSTR(Email, '@') + 1)

-- Spark SQL
substring(Email, instr(Email, '@') + 1)
```

### Pad with Zeros (Common ID Formatting)
```sql
-- Informatica
LPAD(CAST(CustomerID AS STRING), 10, '0')

-- Spark SQL
lpad(cast(CustomerID as string), 10, '0')
```

### Trim Custom Characters
```sql
-- Informatica
LTRIM(RTRIM(InputString, '*'), '*')

-- Spark SQL
trim(BOTH '*' FROM InputString)
```

### Null-Safe Concatenation
```sql
-- Informatica (concat returns NULL if any arg is NULL)
CONCAT(NVL(FirstName, ''), NVL(LastName, ''))

-- Spark SQL (concat returns NULL if any arg is NULL - same!)
concat(coalesce(FirstName, ''), coalesce(LastName, ''))
-- or use concat_ws which skips NULLs:
concat_ws(' ', FirstName, LastName)
```
