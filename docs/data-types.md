# Data Types

This document describes the column types CamusDB supports, how to declare them in `CREATE TABLE`, the
literal/value formats accepted on the SQL and HTTP paths, and the rules around length bounds, indexing,
and reserved keywords.

> Status: the type system is **alpha**. The on-disk format is backward compatible (see
> [Backward compatibility](#backward-compatibility)), but new types may gain capabilities (e.g. wider
> coercion or array indexing) in future releases.

---

## Type reference

| Type | Stores | Indexable | Notes |
|------|--------|-----------|-------|
| `oid` | 12-byte ObjectId | yes | CamusDB's native identifier type; shares its key encoding with `string`. Spelled `oid`/`object_id` in SQL, `id` over HTTP (see [aliases](#aliases)). |
| `int64` | 64-bit signed integer | yes | |
| `float64` | IEEE-754 double | yes | |
| `float32` | IEEE-754 single | yes | Stored at single precision; comparisons and storage narrow to `float`. |
| `bool` | boolean | yes | |
| `string` | UTF-16 text | yes | Length-bounded — see [String and bytes length](#string-and-bytes-length-bounds). |
| `string(N)` | UTF-16 text, max `N` chars | yes | `N` is a positive integer count of characters. |
| `date` | calendar date (no time) | yes | Stored as UTC ticks truncated to midnight. |
| `datetime` | instant in time (UTC) | yes | Stored as UTC ticks. |
| `bytes` | opaque byte string | yes | Length-bounded (default 10 MB). |
| `array(T)` | ordered list of `T` | **no** | `T` is a scalar type; see [Arrays](#arrays). |

### Aliases

These spellings are accepted as synonyms:

| Alias | Canonical type |
|-------|----------------|
| `int`, `integer` | `int64` |
| `real` | `float32` |
| `timestamp` | `datetime` |
| `blob` | `bytes` |
| `object_id` (SQL), `id` (HTTP) | `oid` |
| `boolean` | `bool` |

> The ObjectId type is spelled `oid` or `object_id` in SQL and `id` in HTTP create-table requests. The
> two paths do not currently accept each other's spelling.

### Example

```sql
CREATE TABLE events (
    id        oid NOT NULL,
    name      string(64),
    payload   bytes,
    score     float32,
    happened  datetime,
    day       date,
    tags      array(int64),
    PRIMARY KEY (id)
)
```

(Here the column is *named* `id` — a plain identifier — and its *type* is `oid`.)

---

## String and bytes length bounds

A `string(N)` column accepts at most `N` characters. A bare `string` column (no size) defaults to a
maximum of **2 621 440 characters**. A `bytes` column defaults to a maximum of **10 MB**
(10 485 760 bytes).

- **String length is measured in UTF-16 code units** (`.Length`), so a character outside the Basic
  Multilingual Plane (e.g. many emoji) counts as **2** toward the limit.
- **Bytes length is measured in bytes.**
- A `NULL` value has no length and is never rejected by the bound.

**Over-length values are rejected, never truncated.** An `INSERT` or `UPDATE` whose String/Bytes value
exceeds the column's bound fails with error code **`CADB0302` (`ValueTooLong`)** and a message naming the
column, the limit, and the actual length:

```
value too long for column 'name' (max 64, got 71)
```

This mirrors PostgreSQL's default behavior (`SQLSTATE 22001`). Unlike PostgreSQL, CamusDB does **not**
silently truncate trailing whitespace, and does **not** truncate on an explicit `CAST` to a narrower
bound — an overflowing cast raises the same error.

---

## Arrays

`array(T)` declares a homogeneous, ordered list of a **scalar** element type `T`:

```sql
CREATE TABLE t (id oid, tags array(int64), labels array(string), PRIMARY KEY (id))
```

Current (v1) limitations:

- **Element type must be scalar.** Nested arrays (`array(array(...))`) are rejected.
- **Arrays are not indexable.** An `array` column cannot appear in a `PRIMARY KEY` or any index — doing
  so is rejected at `CREATE TABLE` time.
- **Nested arrays are rejected**, in a literal as well as a declaration — `ColumnValue` models exactly
  one element type.
- Elements may be `NULL` regardless of the declared element type.

---

## Literal and value formats

How a value is written depends on the path. **The SQL literal form and the JSON form differ for
`bytes`** — note that below.

### In SQL statements

| Type | Literal form | Example |
|------|--------------|---------|
| `int64` | integer | `42` |
| `float64`, `float32` | decimal | `3.14` |
| `string` | quoted | `'hello'` or `"hello"` |
| `bool` | `true` / `false` | `true` |
| `oid` | quoted ObjectId string | `'652b...'` |
| `date` | quoted `yyyy-MM-dd` | `'2026-03-15'` |
| `datetime` | quoted ISO-8601 UTC | `'2026-03-15T12:00:00Z'` |
| `bytes` | **`X'…'` hex string** | `X'DEADBEEF'` |
| `array(T)` | `ARRAY[…]` | `ARRAY[1, 2, 3]` |

Numeric literals are parsed with the invariant culture (`.` is always the decimal separator,
independent of server locale). Date/datetime strings are parsed as UTC; a value with no timezone is
assumed to be UTC. Unparseable date/datetime/bytes literals raise `InvalidInput`.

#### String literals

There are two forms, following PostgreSQL.

**Plain — `'…'` or `"…"`.** No escape processing at all. A backslash is an ordinary character, and
the only special sequence is a doubled delimiter. Use this for essentially everything, including
regex patterns and Windows paths:

```sql
SELECT * FROM t WHERE name ~ '(\d+)';        -- pattern is (\d+), no doubling
INSERT INTO t (path) VALUES ('C:\Users');    -- stores C:\Users
SELECT 'it''s';                              -- stores it's
SELECT 'say "hi"';                           -- the other delimiter needs no escaping
```

**Escape — `E'…'` or `E"…"`.** A backslash introduces an escape, which is how control characters get
a spelling:

| Escape | Meaning |
|--------|---------|
| `\\` | backslash |
| `\'` `\"` | quote (or double the delimiter: `''`) |
| `\n` `\r` `\t` `\0` `\a` `\b` `\f` `\v` | control characters |
| `\NNN` | character from three octal digits |
| `\xHH` | character from two hex digits |
| `\uHHHH` `\UHHHHHHHH` | Unicode code point |

```sql
COMMENT ON TABLE t IS E'line1\nline2';       -- stores an embedded newline
```

An unrecognized escape yields the character itself, so `E'\d'` is `d` — which is exactly why regex
patterns belong in the plain form. A truncated numeric escape (`E'\x4'`) or an unpaired surrogate
raises `InvalidInput`.

Every value has a literal form, so anything that can be stored can also be emitted by
`SHOW CREATE TABLE` and read back unchanged. The server emits the plain form and falls back to
`E'…'` only when the value contains a control character.

A bytes literal is written `X'4D5A'` (or lowercase `x'…'`); `X''` is the empty byte string, and an
odd number of hex digits is an error. Note that a bare `0xFF` is an **integer** literal, not bytes —
it keeps that meaning, which is why bytes got their own syntax. The legacy form of passing bytes as a
string whose text starts with `0x` still coerces on insert, but only where the target column type is
known; `X'…'` carries its type on its own.

An array literal is written `ARRAY[a, b, c]`. Its element type is inferred from the first non-NULL
element and every other element must agree, so `ARRAY[1, 'two']` is an error. Elements coerce to the
column's declared element type the same way scalars do, so `ARRAY[1, 2]` is accepted by an
`array(float64)` column. `ARRAY[]` is empty and adopts the column's element type. Nested arrays
(`ARRAY[ARRAY[1]]`) are rejected.

`CAST` works for all scalar types, e.g. `CAST('2026-01-01' AS date)`, `CAST(x AS float32)`,
`CAST('0xFF00' AS bytes)`.

### Over HTTP / JSON

The HTTP API exchanges values as JSON:

| Type | JSON form |
|------|-----------|
| `int64`, `float64`, `float32` | JSON number |
| `string` | JSON string |
| `bool` | JSON boolean |
| `id` | JSON string (ObjectId) |
| `date`, `datetime` | ISO-8601 string (responses include an `isoValue` field: `yyyy-MM-dd` for date, round-trip `o` format for datetime) |
| `bytes` | **base64 string** |
| `array(T)` | JSON array of the element type |

> **Bytes format differs by path:** SQL literals use `0x`-hex, JSON uses base64. A round-trip through
> each path is lossless, but the encodings are not interchangeable.

To declare `string(N)` / `array(T)` over HTTP, the create-table column carries `maxLength` and
`arrayElementType` fields alongside `type`.

---

## Temporal functions

The built-in date/time functions return engine-typed `date` or `datetime` values, not strings. The
JSON wire representation is unchanged (ISO-8601 string via the `isoValue` field), but the engine
type is now correct — so comparisons against typed columns, INSERT without CAST, and indexing all
work with function results.

| Function | Return type | Notes |
|----------|-------------|-------|
| `NOW()` / `CURRENT_TIMESTAMP()` | `datetime` | Current UTC instant |
| `CURRENT_DATE()` | `date` | Current UTC date |
| `DATE_ADD(temporal, n, unit)` | `datetime` | See promotion rule below |
| `DATE_TRUNC(unit, temporal)` | `datetime` | Always yields datetime (Postgres-compatible) |
| `FROM_UNIXTIME(seconds)` | `datetime` | Unix epoch seconds → UTC datetime |
| `DATE_DIFF(a, b, unit)` | `int64` | Difference in `unit` units (unchanged) |
| `DATE_PART(unit, temporal)` | `int64` | Extracts calendar component (unchanged) |
| `UNIX_TIMESTAMP([temporal])` | `int64` | UTC seconds since epoch (unchanged) |

**DATE_ADD promotion rule:** the return type is always `datetime`. The evaluator promotes any `date`
input to `datetime` before applying the arithmetic, consistent with Postgres `date + interval`
behavior. The unit argument stays a string literal (`'day'`, `'hour'`, etc.).

**Typed temporal arguments:** all temporal-consuming functions (`DATE_ADD`, `DATE_DIFF`, `DATE_PART`,
`DATE_TRUNC`, `UNIX_TIMESTAMP`) accept `date` or `datetime` column references directly, in addition
to string literals. Passing a typed temporal column no longer requires `CAST` to string.

**Wire format unchanged:** `NOW()` still serializes as an ISO-8601 string in JSON responses (via
`isoValue`). Only the engine-internal type changes.

Examples:

```sql
-- INSERT without CAST (NOW() returns datetime, not string)
INSERT INTO events (id, created) VALUES (gen_id(), NOW())

-- Typed comparison (datetime column vs datetime function result)
SELECT * FROM events WHERE created < NOW()

-- Typed column through DATE_ADD
SELECT DATE_ADD(created, 7, 'day') FROM events
```

---

## Reserved keywords

The type keywords (and their aliases) are **reserved words** and cannot be used as column or table names
in SQL. The reserved type words are:

```
oid  object_id  int  int64  integer  string  bool  boolean  float32  float64  real
date  datetime  timestamp  bytes  blob  array
```

Note that `id` is **not** a SQL type keyword and remains usable as an identifier (column/table name) —
the ObjectId type is spelled `oid`/`object_id` in SQL.

Only the exact keyword is reserved — identifiers that merely *start with* a keyword (e.g. `internal`,
`interval`, `dates`) are fine. Tables created through the HTTP / programmatic API are unaffected by SQL
keyword reservation, since they bypass the SQL parser.

---

## Indexing

Every scalar type is range-indexable: index keys use an order-preserving, pure-ASCII encoding so that
key order matches value order in both the in-memory and on-disk paths. `array` is the only type that
cannot be indexed.

`string` and `oid` share the same key encoding, so a quoted string literal in a query matches an
`oid`-typed stored key (e.g. `WHERE id = '652b...'`, where `id` is an `oid`-typed column).

---

## Backward compatibility

Rows are self-describing per schema version: each stored row records the schema version it was written
under and decodes against that version's column layout. Adding a column of a new type bumps the schema
version; existing rows keep decoding against their original layout with no migration. This makes adding
new-typed columns to an existing table a safe, online operation.
