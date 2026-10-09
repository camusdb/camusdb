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
| `numeric` | exact decimal: 38 digits, 9 after the point | yes | Fixed precision and scale, as Spanner's NUMERIC. See [NUMERIC](#numeric). |
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
| `decimal` | `numeric` |

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

## NUMERIC

`numeric` (alias `decimal`) stores an exact decimal number with the semantics of Spanner GoogleSQL
NUMERIC. Use it for money and other values that a binary float cannot hold exactly.

| Property | Value |
|----------|-------|
| Precision | 38 significant digits |
| Scale | 9 digits after the decimal point, fixed |
| Range | `-99999999999999999999999999999.999999999` to `99999999999999999999999999999.999999999` |
| Rounding | Half away from zero, at the ninth fraction digit (`0.0000000005` → `0.000000001`) |
| Out of range | `CADB0417` (`NumericValueOutOfRange`): HTTP 400, gRPC `OUT_OF_RANGE` |
| Text form | Canonical: no exponent, no trailing zeros, no point for a whole number (`1.5`, `1200`, `0`) |

`NUMERIC(P)` and `NUMERIC(P, S)` are refused with `FeatureNotSupported`: the precision and the scale are
fixed, as in Spanner. `array(numeric)` is refused too.

### Writing a NUMERIC value

- The typed literal `NUMERIC '<text>'` is exact: `NUMERIC '12345678901234567890.123456789'`.
- A decimal literal written into a NUMERIC column converts from its source text, so
  `INSERT INTO t (p) VALUES (12345678901234567890.123456789)` stores every digit.
- An integer literal must fit `int64`. Write a wider whole number as `NUMERIC '…'` or with a decimal
  point.
- An `int64`, `float64`, `float32` or `string` value converts when it is written into a NUMERIC column.
  A computed `float64` converts from its exact binary value, so `123456789.123` computed as a double
  stores `123456789.122999996`.
- `CAST(x AS numeric)` accepts `int64`, `float64`, `float32` and `string`. `CAST(p AS int64)` rounds half
  away from zero, and fails when the result does not fit. `CAST(p AS string)` gives the canonical text.

### Expressions and result types

| Operation | Result |
|-----------|--------|
| `numeric` with `numeric` or `int64` (`+ - * /`, `%`) | `numeric`, exact; `*` and `/` round to 9 digits |
| `numeric` with `float64` or `float32` | `float64` |
| A bare decimal literal in an expression (`p + 0.1`) | `float64`, because the literal is a float; write `p + NUMERIC '0.1'` |
| `SUM(numeric)` | `numeric`, exact; only the final total is range-checked |
| `AVG(numeric)` | `numeric`, the exact total divided by the count, rounded half away from zero |
| `MIN`, `MAX`, `abs`, `ceil`, `floor`, `round(x[, n])`, `trunc(x[, n])`, `mod`, `sign` | `numeric` |
| `sqrt`, `pow` | `float64` (Spanner returns NUMERIC here) |
| `COALESCE`, `CASE` with `numeric` and `int64` branches | `numeric`; with a `float64` or `float32` branch, `float64` |

Division by zero fails. An overflow anywhere fails with `CADB0417`.

- `round(x, n)` and `trunc(x, n)` with `n` far left of the point (down to the minimum `int64`) give `0`.
- In a cluster, a `SUM` or `AVG` that can see a NUMERIC value is not split into a partial aggregate for
  each range. The node that runs the query adds the rows exactly, so a range total past the limit does
  not fail a query whose final result is in range.
- A derived table (a subquery in `FROM`) widens each NUMERIC column's cells to `numeric`, so that
  `COALESCE(p, 0)` gives NUMERIC `0` for a NULL `p` there. A top-level `SELECT` does not widen: for a
  NULL `p`, `SELECT COALESCE(p, 0)` returns the `int64` `0`, while the column metadata says `numeric`.

### Comparing NUMERIC values

- `numeric` against `numeric` or `int64` compares exactly: `9007199254740993` does not equal
  `9007199254740992`, although the two are one double.
- `numeric` against `float64` or `float32` compares as doubles. This rule is not transitive:
  `NUMERIC '9007199254740992'` and `NUMERIC '9007199254740993'` differ, but both equal
  `9007199254740992.0`. An `IN` list keeps every item, so `x IN (a, b, …)` is always `x = a OR x = b OR …`.
- `numeric` against `string` has no comparison rule (see
  [Comparing values of different types](#comparing-values-of-different-types)). Write the value as
  `NUMERIC '1.5'`.
- An index on a NUMERIC column is used for an `int64` or `NUMERIC '…'` constant. A float constant
  (`p < 9.99`) uses no index, because many NUMERIC values widen to one double. Write
  `p < NUMERIC '9.99'` to use the index.

### On the wire

A NUMERIC value is text on every transport, so no client rounds it through a double:

- HTTP and JSON: a JSON string, `"12345678901234567890.123456789"`. A parameter is
  `{"type": 12, "strValue": "…"}`.
- gRPC: the `numeric_value` field, column type `COLUMN_TYPE_NUMERIC`. See
  [grpc-client-protocol.md](grpc-client-protocol.md).

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

### Large values

A large `string`, `bytes` or `array` value can be stored compressed, or under its own key outside the
row. Each column has a storage strategy (`PLAIN`, `MAIN`, `EXTERNAL`, or the default `EXTENDED`) that
decides which forms the writer may use:

```sql
CREATE TABLE files (id oid PRIMARY KEY, name string, content bytes STORAGE EXTERNAL);
ALTER TABLE files ALTER COLUMN content SET STORAGE PLAIN;
```

The form never changes a result. It changes the I/O a query does and the size of each stored version.
See [Large values: compression and out-of-line storage](storage-layout.md).

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

Read one element with a subscript, `tags[1]`. The first element is `1`, and an index outside the array
gives `NULL`. See [PostgreSQL expression syntax](sql-expression-syntax.md#array-subscripts).

Three functions read an array as a whole:

| Function | Returns | Notes |
|---|---|---|
| `cardinality(a)` | `int64` | The number of elements, `NULL` elements included. `0` for an empty array. |
| `array_length(a, 1)` | `int64` | The same count, but `NULL` for an empty array, as in PostgreSQL. Any dimension other than `1` gives `NULL`. |
| `array_contains(a, v)` | `bool` | `true` when `v` is an element of `a`. |

All three return `NULL` for a `NULL` array. To count an empty array as `0`, use `cardinality`, not
`array_length`. `array_contains(a, v)` gives the same answer as `v IN (...)` over
the elements of `a`:

- An empty array gives `false`, even when `v` is `NULL`.
- A `NULL` value gives `NULL`.
- No match gives `NULL` when the array holds a `NULL` element, and `false` otherwise.
- Numbers compare by value, so `array_contains(ARRAY[1, 2], 2.0)` is `true`. A value of another type is
  a non-match, not an error.

PostgreSQL writes the same test as `v = ANY (a)`, and CamusDB accepts that form too: `v = ANY (a)`,
`v = SOME (a)` and `v <> ALL (a)` work, with the same rules. See
[`= ANY`, `= SOME` and `<> ALL`](sql-expression-syntax.md#-any--some-and--all). CamusDB does not
support `@>` or `<@` yet.

---

## Literal and value formats

How a value is written depends on the path. **The SQL literal form and the JSON form differ for
`bytes`** — note that below.

### In SQL statements

| Type | Literal form | Example |
|------|--------------|---------|
| `int64` | integer | `42` |
| `float64`, `float32` | decimal | `3.14` |
| `numeric` | `NUMERIC '…'`, or a decimal written into a NUMERIC column | `NUMERIC '9.99'` |
| `string` | quoted | `'hello'` or `"hello"` |
| `bool` | `true` / `false` | `true` |
| `oid` | quoted ObjectId string | `'652b...'` |
| `date` | quoted `yyyy-MM-dd` | `'2026-03-15'` |
| `datetime` | quoted ISO-8601 UTC | `'2026-03-15T12:00:00Z'` |
| `bytes` | **`X'…'` hex string** | `X'DEADBEEF'` |
| `array(T)` | `ARRAY[…]` | `ARRAY[1, 2, 3]` |

Numeric literals are parsed with the invariant culture (`.` is always the decimal separator,
independent of server locale). Date/datetime strings are parsed as UTC; a value with no timezone is
assumed to be UTC. Unparseable date/datetime/bytes literals raise `InvalidInput`. A decimal number
can group its digits with underscores, `200_000`; see
[PostgreSQL expression syntax](sql-expression-syntax.md#digit-separators).

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

An array literal is written `ARRAY[a, b, c]`. Each element can be any expression: a literal, a column,
a cast, a function call or arithmetic, as in `ARRAY[n::string, upper(name), n + 1]`. Its element type
is inferred from the first non-NULL element and every other element must agree, so `ARRAY[1, 'two']`
is an error. Elements coerce to the
column's declared element type the same way scalars do, so `ARRAY[1, 2]` is accepted by an
`array(float64)` column. `ARRAY[]` is empty and adopts the column's element type. Nested arrays
(`ARRAY[ARRAY[1]]`) are rejected.

`CAST` works for all scalar types, e.g. `CAST('2026-01-01' AS date)`, `CAST(x AS float32)`,
`CAST('0xFF00' AS bytes)`. The PostgreSQL shorthand `x::type` is the same as `CAST(x AS type)`; see
[PostgreSQL expression syntax](sql-expression-syntax.md#the--cast).

### Over HTTP / JSON

The HTTP API exchanges values as JSON:

| Type | JSON form |
|------|-----------|
| `int64`, `float64`, `float32` | JSON number |
| `numeric` | JSON string, canonical decimal text |
| `string` | JSON string |
| `bool` | JSON boolean |
| `id` | JSON string (ObjectId) |
| `date`, `datetime` | ISO-8601 string (responses include an `isoValue` field: `yyyy-MM-dd` for date, round-trip `o` format for datetime) |
| `bytes` | **base64 string** |
| `array(T)` | JSON array of the element type |

> **Bytes format differs by path:** SQL literals use `0x`-hex, JSON uses base64. A round-trip through
> each path is lossless, but the encodings are not interchangeable.

### Parameters of type `id`

A parameter of type `id` must hold an object id: 24 hex characters. The server converts upper-case hex
to lower case. Any other text fails with `CADB0400` (invalid input), and the message names the
parameter. The usual cause is a client that sends a GUID as an `id` parameter. A GUID has 16 bytes and
an object id has 12, so a GUID can never be an object id: send it as a `uuid` parameter.

### Comparing values of different types

Some pairs of types have a conversion rule: two numbers of different types (`int64` against `numeric`
exactly, any float as a double), and a `string` against a `uuid` or an `id` (the string is parsed).
Other pairs have no rule, for example a `uuid` against an `id`, an `int64` against a `uuid`, or a
`numeric` against a `string`. For such a pair:

- `=`, `IN` and a simple `CASE` find no match. `<>` and `NOT IN` find a match.
- `<`, `<=`, `>`, `>=` and `BETWEEN` fail with `CADB0400`, because the two types have no order.

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
date  datetime  timestamp  bytes  blob  array  numeric  decimal
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
