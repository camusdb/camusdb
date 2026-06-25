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
- **No inline SQL array literal.** You cannot write an array constant in a SQL statement. Array values
  are written through the parameter / HTTP path only (a JSON array bound to an `array` column).
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
| `bytes` | **`0x`-prefixed hex** | `0xDEADBEEF` |
| `array(T)` | — (not supported inline) | use a parameter |

Numeric literals are parsed with the invariant culture (`.` is always the decimal separator,
independent of server locale). Date/datetime strings are parsed as UTC; a value with no timezone is
assumed to be UTC. Unparseable date/datetime/bytes literals raise `InvalidInput`.

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
