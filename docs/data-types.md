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
| `int64` | 64-bit signed integer | yes | Range `-9223372036854775808` to `9223372036854775807`. A result outside it is an error, never a wrapped value. See [INT64 overflow](#int64-overflow). |
| `smallint` | `int64` with a 16-bit range | yes | Range `-32768` to `32767`. A value outside it is an error on a write and on `CAST`. See [Integer type names](#integer-type-names). |
| `int`, `integer` | `int64` with a 32-bit range | yes | Range `-2147483648` to `2147483647`. A value outside it is an error on a write and on `CAST`. See [Integer type names](#integer-type-names). |
| `float64` | IEEE-754 double | yes | `'Infinity'`, `'-Infinity'` and `'NaN'` are values. An overflow or an underflow is an error. See [Float values](#float-values-infinity-nan-and-overflow). |
| `float32` | IEEE-754 single | yes | Stored at single precision; comparisons and storage narrow to `float`. A value past the `float32` range is an error, never infinity. |
| `numeric` | exact decimal: 38 digits, 9 after the point | yes | Fixed precision and scale, as Spanner's NUMERIC. See [NUMERIC](#numeric). |
| `bool` | boolean | yes | |
| `string` | UTF-16 text | yes | Length-bounded — see [String and bytes length](#string-and-bytes-length-bounds). |
| `string(N)` | UTF-16 text, max `N` chars | yes | `N` is a positive integer count of characters. |
| `date` | calendar date (no time) | yes | Stored as UTC ticks truncated to midnight. |
| `datetime` | instant in time (UTC) | yes | Stored as UTC ticks. A `date` value written to a `datetime` column (INSERT, UPDATE, DEFAULT) becomes midnight UTC of that day. A `datetime` value written to a `date` column is refused, because it would lose the time of day. |
| `bytes` | opaque byte string | yes | Length-bounded (default 10 MB). |
| `array(T)` | ordered list of `T` | **no** | `T` is a scalar type; see [Arrays](#arrays). |
| `jsonb` | JSON document, binary (canonical CBOR) | GIN only | PostgreSQL operators and functions. No ordered index, primary key, `UNIQUE` or foreign key. See [json.md](json.md). |

### Aliases

These spellings are accepted as synonyms:

| Alias | Canonical type |
|-------|----------------|
| `bigint` | `int64` |
| `real` | `float32` |
| `double precision` | `float64` |
| `timestamp` | `datetime` |
| `blob` | `bytes` |
| `object_id` (SQL), `id` (HTTP) | `oid` |
| `boolean` | `bool` |
| `decimal` | `numeric` |
| `json` | `jsonb` (the text is canonical; see [json.md](json.md#differences-from-postgresql)) |

An alias is accepted everywhere a type is written: a column definition, `ALTER TABLE ... ADD COLUMN`,
`CAST(x AS bigint)`, `x::bigint`, and an array element type such as `array(bigint)`. The spelling
is not kept: `SHOW CREATE TABLE` renders the canonical type. `bigint` and the word `precision` stay
usable as table and column names.

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

## Integer type names

Each integer type name promises a range, and the engine enforces it. These are the PostgreSQL ranges.

| Name | Range | `SHOW CREATE TABLE` and `SHOW COLUMNS` |
|------|-------|----------------------------------------|
| `smallint` | `-32768` to `32767` | `SMALLINT` |
| `int`, `integer` | `-2147483648` to `2147483647` | `INT` |
| `int64`, `bigint` | `-9223372036854775808` to `9223372036854775807` | `INT64` |

A value outside the range fails with `CADB0420` (`IntegerValueOutOfRange`): HTTP 400, gRPC
`OUT_OF_RANGE`. The statement stores nothing. The rule applies to:

- `INSERT`, `INSERT … SELECT`, `UPDATE`, both arms of `ON CONFLICT`, and the HTTP and gRPC row
  endpoints. This includes a value that a sequence default or a trigger gives.
- A constant `DEFAULT`, when the statement that declares it runs.
- `ALTER TABLE … ADD COLUMN`. A sequence or function default that passes the range while it fills the
  existing rows stops the fill, and the column is removed.
- Each element of an array column, such as `array(smallint)`.
- `CAST(x AS smallint)`, `x::int` and the other narrow spellings. The result is an `int64` value. A float
  rounds first, so `CAST(32767.5 AS smallint)` fails.

The range is a write rule, and every name stores as `int64`:

- Arithmetic is `int64`. If `s` is a `smallint` that holds `32767`, then `SELECT s + 1` gives `32768`.
  `UPDATE … SET s = s + 1` fails, because it writes that result to the column.
- A query result and a wire value are `int64`, and a comparison reads an `int64`. `WHERE s = 40000`
  finds no row, and it is not an error.
- An `int64` column and a `smallint` column have the same row bytes and index keys, so no storage
  revision is involved.

Differences and limits:

- **A column created before this rule keeps no limit.** The catalog did not record the name, so an old
  column declared `int` or `smallint` can hold any `int64`, and it renders as `INT64`.
- A stored `CHECK` or view body from before this rule keeps no limit. The engine stored its
  `CAST(x AS int)` as `CAST(x AS int64)`.
- `serial` and `bigserial` are `int64`. PostgreSQL makes `serial` 32 bits.
- A sequence on a narrow column keeps its own bounds. PostgreSQL limits the sequence to the column type.
  Here the sequence goes on, and the write of a value past the range fails with `CADB0420`.
- `CREATE TABLE … AS SELECT` makes `int64` columns, whatever the source column declares. PostgreSQL keeps
  the type of the source column.

---

## INT64 overflow

An `int64` value is never wrapped. A result outside the range `-9223372036854775808` to
`9223372036854775807` fails with `CADB0420` (`IntegerValueOutOfRange`): HTTP 400, gRPC
`OUT_OF_RANGE`. PostgreSQL reports the same condition as `bigint out of range`. A statement that
overflows stores nothing: an `UPDATE`, `INSERT` or `DELETE` fails as a whole, and in an explicit
transaction the failed statement aborts the transaction.

These operations are checked:

- `+`, `-`, `*` and `/` with two `int64` operands, and unary minus. `-9223372036854775808 / -1` and
  `-(-9223372036854775807 - 1)` fail.
- `SUM` of `int64` values, as a plain, grouped or window aggregate. Only the final total is checked,
  so a running total that passes the range and comes back gives the exact result, whatever the order
  of the rows. `AVG` divides the exact total and never overflows.
- An integer literal past the range, such as `9223372036854775808`. Write a wider number as
  `NUMERIC '…'`.
- A conversion to `int64`: `CAST` of a `float64`, a `numeric`, a `string` or a `jsonb` number, and
  `abs(-9223372036854775808)`. A string that is not an integer, such as `'12abc'`, fails with
  `CADB0400` instead.

These operations do not fail:

- `%` (`mod`): `-9223372036854775808 % -1` is `0`, as in PostgreSQL.
- The shift operators `<<` and `>>` work on bits, not on values, and use the count modulo 64 (see
  [SQL expression syntax](sql-expression-syntax.md)).
- A sequence never wraps. `nextval` past `MAXVALUE`, which is at most `9223372036854775807`, fails
  with `CADB0544`.

An overflow in a `WHERE` clause is an error too, so `WHERE v + 1 > 0` cannot drop or keep a row by a
wrapped comparison. In a cluster, a `SUM` or `AVG` of an `int64` column that is split into a partial
aggregate for each range adds each range exactly, and checks only the final total.

---

## Float values: Infinity, NaN and overflow

`float64` and `float32` follow the PostgreSQL rule for `float8` and `float4`.

### Infinity and NaN are values

`'Infinity'`, `'-Infinity'` and `'NaN'` are legal values. Write them as text, which converts to a
float column as a PostgreSQL string literal does, or with `CAST`, or as a parameter:

```sql
INSERT INTO readings (id, v) VALUES (1, 'Infinity'), (2, '-Infinity'), (3, 'NaN');
SELECT CAST('NaN' AS float64);
SELECT * FROM readings WHERE v = CAST('Infinity' AS float64);
```

- The text form is case-insensitive and accepts `NaN`, `Infinity`, `+Infinity`, `-Infinity`, `inf`,
  `+inf` and `-inf`, with white space around it. Other spellings, such as `∞`, fail with `CADB0400`.
- A row that holds one of these values works like any other row. You can read, compare, sort, group,
  index, update and delete it, and the value survives a restart.
- An operation on an infinite or NaN value gives the IEEE result: `'Infinity' + 1` is `Infinity`,
  `'Infinity' - 'Infinity'` is `NaN`, and `1 / 'Infinity'` is `0`.
- A `SUM` or `AVG` over values that include an infinity or a NaN gives the IEEE total.
- `CAST` to `int64` of an infinity or a NaN fails with `CADB0420`, as PostgreSQL reports
  `bigint out of range`. `CAST` to `numeric` fails with `CADB0400`, because a CamusDB `numeric` has
  no infinity and no NaN.
- `CAST(x AS string)` gives `Infinity`, `-Infinity`, `NaN` and `-0`.
- `to_jsonb` of an infinity or a NaN gives the JSON string `"Infinity"`, `"-Infinity"` or `"NaN"`,
  as in PostgreSQL, because JSON has no number for it.

### Order and equality

- The order is: `-Infinity` < every number < `Infinity` < `NaN`. `NaN` sorts above every other value,
  so `ORDER BY v DESC` puts it first, and `MAX(v)` is `NaN` when a row holds it.
- `NaN = NaN` is true, so `WHERE v = CAST('NaN' AS float64)` finds the NaN rows, and `GROUP BY` and
  `DISTINCT` give one group for them. `NaN > 1` is true.
- `-0` equals `0`. A UNIQUE index refuses `-0` next to `0`, and `WHERE v = 0` finds both rows. A stored
  `-0` reads back as `-0`, except where a query reads the value from an index key (a covering index
  read), which gives `0`.
- This order differs from IEEE comparison, where every comparison with NaN is false. It is the order of
  PostgreSQL.

### Overflow and underflow are errors

A result from finite values that a float cannot hold fails with `CADB0421`
(`FloatValueOutOfRange`): HTTP 400, gRPC `OUT_OF_RANGE`. It never becomes an infinity or a zero.

- An *overflow* is a result too large for the type: `1e308 * 10`, `pow(10, 400)`, `exp(1000)`, the literal `1e400`,
  `CAST('1e400' AS float64)`, or a `SUM` or `AVG` total past the range.
- An *underflow* is a nonzero result too small for the type: `1e-300 * 1e-300`, `pow(10, -400)`, `exp(-1000)`, or
  the literal `1E-400`. A literal or a text with a zero mantissa, such as `0e-400`, is zero.
- A subnormal result is a value, not an underflow: `4e-320` is stored as it is.
- A `float64` value written to a `float32` column, or cast to `float32`, fails when it is past the
  `float32` range (`1e300`) or rounds to zero (`1e-300`). Arithmetic on `float32` values checks the
  `float32` range too.
- A statement that overflows stores nothing, as an `int64` overflow does (see
  [INT64 overflow](#int64-overflow)).
- A plain `SUM` adds in row order and fails when the running total passes the range. A window `SUM`
  adds the frame exactly and checks only its total.

These operations have no real result and fail with `CADB0400`: `x / 0` for every `x`, `pow(0, y)` for
a negative `y`, `pow(x, y)` for a negative `x` and a finite non-integer `y`, `sqrt` of a negative
number, `ln`, `log` and `log10` of zero or of a negative number, `log(1, x)`, and `sign(NaN)`. `round(x, n)` and `trunc(x, n)` accept any scale and never turn a finite
value into a NaN or an infinity.

### Over the wire

- **HTTP JSON** has no number for an infinity or a NaN. A `float64` or `float32` cell writes them as
  the strings `"Infinity"`, `"-Infinity"` and `"NaN"`. The column's `type` is still the float type, so
  a client reads a string in a float column as that value. Every finite value, `-0` included, is a
  JSON number. A float parameter accepts the same strings: `{"type": 5, "floatValue": "NaN"}`.
- **gRPC** carries the IEEE value in the `double` or `float` field, with no conversion.

### Indexes that hold -0 or NaN from before

An index key now encodes `-0` as `0`, and every NaN as one key above `Infinity`. Before, `-0` had its
own key below `0`, and a NaN with the sign bit set had a key below `-Infinity`. An index built before
this rule on a float column that holds `-0` or a NaN has keys of the old form. Rebuild such an index
with `DROP INDEX` and `CREATE INDEX`. An index on a column that holds neither value needs no rebuild.

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
| `sqrt`, `pow` and `^`, `ln`, `log`, `log10`, `exp` | `float64` (Spanner returns NUMERIC for `sqrt`, `pow`, `ln`, `log` and `log10`) |
| `COALESCE`, `CASE` with `numeric` and `int64` branches | `numeric`; with a `float64` or `float32` branch, `float64` |

Division by zero fails. An overflow anywhere fails with `CADB0417`.

- `round(x, n)` and `trunc(x, n)` with `n` far left of the point (down to the minimum `int64`) give `0`.
- In a cluster, a `SUM` or `AVG` that can see a NUMERIC value is not split into a partial aggregate for
  each range. The node that runs the query adds the rows exactly, so a range total past the limit does
  not fail a query whose final result is in range.
- `COALESCE(p, 0)` gives NUMERIC `0` for a NULL `p`, at the top level and in a derived table: the
  result type of a `CASE`, `COALESCE`, `IFNULL` or `IF` comes from the declared column types, and
  every value is converted to it (see
  [Result types of CASE, COALESCE, IFNULL and IF](#result-types-of-case-coalesce-ifnull-and-if)).

### Comparing NUMERIC values

- `numeric` against `numeric` or `int64` compares exactly: `9007199254740993` does not equal
  `9007199254740992`, although the two are one double.
- `numeric` against `float64` or `float32` compares as doubles. This rule is not transitive:
  `NUMERIC '9007199254740992'` and `NUMERIC '9007199254740993'` differ, but both equal
  `9007199254740992.0`. An `IN` list keeps every item, so `x IN (a, b, …)` is always `x = a OR x = b OR …`.
- `numeric` against `string` has no comparison rule, so `p = '1.5'` fails with `CADB0400` (see
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
- Numbers compare by value, so `array_contains(ARRAY[1, 2], 2.0)` is `true`. A value whose type has no
  comparison rule with the element type fails with `CADB0400`, as `'x' IN (1, 2)` does:
  `array_contains(ARRAY[1, 2], 'x')` is an error (see
  [Comparing values of different types](#comparing-values-of-different-types)).

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
assumed to be UTC. A `datetime` string can also be a bare date, `'2026-03-15'`, which is midnight UTC
of that day. Unparseable date/datetime/bytes literals raise `InvalidInput`. A decimal number
can group its digits with underscores, `200_000`; see
[PostgreSQL expression syntax](sql-expression-syntax.md#digit-separators).

#### Comparing dates, datetimes and strings

A comparison (`=`, `<>`, `<`, `BETWEEN`, `IN`, a simple `CASE`, a `CHECK`) between a `date` and a
`datetime` compares the date as midnight UTC of that day, as in PostgreSQL. A string compared with a
`date` or a `datetime` is parsed into that type with the literal forms above, so no `CAST` is needed:

```sql
SELECT * FROM events WHERE day = '2026-03-15';                 -- date column
SELECT * FROM events WHERE created >= '2026-03-01'             -- datetime column, from midnight UTC
                       AND created <  '2026-04-01';
SELECT * FROM events WHERE created = day;                      -- true only at midnight
SELECT * FROM events WHERE day < make_timestamp(2026, 3, 15, 10, 0, 0);  -- includes 2026-03-15
```

A string that does not parse is an `InvalidInput` error, not a row that fails to match: with a
silent non-match, `created >= '2026-13-01'` would look like a valid empty result. A `date` takes only
the `yyyy-MM-dd` form, so `day = '2026-03-15T00:00:00Z'` is an error too. An index on a `date` or
`datetime` column serves these comparisons, because the planner converts the constant into the
column's type before it builds the index key.

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

#### Cast to `int64`

`CAST(x AS int64)`, `x::int64` and `to_int64(x)` use one rule. A number with a fraction rounds half
away from zero, for every numeric type:

| Value | `float64`, `float32`, `numeric`, `jsonb` number |
|-------|-------------------------------------------------|
| `0.4` | `0` |
| `0.5` | `1` |
| `-0.5` | `-1` |
| `1.5` | `2` |
| `2.5` | `3` |
| `-2.5` | `-3` |

- This is the GoogleSQL rule, and `round(x)` gives the same integer. PostgreSQL uses it for `numeric`
  and `jsonb`, but rounds `float8::bigint` half to even, so `2.5::float8::bigint` is `2` in PostgreSQL.
- The cast reads the exact binary value of a float, so `0.49999999999999994` gives `0`. A value that
  the float type cannot hold is already rounded when it is stored: `9007199254740993.0` is the `float64`
  value `9007199254740992`.
- `NaN`, `Infinity`, `-Infinity` and a result past the `int64` range fail with `CADB0420`
  (`IntegerValueOutOfRange`): HTTP 400, gRPC `OUT_OF_RANGE`.
- A string must spell an integer. `CAST('2.5' AS int64)` fails with `CADB0400`, and it does not round.
  `doc ->> 'a'` gives a string, so cast `doc -> 'a'` to round a `jsonb` number.
- There is no implicit conversion. An `int64` column, an integer function argument, an array subscript,
  a bitwise operator and a `LIMIT` or `OFFSET` parameter refuse a float. Write the `CAST` where you want
  the rounded value.

#### Text form of each type

Every scalar type has one text form. `CAST(x AS string)`, `x::string`, `to_string(x)`,
`concat(…)` and the `||` operator all use it, so they always give the same text:

| Type | Text form | Example |
|------|-----------|---------|
| `string` | the value | `hello` |
| `oid` | the ObjectId | `652b1f0e3c9d4a2b1c0d9e8f` |
| `int64` | decimal digits | `-7` |
| `float64` | shortest text that reads back as the same `float64` | `0.1` |
| `float32` | shortest text that reads back as the same `float32` | `1.1` |
| `numeric` | canonical decimal, no exponent, no trailing zeros | `12.5` |
| `bool` | `true` / `false` | `true` |
| `uuid` | lowercase, with hyphens | `550e8400-e29b-41d4-a716-446655440000` |
| `date` | `yyyy-MM-dd` | `2026-03-15` |
| `datetime` | ISO-8601 in UTC, seven fractional digits | `2026-03-15T08:30:00.1230000Z` |
| `bytes` | `\x` and lowercase hex (PostgreSQL `bytea_output = hex`) | `\xdeadbeef` |
| `array(T)` | none: the cast, `concat` and `\|\|` raise `InvalidInput` | |
| `jsonb` | the PostgreSQL `jsonb_out` text; `\|\|` with a document merges documents instead (see [json.md](json.md#operators)) | `{"a": [1, 2]}` |

The text form uses the invariant culture and depends on the value only, so it is safe in a column
`DEFAULT` and a stored `CHECK`. `concat` skips a NULL argument and returns NULL when every argument is
NULL. The `||` operator gives NULL for a NULL operand; see
[Concatenation](sql-expression-syntax.md#concatenation-).

The string cast reads each text form back, so `CAST(CAST(x AS string) AS T)` returns `x` for `date`,
`datetime`, `bytes`, `float32`, `float64`, `uuid` and `numeric`. A string cast to `bytes` accepts the
hex after either a `\x` or a `0x` prefix. Write `'\xdeadbeef'` as a plain string literal: a plain
literal does no escape processing, so the backslash stays in the text.

#### Bytes functions

To convert `bytes` to text and back with an explicit format (`hex`, `base64`, `escape`), and to read
or change single bytes and bits, see [bytes-functions.md](bytes-functions.md).

### Over HTTP / JSON

The HTTP API exchanges values as JSON:

| Type | JSON form |
|------|-----------|
| `int64` | JSON number |
| `float64`, `float32` | JSON number; an infinity or a NaN is the string `"Infinity"`, `"-Infinity"` or `"NaN"` (see [Float values](#over-the-wire)) |
| `numeric` | JSON string, canonical decimal text |
| `string` | JSON string |
| `bool` | JSON boolean |
| `id` | JSON string (ObjectId) |
| `date`, `datetime` | ISO-8601 string (responses include an `isoValue` field: `yyyy-MM-dd` for date, round-trip `o` format for datetime) |
| `bytes` | **base64 string** |
| `array(T)` | JSON array of the element type |
| `jsonb` | the document itself as native JSON; numbers keep all their digits. With `"jsonbAsText": true` on the request, a JSON string that holds the text form, so JSON `null` differs from SQL `NULL` (see [json.md](json.md#json-null-over-rest)). A parameter is `{"type": 13, "strValue": "<json text>"}` |

> **Bytes format differs by path:** SQL literals use `0x`-hex, JSON uses base64. A round-trip through
> each path is lossless, but the encodings are not interchangeable.

#### Array columns in REST query results

The REST result endpoints (`/execute-sql-query`, `/execute-sql-query-stream`, and the
`RETURNING` result of `/execute-sql-non-query`) write rows positionally, without a per-cell type.
Each entry of `columns` gives the column's `type` as a `ColumnType` number. An array is written as a
plain JSON array of its element values: a `date` element as ticks, a `bytes` element as base64, a
`float64` zero as `0`. To decode the elements, a client reads `elementType`, which an array column
(`type` 10) carries:

```json
{"name": "ad", "type": 10, "elementType": 8}
```

`elementType` uses the same numbers as `type`. A non-array column has no `elementType` field. A
table column's element type comes from its declaration, so it is present even when the result has
no rows. For a computed array, such as a function result, it comes from the first non-null array in
the result. If the result has none, the field is omitted.

gRPC needs no such field: each array value carries its own element type.

### Parameters of type `id`

A parameter of type `id` must hold an object id: 24 hex characters. The server converts upper-case hex
to lower case. Any other text fails with `CADB0400` (invalid input), and the message names the
parameter. The usual cause is a client that sends a GUID as an `id` parameter. A GUID has 16 bytes and
an object id has 12, so a GUID can never be an object id: send it as a `uuid` parameter.

### Comparing values of different types

CamusDB follows GoogleSQL: two values can be compared only when their types have a comparison rule.
These pairs have a rule:

- Two values of the same type.
- Two numbers of different types: `int64` against `numeric` compares exactly, and a float compares
  with any other number as a double.
- A `date` against a `datetime`, or a `string` against either. The string is parsed. A string that
  does not parse fails with `CADB0400` (see
  [Comparing dates, datetimes and strings](#comparing-dates-datetimes-and-strings)).
- A `string` against a `uuid`, an `id` or a `jsonb`. The string is parsed. A string that does not
  parse equals no value of that type. It is not an error.
- Two arrays with the same element type, or two arrays where one is `ARRAY[]` or holds only `NULL`
  elements. The elements compare with the rule for one type, so the element types must be the same:
  `ARRAY[1] = ARRAY['a']` and `ARRAY[1] = ARRAY[1.0]` fail with `CADB0400`. The message names both
  element types: `… argument types: ARRAY<Integer64>, ARRAY<String>`.

Every other pair has no rule. Examples are `int64` against `string` (`5 = '5'`), `float64` against
`string` (`0.0 = '0.0'`), `bool` against `int64`, `bytes` against `string`, `uuid` against `id`, and
`numeric` against `string`. **Any comparison of such a pair fails with `CADB0400`**, and the message
names the operator and both types:

```
No matching signature for operator = for argument types: Integer64, String
```

This applies to `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN` and `NOT IN`, the simple `CASE`,
`NULLIF`, `= ANY (…)`, `array_contains`, `JOIN … ON` and `HAVING`. It applies to a column, a literal
and a typed parameter alike. A CHECK constraint that compares such a pair rejects the row at write time
with `CADB0303` instead (see [CHECK constraints](check-constraints.md)). To compare the values, write a CAST:
`CAST(s AS INT64) = a`.

The error is deliberate. Before, such an equality was silently FALSE, so a statement such as
`DELETE FROM t WHERE text_col = int_col` reported success and deleted nothing.

Notes:

- `x IN (a, b)` means `x = a OR x = b`, and `OR` evaluates both sides. So an item with no rule fails
  the statement even when another item matches: `1 IN (1, 'a')` fails.
- A simple `CASE` stops at the first `WHEN` that matches. A later `WHEN` is not compared, so
  `CASE 5 WHEN 5 THEN 1 WHEN 'a' THEN 2 END` returns 1.
- A NULL operand makes the comparison UNKNOWN before any type check, so `NULL = 'a'` is NULL, not an
  error.
- The check runs when values are compared, not when the statement is parsed. A predicate over an
  empty table compares no values and raises no error.
- An index is never used to answer a comparison that has no rule. The filter evaluates it and raises
  the error, so an index seek and a table scan give the same result.
- A `UNION`, `INTERSECT` or `EXCEPT` refuses two array columns whose element types are known and are
  not the same, as it refuses two scalar types that cannot be combined. When an array expression's
  element type is known only from its value, this check does not apply.

### Result types of CASE, COALESCE, IFNULL and IF

A `CASE`, `COALESCE`, `IFNULL` (and its alias `NVL`) and `IF` return one of several inputs: the
`THEN` and `ELSE` results of a `CASE`, the arguments of `COALESCE` and `IFNULL`, and the two result
arguments of `IF`. CamusDB follows GoogleSQL: the inputs must have a common type, and every value is
converted to it. Two types combine like this:

| Inputs | Result type |
|---|---|
| A `NULL` input, or an omitted `ELSE` | the type of the other inputs |
| One type | that type |
| Two numbers | the type that `+` gives for them: `int64` with `float64` gives `float64`, `int64` with `numeric` gives `numeric` |
| A `date` with a `datetime` | `datetime`; the date becomes its midnight |
| A quoted string literal with a `date` or a `datetime` | that temporal type; the literal is parsed |
| Two arrays | an array, when the element types are the same or one array is `ARRAY[]` |
| Any other pair | error `CADB0400` |

The error names the construct and both types:

```
No common type for the results of CASE: Integer64 and String; write a CAST to make the types agree
```

Examples:

- `CASE WHEN c THEN 1 ELSE 1.5 END` is `float64` in every row, also in a row that returns `1`.
- `COALESCE(int_col, float_col)` is `float64`, also in a row where `float_col` is `NULL`.
- `CASE WHEN c THEN 1 ELSE 'a' END` and `COALESCE(1, 'a')` fail. `IFNULL(true, 1)` fails.
- `CASE WHEN c THEN 1 ELSE '2' END` fails. A quoted literal is a `string`, so it does not become a
  number. PostgreSQL reads `'2'` as an integer here; GoogleSQL does not. Write `CAST('2' AS INT64)`.
- `COALESCE(date_col, '2026-01-01')` is a `date`: a quoted literal does become a `date` or a
  `datetime`, as in GoogleSQL. The literal is parsed when the statement is bound, so
  `COALESCE(date_col, 'soon')` fails with `CADB0400` before any row is read.

**Only a literal becomes a date.** A `string` column with a `date` column fails:
`CASE WHEN c THEN date_col ELSE text_col END` is an error. A `CASE`, `COALESCE`, `IFNULL` or `IF`
whose inputs are all literals is not a literal itself, so
`COALESCE(date_col, CASE WHEN c THEN '2026-01-01' ELSE '2026-01-02' END)` fails, as in GoogleSQL. A
`string` value whose type only its value shows, such as a parameter, is parsed when it is returned:
a `string` parameter that holds `'2026-01-01'` becomes that date, and one that holds other text fails.

**When the check runs.** The check runs when the statement is bound, before any row is read, for
every input whose type the statement shows: a literal, a `CAST`, a column, arithmetic over those, and
a nested `CASE`, `COALESCE`, `IFNULL` or `IF`. So a branch that never runs, and a statement over an
empty table, still fail. The check covers the select list, `WHERE`, `GROUP BY`, `HAVING`,
`ORDER BY`, `JOIN … ON`, and the `SET` values and `WHERE` of an `UPDATE` and a `DELETE`.

An input whose type only its value shows, such as a parameter or a function call, is checked when
the value is returned. A branch that does not run is not checked then:
`CASE WHEN false THEN @p ELSE 1 END` with a `string` `@p` returns `1`.

**Evaluation order does not change.** A `CASE` stops at the first `WHEN` that matches, and an `IF`
evaluates only the result that its condition selects. `COALESCE` and `IFNULL` evaluate every
argument.

#### IF

`IF(condition, true_result, else_result)` returns `true_result` when `condition` is `TRUE`, and
`else_result` when it is `FALSE` or `NULL`. It is the same as
`CASE WHEN condition THEN true_result ELSE else_result END`:

- A `condition` that is not a boolean fails with `CADB0400`.
- Only the selected result is evaluated, so `IF(true, 1, 1 / 0)` returns `1`.
- The two results follow the rule above.

`IF` is a reserved word, so `IF NOT EXISTS` and `IF EXISTS` in DDL do not change. `EXPLAIN` shows
`IF(…)`.

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
| `NOW()` / `CURRENT_TIMESTAMP()` / `TRANSACTION_TIMESTAMP()` | `datetime` | Start of the transaction, UTC. See [Current time](#current-time) |
| `STATEMENT_TIMESTAMP()` | `datetime` | Start of the statement, UTC |
| `CLOCK_TIMESTAMP()` / `SYSDATE()` | `datetime` | The clock at each call, UTC |
| `CURRENT_DATE()` | `date` | UTC date of the start of the transaction |
| `DATE_ADD(temporal, n, unit)` | `datetime` | See promotion rule below |
| `DATE_TRUNC(unit, temporal)` | `datetime` | Always yields datetime (Postgres-compatible) |
| `FROM_UNIXTIME(seconds)` | `datetime` | Unix epoch seconds → UTC datetime. A fraction rounds to the nearest microsecond |
| `DATE_DIFF(a, b, unit)` | `int64` | Difference in `unit` units (unchanged) |
| `DATE_PART(unit, temporal)` | `int64` | Extracts calendar component (unchanged) |
| `EXTRACT(unit FROM temporal)` | `int64` | SQL-standard form of `DATE_PART(unit, temporal)` |
| `UNIX_TIMESTAMP([temporal])` | `int64` | UTC seconds since epoch. With no argument, of the start of the transaction |
| `UNIX_TIMESTAMP_MS([temporal])` | `int64` | UTC milliseconds since epoch, rounded down. With no argument, of the start of the transaction |
| `MAKE_DATE(year, month, day)` | `date` | Integer parts. See [Date constructors](#date-constructors) |
| `MAKE_TIMESTAMP(year, month, day, hour, minute, second)` | `datetime` | UTC. `second` can be fractional |
| `MAKEDATE(year, dayofyear)` | `date` | MySQL. Day 1 is January 1 |
| `LAST_DAY(temporal)` | `date` | MySQL. The last day of the UTC month of the argument |
| `YEAR`, `MONTH`, `DAY` / `DAYOFMONTH`, `HOUR`, `MINUTE`, `SECOND`, `QUARTER`, `DAYOFYEAR` `(temporal)` | `int64` | MySQL shortcuts: the same value as `DATE_PART` with that unit |
| `DAYOFWEEK(temporal)` | `int64` | MySQL rule: 1 = Sunday ... 7 = Saturday |
| `WEEKDAY(temporal)` | `int64` | MySQL rule: 0 = Monday ... 6 = Sunday |
| `WEEK(temporal [, mode])` | `int64` | Mode 0 (default): weeks start on Sunday, 0-53, the days before the first Sunday are week 0. Mode 3: ISO-8601 week, 1-53. Other modes are rejected |
| `DAYNAME(temporal)`, `MONTHNAME(temporal)` | `string` | English names (`Sunday`, `February`), independent of server locale |
| `TO_CHAR(temporal, pattern)`, `DATE_FORMAT(temporal, pattern)` | `string` | Format with a PostgreSQL template or MySQL `%` specifiers; see [date-patterns.md](date-patterns.md) |
| `TO_TIMESTAMP(text, pattern)`, `STR_TO_DATE(text, pattern)` | `datetime` | Strict parse with a pattern; see [date-patterns.md](date-patterns.md) |
| `TO_DATE(text, pattern)` | `date` | Strict parse with a PostgreSQL template, date only. `TO_DATE(x)` with one argument is still the cast to `date` |
| `TO_TIMESTAMP(seconds)` | `datetime` | The PostgreSQL spelling of `FROM_UNIXTIME(seconds)` |

### Date constructors

`MAKE_DATE`, `MAKE_TIMESTAMP`, `MAKEDATE` and `LAST_DAY` are deterministic. A `CHECK` can call them, and
a `DEFAULT` that calls them is evaluated once, when the table is created. A NULL argument gives NULL.

An invalid part raises `InvalidInput` (`CADB0400`), and the message names the part:

| Call | Result |
|------|--------|
| `MAKE_DATE(2024, 2, 29)` | `2024-02-29` |
| `MAKE_DATE(2023, 2, 29)` | Error: `day 29 is out of range 1-28 for 2023-02` |
| `MAKE_DATE(2026, 13, 1)` | Error: `month 13 is out of range 1-12` |
| `MAKE_TIMESTAMP(2026, 1, 1, 0, 0, 0.123)` | `2026-01-01T00:00:00.1230000Z` |
| `MAKE_TIMESTAMP(2026, 12, 31, 23, 59, 59.9999999)` | `2027-01-01T00:00:00.0000000Z` |
| `MAKEDATE(2024, 60)` | `2024-02-29` |
| `MAKEDATE(2011, 366)` | `2012-01-01` |
| `LAST_DAY('2024-02-10')` | `2024-02-29` |

The rules:

- The year must be 1-9999. PostgreSQL reads a negative year as a year BC. CamusDB has no BC dates, so it rejects it.
- `MAKE_TIMESTAMP` accepts an integer, float or NUMERIC second that is at least 0 and less than 60. The
  fraction rounds to the nearest microsecond, as in PostgreSQL. A second that rounds up to 60 moves into
  the next minute.
- `MAKEDATE` follows MySQL when the day of the year is past the end of the year: the date moves into the
  next year. MySQL returns NULL for a day of the year below 1, and CamusDB raises an error. MySQL reads
  a two-digit year as 1970-2069, and CamusDB reads every year as written.
- `LAST_DAY` reads the month of the UTC value, so an offset can move the value into another month first.

`UNIX_TIMESTAMP_MS` gives the millisecond epoch that JavaScript uses (`Date.now()`). Neither PostgreSQL
nor MySQL has this function. The name adds the unit to `UNIX_TIMESTAMP` and keeps its arity and clock
rule. `FROM_UNIXTIME(UNIX_TIMESTAMP_MS(t) / 1000.0)` equals `DATE_TRUNC('millisecond', t)` up to about
the year 2255. After that year, a float cannot hold each microsecond. Use a NUMERIC divisor for later
dates: `FROM_UNIXTIME(UNIX_TIMESTAMP_MS(t) / NUMERIC '1000')`.

### Current time

Each function that reports the current time reads one of three clocks:

| Function | Value | Same value for |
|----------|-------|----------------|
| `NOW()`, `CURRENT_TIMESTAMP()`, `TRANSACTION_TIMESTAMP()`, `CURRENT_DATE()`, `UNIX_TIMESTAMP()`, `UNIX_TIMESTAMP_MS()` | Start of the transaction | Every row and every statement of the transaction |
| `STATEMENT_TIMESTAMP()` | Start of the statement | Every row of the statement |
| `CLOCK_TIMESTAMP()`, `SYSDATE()` | The clock when the call runs | Nothing. Each call reads the clock again |

So `SELECT NOW() = NOW()` is true, `INSERT INTO t SELECT id, NOW() FROM s` writes one time into every
row, and two statements in one `BEGIN ... COMMIT` see the same `NOW()`. A statement outside an
explicit transaction runs in its own transaction, so its `NOW()` is the start of that statement.
`STATEMENT_TIMESTAMP()` is never earlier than `NOW()`.

**Behavior change.** Before this change, `NOW()`, `CURRENT_TIMESTAMP()`, `CURRENT_DATE()` and
`UNIX_TIMESTAMP()` read the clock on each call, so one statement could write a different time into
each row. They now report the start of the transaction, as in PostgreSQL. To read the clock on each
call, use `CLOCK_TIMESTAMP()`.

**Comparison with PostgreSQL and MySQL.** CamusDB follows PostgreSQL. MySQL fixes `NOW()` at the start
of the statement, not of the transaction. CamusDB needs the parentheses: write `CURRENT_TIMESTAMP()`,
not `CURRENT_TIMESTAMP`.

| CamusDB | PostgreSQL | MySQL |
|---------|------------|-------|
| `NOW()`, `CURRENT_TIMESTAMP()`, `TRANSACTION_TIMESTAMP()` | The same: start of the transaction | `NOW()` is the start of the statement. There is no transaction-start function |
| `STATEMENT_TIMESTAMP()` | The same | `NOW()` |
| `CLOCK_TIMESTAMP()`, `SYSDATE()` | `clock_timestamp()` | `SYSDATE()` |
| `CURRENT_DATE()` | The same: date of the start of the transaction | `CURDATE()`: date of the start of the statement |
| `UNIX_TIMESTAMP()` | `extract(epoch FROM now())`, which keeps the fraction | `UNIX_TIMESTAMP()`: start of the statement |

**Precision and clocks.** The value has the full precision of the server clock (100 ns ticks). The
start of a transaction is the later of the wall clock and the hybrid logical clock (HLC) of the
transaction. The HLC is the clock the cluster uses to order events. Its physical part is in
milliseconds and is never earlier than a clock the node heard from. So when a transaction follows
another one on a different node, its `NOW()` is not earlier, to the millisecond, even when the clocks
of the two nodes differ. When the clock of a node is behind the HLC, `NOW()` can be later than
`CLOCK_TIMESTAMP()` on that node.

**Column defaults.** A `DEFAULT NOW()` gives every row of one `INSERT` the same time, which also
equals an explicit `NOW()` in that `INSERT`. A `DEFAULT STATEMENT_TIMESTAMP()` gives every row the
start of its statement. `ALTER TABLE ... ADD COLUMN ... DEFAULT NOW()` gives every existing row one
time: the time the fill of the column started. The fill commits in batches, so it cannot use the time
of one batch. A fill that a new leader resumes uses its own start time for the rows that remain.

**Other rules:**

- A retried transaction is a new transaction, so it reports a new `NOW()`, as in PostgreSQL.
- A query that calls any of these functions does not use the shared query result cache.
- `WHERE col > NOW()` can use an index on `col`, also on a table of a join, as a literal bound
  can. The index range and the row filter read the same time, the start of the transaction.
- In a distributed query, the node that runs the statement evaluates every call. A call is never
  sent to another node.
- With `AS OF SYSTEM TIME`, `NOW()` is still the start of the transaction, not the time of the
  snapshot.

### Date units

Every unit is computed on the UTC value. The unit is a string (`'week'`), or a bare word in
`EXTRACT(week FROM x)`. A unit that has no meaning in a function is rejected with the same
`InvalidInput` error as an unknown unit.

| Unit (aliases) | `DATE_PART` / `EXTRACT` | `DATE_TRUNC` | `DATE_ADD` / `DATE_DIFF` |
|----------------|-------------------------|--------------|--------------------------|
| `year` (`years`) | calendar year | January 1 | years |
| `quarter` (`quarters`) | 1-4 | first day of the quarter | 3 months |
| `month` (`months`) | 1-12 | first day of the month | months |
| `week` (`weeks`) | ISO-8601 week, 1-53 | Monday 00:00 | 7 days |
| `day` (`days`) | 1-31 | 00:00 | days |
| `hour`, `minute`, `second` (plurals) | 0-23, 0-59, 0-59 | start of the unit | the unit |
| `millisecond` (`milliseconds`) | fraction of the second, 0-999 | yes | yes |
| `microsecond` (`microseconds`, `us`) | fraction of the second, 0-999999 | yes | yes |
| `dow` | 0 = Sunday ... 6 = Saturday | no | no |
| `isodow` | 1 = Monday ... 7 = Sunday | no | no |
| `doy` | day of the year, 1-366 | no | no |
| `isoyear` | ISO-8601 year that owns the ISO week | no | no |
| `epoch` | whole seconds since 1970-01-01 UTC | no | no |
| `decade` (`decades`) | year / 10 | January 1 of a year that ends in 0 | 10 years |
| `century` (`centuries`) | 2000 is century 20, 2001 is century 21 | January 1 of a year that ends in 01 | 100 years |
| `millennium` (`millennia`, `millenniums`) | 2000 is millennium 2, 2001 is millennium 3 | January 1 of a year that ends in 001 | 1000 years |

The ISO week and the ISO year can differ from the calendar: 2021-01-01 is in week 53 of ISO year 2020,
and 2024-12-30 is in week 1 of ISO year 2025.

**Differences from PostgreSQL.** A part is always `int64`, because the result type cannot depend on
the unit literal. PostgreSQL returns `numeric` and keeps fractions, so two units differ:

- `epoch` is whole seconds, rounded down. Half a second before 1970-01-01 gives `-1`.
- `millisecond` and `microsecond` are the fraction of the current second only, as MySQL's
  `MICROSECOND()`. For `12.345` seconds CamusDB returns `345` milliseconds; PostgreSQL returns `12345`.

`DATE_DIFF` truncates toward zero. `day` and `week` count calendar days and ignore the time of day;
`month`, `quarter` and the year-based units count complete anniversaries.

`DATE(x)` is not a function, because `date` is a reserved type keyword. Use `CAST(x AS date)`.

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

-- Weekly report: weeks start on Monday
SELECT DATE_TRUNC('week', created) AS week, count(*) FROM events GROUP BY DATE_TRUNC('week', created)

-- SQL-standard and MySQL spellings of one part
SELECT EXTRACT(dow FROM created), DAYOFWEEK(created) FROM events
```

---

## Reserved keywords

The type keywords (and their aliases) are **reserved words** and cannot be used as column or table names
in SQL. The reserved type words are:

```
oid  object_id  smallint  int  int64  integer  string  bool  boolean  float32  float64  real
date  datetime  timestamp  bytes  blob  array  numeric  decimal
```

Note that `id` is **not** a SQL type keyword and remains usable as an identifier (column/table name) —
the ObjectId type is spelled `oid`/`object_id` in SQL. `bigint` and `precision` are also usable as names.

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

A float key encodes the value in the SQL order: `-0` has the key of `0`, and every NaN has one key
above `Infinity`. An index seek and a table scan therefore return the same rows for every float
predicate (see [Float values](#order-and-equality)).

---

## Backward compatibility

Rows are self-describing per schema version: each stored row records the schema version it was written
under and decodes against that version's column layout. Adding a column of a new type bumps the schema
version; existing rows keep decoding against their original layout with no migration. This makes adding
new-typed columns to an existing table a safe, online operation.
