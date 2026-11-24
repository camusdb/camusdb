# Data Types

This document describes the column types CamusDB supports and how to declare them in `CREATE TABLE`.
It also covers the literal and value formats of the SQL and HTTP paths. The last sections give the
rules for length bounds, indexing and reserved keywords.

> Status: the type system is **alpha**. The on-disk format is backward compatible (see
> [Backward compatibility](#backward-compatibility)), but new types may gain capabilities (e.g. wider
> coercion or array indexing) in future releases.

---

## Type reference

| Type | Stores | Indexable | Notes |
|------|--------|-----------|-------|
| `oid` | 12-byte ObjectId | yes | CamusDB's native identifier type; shares its key encoding with `string`. Spelled `oid`/`object_id` in SQL, `id` over HTTP (see [aliases](#aliases)). |
| `int64` | 64-bit signed integer | yes | Range `-9223372036854775808` to `9223372036854775807`. A result outside it is an error. The value does not wrap. See [INT64 overflow](#int64-overflow). |
| `smallint` | `int64` with a 16-bit range | yes | Range `-32768` to `32767`. A value outside it is an error on a write and on `CAST`. See [Integer type names](#integer-type-names). |
| `int`, `integer` | `int64` with a 32-bit range | yes | Range `-2147483648` to `2147483647`. A value outside it is an error on a write and on `CAST`. See [Integer type names](#integer-type-names). |
| `float64` | IEEE-754 double | yes | `'Infinity'`, `'-Infinity'` and `'NaN'` are values. An overflow or an underflow is an error. See [Float values](#float-values-infinity-nan-and-overflow). |
| `float32` | IEEE-754 single | yes | Stored at single precision; comparisons and storage narrow to `float`. A value past the `float32` range is an error. It does not become infinity. |
| `numeric` | exact decimal: 38 digits, 9 after the point | yes | Fixed precision and scale, as Spanner's NUMERIC. See [NUMERIC](#numeric). |
| `bool` | boolean | yes | |
| `string` | Unicode text | yes | Length-bounded — see [String and bytes length](#string-and-bytes-length-bounds). |
| `string(N)` | Unicode text, max `N` characters | yes | `N` is a positive integer count of code points. |
| `date` | calendar date (no time) | yes | Stored as UTC ticks truncated to midnight. |
| `datetime` | instant in time (UTC) | yes | Stored as UTC ticks. A `date` value written to a `datetime` column (INSERT, UPDATE, DEFAULT) becomes midnight UTC of that day. A `datetime` value written to a `date` column is an error. The write would lose the time of day. |
| `bytes` | opaque byte string | yes | Length-bounded (default 10 MB). |
| `array(T)` | ordered list of `T` | **no** | `T` is a scalar type; see [Arrays](#arrays). |
| `jsonb` | JSON document, binary (canonical CBOR) | GIN only | PostgreSQL operators and functions. No ordered index, primary key, `UNIQUE` or foreign key. See [json.md](json.md). |
| `interval` | months, days and microseconds | not stored | A value of an expression only: no table column can have this type. A client receives it as a string. See [Intervals](#intervals). |

### Aliases

Each alias in this table is a synonym for its canonical type.

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

You can use an alias in any place where a type is written, such as a column definition,
`CAST(x AS bigint)` or an array element type like `array(bigint)`. The spelling is not stored: `SHOW CREATE TABLE`
renders the canonical type. You can also use `bigint` and the word `precision` as table and column
names.

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

(Here the column is *named* `id`, which is an ordinary identifier, and its *type* is `oid`.)

---

## Integer type names

Each integer type name promises a range, and the engine enforces it. These are the PostgreSQL ranges.

| Name | Range | `SHOW CREATE TABLE` and `SHOW COLUMNS` |
|------|-------|----------------------------------------|
| `smallint` | `-32768` to `32767` | `SMALLINT` |
| `int`, `integer` | `-2147483648` to `2147483647` | `INT` |
| `int64`, `bigint` | `-9223372036854775808` to `9223372036854775807` | `INT64` |

A value outside the range returns `CADB0420` (`IntegerValueOutOfRange`): HTTP 400, gRPC
`OUT_OF_RANGE`. The statement does not store any change. The range is checked in the following places.

- DML statements: `INSERT`, `INSERT … SELECT`, `UPDATE` and both arms of `ON CONFLICT`.
- The HTTP and gRPC row endpoints. This includes a value that a sequence default or a trigger gives.
- A constant `DEFAULT`, when the statement that declares it is executed.
- `ALTER TABLE … ADD COLUMN`. A sequence or function default that passes the range while it fills the
  existing rows stops the fill, and the column is removed.
- Each element of an array column, such as `array(smallint)`.
- `CAST(x AS smallint)`, `x::int` and the other narrow spellings. The result is an `int64` value. A float
  is rounded first. This means that `CAST(32767.5 AS smallint)` fails.

The range is a write rule. All integer names are stored as `int64`, with these effects.

- Arithmetic is `int64`. If `s` is a `smallint` with the value `32767`, then `SELECT s + 1` gives `32768`.
  `UPDATE … SET s = s + 1` fails. It writes that result to the column.
- A query result and a wire value are `int64`, and a comparison reads an `int64`. `WHERE s = 40000`
  finds no row, and it is not an error.
- An `int64` column and a `smallint` column have the same row bytes and index keys. No storage
  revision is involved.

The rule has some limits, and some differences from PostgreSQL.

- **A column created before this rule has no range limit.** The catalog did not record the name. An
  old column declared `int` or `smallint` can store any `int64`, and it renders as `INT64`.
- A stored `CHECK` or view body from before this rule is not limited either. The engine stored its
  `CAST(x AS int)` as `CAST(x AS int64)`.
- `serial` and `bigserial` are `int64`. PostgreSQL makes `serial` 32 bits.
- A sequence on a narrow column uses the bounds of the sequence. PostgreSQL limits the sequence to the
  column type. Here the sequence goes on, and the write of a value past the range returns `CADB0420`.
- `CREATE TABLE … AS SELECT` makes `int64` columns, whatever the source column declares. PostgreSQL uses
  the type of the source column.

---

## INT64 overflow

An `int64` value does not wrap. A result outside the range `-9223372036854775808` to
`9223372036854775807` returns `CADB0420` (`IntegerValueOutOfRange`): HTTP 400, gRPC
`OUT_OF_RANGE`. In PostgreSQL, the same condition is the error `bigint out of range`. A statement that
overflows does not store any change: the `UPDATE`, `INSERT` or `DELETE` fails, and in an explicit
transaction the failed statement aborts the transaction.

The overflow check covers the following operations.

- `+`, `-`, `*` and `/` with two `int64` operands, and unary minus. `-9223372036854775808 / -1` and
  `-(-9223372036854775807 - 1)` fail.
- `SUM` of `int64` values, as an ungrouped, grouped or window aggregate. Only the final total is
  checked. This means that a running total that passes the range and comes back gives the exact result,
  whatever the order of the rows. `AVG` divides the exact total and does not overflow.
- An integer literal past the range, such as `9223372036854775808`. Write a wider number as
  `NUMERIC '…'`.
- A conversion to `int64`: `CAST` of a `float64`, a `numeric`, a `string` or a `jsonb` number, and
  `abs(-9223372036854775808)`. A string that is not an integer, such as `'12abc'`, returns
  `CADB0400`.

The following operations do not return `CADB0420`.

- `%` (`mod`): `-9223372036854775808 % -1` is `0`, as in PostgreSQL.
- The shift operators `<<` and `>>` work on bits. They use the shift count modulo 64 (see
  [SQL expression syntax](sql-expression-syntax.md)).
- A sequence does not wrap either. `nextval` past `MAXVALUE`, which is at most `9223372036854775807`,
  returns `CADB0544`.

An overflow in a `WHERE` clause is an error too. This means that `WHERE v + 1 > 0` cannot drop or keep a
row by a wrapped comparison. In a cluster, a `SUM` or `AVG` of an `int64` column that is split into a partial
aggregate for each range adds each range exactly, and checks only the final total.

---

## Float values: Infinity, NaN and overflow

`float64` and `float32` follow the PostgreSQL rule for `float8` and `float4`.

### Infinity and NaN are values

`'Infinity'`, `'-Infinity'` and `'NaN'` are legal values. You can write them as text, which converts to a
float column as a PostgreSQL string literal does. You can also use `CAST` or a parameter.

```sql
INSERT INTO readings (id, v) VALUES (1, 'Infinity'), (2, '-Infinity'), (3, 'NaN');
SELECT CAST('NaN' AS float64);
SELECT * FROM readings WHERE v = CAST('Infinity' AS float64);
```

- The text form is case-insensitive. The allowed spellings are `NaN`, `Infinity`, `+Infinity`, `-Infinity`, `inf`,
  `+inf` and `-inf`, with white space around it. Other spellings, such as `∞`, fail with `CADB0400`.
- A row with one of these values works like any other row. For example, you can sort, index or update
  it, and the value survives a restart.
- An operation on an infinite or NaN value gives the IEEE result: `'Infinity' + 1` is `Infinity`,
  `'Infinity' - 'Infinity'` is `NaN`, and `1 / 'Infinity'` is `0`.
- A `SUM` or `AVG` over values that include an infinity or a NaN gives the IEEE total.
- `CAST` to `int64` of an infinity or a NaN returns `CADB0420`, as PostgreSQL returns
  `bigint out of range`. `CAST` to `numeric` returns `CADB0400`. A CamusDB `numeric` cannot represent
  an infinity or a NaN.
- `CAST(x AS string)` gives `Infinity`, `-Infinity`, `NaN` and `-0`.
- `to_jsonb` of an infinity or a NaN gives the JSON string `"Infinity"`, `"-Infinity"` or `"NaN"`,
  as in PostgreSQL. JSON does not define a number for these values.

### Order and equality

- The order is `-Infinity` < any number < `Infinity` < `NaN`. `NaN` sorts above all other values.
  This means that `ORDER BY v DESC` puts it first, and `MAX(v)` is `NaN` when a row contains it.
- `NaN = NaN` is true. `WHERE v = CAST('NaN' AS float64)` finds the NaN rows, and `GROUP BY` and
  `DISTINCT` give one group for them. `NaN > 1` is true.
- `-0` equals `0`. A UNIQUE index does not allow `-0` next to `0`, and `WHERE v = 0` finds both rows. A stored
  `-0` reads back as `-0`, except where a query reads the value from an index key (a covering index
  read), which gives `0`.
- This order differs from IEEE comparison, where all comparisons with NaN are false. It is the order of
  PostgreSQL.

### Overflow and underflow are errors

A result from finite values that a float cannot represent returns `CADB0421`
(`FloatValueOutOfRange`): HTTP 400, gRPC `OUT_OF_RANGE`. The result does not become an infinity or a zero.

- An *overflow* is a result too large for the type: `1e308 * 10`, `pow(10, 400)`, `exp(1000)`, `degrees(1e308)`, the literal `1e400`,
  `CAST('1e400' AS float64)`, or a `SUM` or `AVG` total past the range.
- An *underflow* is a nonzero result too small for the type: `1e-300 * 1e-300`, `pow(10, -400)`, `exp(-1000)`, `radians(5e-324)`, or
  the literal `1E-400`. A literal or a text with a zero mantissa, such as `0e-400`, is zero.
- A subnormal result is a valid value: `4e-320` is stored as it is.
- A `float64` value written to a `float32` column, or cast to `float32`, fails when it is past the
  `float32` range (`1e300`) or rounds to zero (`1e-300`). Arithmetic on `float32` values checks the
  `float32` range too.
- A statement that overflows does not store any change, as with an `int64` overflow (see
  [INT64 overflow](#int64-overflow)).
- A `SUM` that is not a window function adds in row order and fails when the running total passes the range. A window `SUM`
  adds the frame exactly and checks only its total.

The following operations have no real result and return `CADB0400`: `x / 0` for any `x`, `pow(0, y)` for
a negative `y`, `pow(x, y)` for a negative `x` and a finite non-integer `y`, `sqrt` of a negative
number, `ln`, `log` and `log10` of zero or of a negative number, `log(1, x)`, `sign(NaN)`, a sine, cosine,
tangent or cotangent of an infinite angle (`sin`, `cos`, `tan`, `cot` and the degree forms `sind`, `cosd`,
`tand`, `cotd`), and an `asin`, `acos`, `asind` or `acosd` argument outside [-1, 1]. `round(x, n)` and `trunc(x, n)` allow any scale, and they do not turn a finite
value into a NaN or an infinity.
`cot(0)`, `tand(90)` and `cotd(0)` return an infinity and do not fail, as in PostgreSQL. The true
value there is infinite. PostgreSQL gives the error code `22003` for an infinite angle and for an
`asin` argument outside [-1, 1]; CamusDB gives `CADB0400`, as it does for the other domain errors.

### Over the wire

- **HTTP JSON** does not define a number for an infinity or a NaN. A `float64` or `float32` cell
  writes them as the strings `"Infinity"`, `"-Infinity"` and `"NaN"`. The column's `type` is the float
  type. This means that a client reads a string in a float column as that value. All finite values,
  `-0` included, are JSON numbers. A float parameter can use the same strings:
  `{"type": 5, "floatValue": "NaN"}`.
- **gRPC** carries the IEEE value in the `double` or `float` field, unconverted.

### Indexes that hold -0 or NaN from before

An index key now encodes `-0` as `0`, and all NaNs as one key above `Infinity`. Before, `-0` had a
separate key below `0`, and a NaN with the sign bit set had a key below `-Infinity`. An index built
before this rule on a float column that contains `-0` or a NaN has keys of the old form. Rebuild such an
index with `DROP INDEX` and `CREATE INDEX`. An index on a column that contains neither value does not
need a rebuild.

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
- A decimal literal written into a NUMERIC column converts from its source text. For example,
  `INSERT INTO t (p) VALUES (12345678901234567890.123456789)` stores all the digits.
- An integer literal must fit `int64`. Write a wider whole number as `NUMERIC '…'` or with a decimal
  point.
- An `int64`, `float64`, `float32` or `string` value converts when it is written into a NUMERIC column.
  A computed `float64` converts from its exact binary value. For example, `123456789.123` computed as
  a double stores `123456789.122999996`.
- `CAST(x AS numeric)` works on `int64`, `float64`, `float32` and `string` values. `CAST(p AS int64)` rounds half
  away from zero, and fails when the result does not fit. `CAST(p AS string)` gives the canonical text.

### Expressions and result types

| Operation | Result |
|-----------|--------|
| `numeric` with `numeric` or `int64` (`+ - * /`, `%`) | `numeric`, exact; `*` and `/` round to 9 digits |
| `numeric` with `float64` or `float32` | `float64` |
| A bare decimal literal in an expression (`p + 0.1`) | `float64`, since the literal is a float; write `p + NUMERIC '0.1'` |
| `SUM(numeric)` | `numeric`, exact; only the final total is range-checked |
| `AVG(numeric)` | `numeric`, the exact total divided by the count, rounded half away from zero |
| `MIN`, `MAX`, `abs`, `ceil`, `floor`, `round(x[, n])`, `trunc(x[, n])`, `mod`, `sign` | `numeric` |
| `sqrt`, `pow` and `^`, `ln`, `log`, `log10`, `exp`, the trigonometric functions, `radians`, `degrees` | `float64` (Spanner returns NUMERIC for `sqrt`, `pow`, `ln`, `log` and `log10`) |
| `COALESCE`, `CASE` with `numeric` and `int64` branches | `numeric`; with a `float64` or `float32` branch, `float64` |

Division by zero fails. An overflow anywhere returns `CADB0417`.

- `round(x, n)` and `trunc(x, n)` with `n` far left of the point (down to the minimum `int64`) give `0`.
- `round(i, n)` with an `int64` `i` is `numeric` for any `n`, as in PostgreSQL. For example,
  `round(15, -1)` is exactly `20` and `round(5, 0)` is the `numeric` `5`. `round(i)` and `trunc(i, n)`
  return `int64`. The truncation of an integer is an integer.
- In a cluster, a `SUM` or `AVG` that can see a NUMERIC value is not split into a partial aggregate for
  each range. The node that executes the query adds the rows exactly. This means that a range total past
  the limit does not fail a query whose final result is in range.
- `COALESCE(p, 0)` gives NUMERIC `0` for a NULL `p`, at the top level and in a derived table: the
  result type of a `CASE`, `COALESCE`, `IFNULL` or `IF` comes from the declared column types, and
  each value is converted to it (see
  [Result types of CASE, COALESCE, IFNULL and IF](#result-types-of-case-coalesce-ifnull-and-if)).

### Comparing NUMERIC values

- `numeric` against `numeric` or `int64` compares exactly: `9007199254740993` does not equal
  `9007199254740992`, although the two are one double.
- `numeric` against `float64` or `float32` compares as doubles. This rule is not transitive:
  `NUMERIC '9007199254740992'` and `NUMERIC '9007199254740993'` differ, but both equal
  `9007199254740992.0`. An `IN` list does not drop any item. This means that `x IN (a, b, …)` is always
  `x = a OR x = b OR …`.
- No comparison rule exists for `numeric` against `string`. `p = '1.5'` returns `CADB0400` (see
  [Comparing values of different types](#comparing-values-of-different-types)). Write the value as
  `NUMERIC '1.5'`.
- An index on a NUMERIC column is used for an `int64` or `NUMERIC '…'` constant. A float constant
  (`p < 9.99`) does not use the index. Many NUMERIC values widen to one double. Write
  `p < NUMERIC '9.99'` to use the index.

### On the wire

A NUMERIC value is sent as text on all transports. This means that a client does not round it through
a double.

- HTTP and JSON: a JSON string, `"12345678901234567890.123456789"`. A parameter is
  `{"type": 12, "strValue": "…"}`.
- gRPC: the `numeric_value` field, column type `COLUMN_TYPE_NUMERIC`. See
  [grpc-client-protocol.md](grpc-client-protocol.md).

---

## String and bytes length bounds

A `string(N)` column can store at most `N` characters. A bare `string` column (no size) defaults to a
maximum of **2 621 440 characters**. A `bytes` column defaults to a maximum of **10 MB**
(10 485 760 bytes).

- **String length is measured in code points**, as in PostgreSQL. An emoji such as `🙂` counts as **1**
  toward the limit. See [Characters and code points](#characters-and-code-points).
- **Bytes length is measured in bytes.**
- The bound does not apply to a `NULL` value.

**An over-length value is an error, and it is not truncated.** An `INSERT` or `UPDATE` whose String/Bytes
value exceeds the column's bound returns error code **`CADB0302` (`ValueTooLong`)**. The message names the
column and the limit, and gives the actual length.

```
value too long for column 'name' (max 64, got 71)
```

This mirrors PostgreSQL's default behavior (`SQLSTATE 22001`). Unlike PostgreSQL, CamusDB does **not**
silently truncate trailing whitespace, and does **not** truncate on an explicit `CAST` to a narrower
bound — an overflowing cast raises the same error.

### Large values

A large `string`, `bytes` or `array` value can be stored compressed, or under a separate key outside the
row. Each column has a storage strategy that decides which forms the writer may use. The default is
`EXTENDED`, and the other strategies are `PLAIN`, `MAIN` and `EXTERNAL`.

```sql
CREATE TABLE files (id oid PRIMARY KEY, name string, content bytes STORAGE EXTERNAL);
ALTER TABLE files ALTER COLUMN content SET STORAGE PLAIN;
```

The form does not change a result. It changes the I/O a query does and the size of each stored version.
See [Large values: compression and out-of-line storage](storage-layout.md).

---


## The bounds of `substring`

`substr(x, start [, count])` is the same function as `substring(x, start [, count])`. Both take `string` or
`bytes`, and both follow PostgreSQL. The slice is the positions from `start` to `start + count`, and only the
positions from 1 up exist. So a `start` below 1 uses up part of the `count`.

| Statement | Result |
|---|---|
| `substring('abcde', 2, 2)` | `bc` |
| `substring('abcde', 0, 2)` | `a` |
| `substring('abcde', -1, 3)` | `a` |
| `substring('abcde', -5, 3)` | An empty string |
| `substring('abcde', 0)` | `abcde` |
| `substring('abcde', 6)` | An empty string |
| `substring('abcde', 2, -1)` | `CADB0400`: a negative `count` is an error, also when `start` is past the end |

A `NULL` argument gives `NULL`, also with a negative `count`. A `start` and `count` whose sum is past the
range of `int64` run to the end of the value.

## Characters and code points

Text functions count code points, as PostgreSQL does. A code point is one Unicode character. For example, `🙂` is one
code point, although it takes four bytes in UTF-8.

| Statement | Result |
|---|---|
| `length('🙂')` | `1` |
| `substring('x🙂y', 2, 1)` | `🙂` |
| `strpos('x🙂y', 'y')` | `3` |
| `ltrim('🙂x', '😀')` | `🙂x` |
| `octet_length('🙂')` | `4`, since this function counts bytes |

The same count applies to `POSITION`, to the character set of `trim`, `ltrim` and `rtrim`, to the `_` of a `LIKE`
pattern, to a `CHAR(n)` or `VARCHAR(n)` limit, and to the `start` argument and the result of `regexp_instr`, `regexp_count` and
`regexp_substr`. For a `bytes` value, these functions count bytes. The sort order of strings is separate, and it
uses UTF-16 code units. See [Differences from PostgreSQL](postgresql-differences.md#strings).

**Half of a pair.** A .NET string stores a character such as `🙂` as a pair of UTF-16 units. One unit alone is
not a character, and it cannot be encoded in UTF-8. A PostgreSQL string cannot contain one. In CamusDB, it is an
error at each place where it can enter, and it is not replaced with U+FFFD.

- A literal with half of a pair, such as `E'\uD83D'`, returns `CADB0400` when the statement is parsed.
- A statement with half of a pair in a string parameter, or in an element of a string array parameter, returns
  `CADB0400` before it is executed. The message names the parameter and the character position. This applies to
  all statements, SELECT included. This means that no function, filter or plan step sees the value.
- Over HTTP, a JSON body with an escaped half (`"\uD83D"`) does not parse, and the request returns
  `CADB0400`. The message gives the JSON path and the byte position. Any other body that does not parse gets
  the same answer.
- Over gRPC, a protobuf string is UTF-8. This means that a client library changes half of a pair to U+FFFD
  before it sends the request. The server then receives U+FFFD, which is a valid character, and cannot see the change.
- A typed write ticket with half of a pair returns `CADB0400`, and the message names the column. The row does
  not change.

**Regular expressions match UTF-16 units.** The regular expression engine does not offer a code point mode. A `.` or a
negated class matches one unit, which is half of `🙂`. This gives two differences from PostgreSQL.

- `regexp_count('🙂', '.')` is `2`, and `regexp_like('🙂', '^.$')` and `'🙂' ~ '^.$'` are `FALSE`.
- A function that would return half of a pair, or a position between the two units of a pair, returns `CADB0400`. This applies to
  `regexp_substr`, `regexp_match`, `regexp_replace`, `regexp_instr` and `regexp_split_to_array`.

To match such a character, name it in the pattern: `regexp_replace('a🙂', '🙂', 'b')` returns `ab`. To match any one
character, write the pair as a group before the dot: `(?:[\uD800-\uDBFF][\uDC00-\uDFFF]|.)`.

## Arrays

`array(T)` declares a homogeneous, ordered list of a **scalar** element type `T`. In this example, `tags` is an
array of integers.

```sql
CREATE TABLE t (id oid, tags array(int64), labels array(string), PRIMARY KEY (id))
```

The current version (v1) has some limitations.

- **Element type must be scalar.** Nested arrays (`array(array(...))`) are rejected.
- **Arrays are not indexable.** An `array` column cannot appear in a `PRIMARY KEY` or any index — doing
  so is rejected at `CREATE TABLE` time.
- **Nested arrays are rejected**, in a literal as well as a declaration — `ColumnValue` models exactly
  one element type.
- Elements may be `NULL` regardless of the declared element type.

Read one element with a subscript, `tags[1]`. The first element is `1`, and an index outside the array
gives `NULL`. See [PostgreSQL expression syntax](sql-expression-syntax.md#array-subscripts).

Three functions return a property of an array, such as its length.

| Function | Returns | Notes |
|---|---|---|
| `cardinality(a)` | `int64` | The number of elements, `NULL` elements included. `0` for an empty array. |
| `array_length(a, 1)` | `int64` | The same count, but `NULL` for an empty array, as in PostgreSQL. Any dimension other than `1` gives `NULL`. |
| `array_contains(a, v)` | `bool` | `true` when `v` is an element of `a`. |

All three return `NULL` for a `NULL` array. To count an empty array as `0`, use `cardinality`. `array_length` returns `NULL`
for it. `array_contains(a, v)` gives the same answer as `v IN (...)` over
the elements of `a`.

- An empty array gives `false`, even when `v` is `NULL`.
- A `NULL` value gives `NULL`.
- No match gives `NULL` when the array contains a `NULL` element, and `false` otherwise.
- Numbers compare by value. For example, `array_contains(ARRAY[1, 2], 2.0)` is `true`. If no comparison
  rule exists between the value's type and the element type, the call returns `CADB0400`, as
  `'x' IN (1, 2)` does:
  `array_contains(ARRAY[1, 2], 'x')` is an error (see
  [Comparing values of different types](#comparing-values-of-different-types)).

PostgreSQL writes the same test as `v = ANY (a)`, and you can use that form in CamusDB too: `v = ANY (a)`,
`v = SOME (a)` and `v <> ALL (a)` work, with the same rules. See
[`= ANY`, `= SOME` and `<> ALL`](sql-expression-syntax.md#-any--some-and--all). CamusDB does not
support `@>` or `<@` yet.

---

## Literal and value formats

How a value is written depends on the path. **The SQL literal form and the JSON form differ for
`bytes`** — see the note below.

### In SQL statements

| Type | Literal form | Example |
|------|--------------|---------|
| `int64` | integer | `42` |
| `float64`, `float32` | decimal | `3.14` |
| `numeric` | `NUMERIC '…'`, or a decimal written into a NUMERIC column | `NUMERIC '9.99'` |
| `string` | quoted | `'hello'` or `"hello"` |
| `bool` | `true` / `false` | `true` |
| `oid` | quoted ObjectId string | `'652b...'` |
| `date` | quoted `yyyy-MM-dd`, or `DATE '…'` | `'2026-03-15'`, `DATE '2026-03-15'` |
| `datetime` | quoted ISO-8601 UTC, or `TIMESTAMP '…'` / `DATETIME '…'` | `'2026-03-15T12:00:00Z'`, `TIMESTAMP '2026-03-15T12:00:00Z'` |
| `bytes` | **`X'…'` hex string** | `X'DEADBEEF'` |
| `array(T)` | `ARRAY[…]` | `ARRAY[1, 2, 3]` |

Numeric literals are parsed with the invariant culture (`.` is always the decimal separator,
independent of server locale). Date/datetime strings are parsed as UTC. If a value does not include a timezone,
it is assumed to be UTC. A `datetime` string can also be a bare date, `'2026-03-15'`, which is midnight UTC
of that day. Unparseable date/datetime/bytes literals raise `InvalidInput`. When date or datetime
text contains a character outside ASCII, the error names the first one, its code point and its position.
For a character that stands for an ASCII one, such as an en dash (U+2013) in place of `-`, the error
also names the ASCII character to type. Text copied from a word processor or a web page often has
such characters.

The typed literals `DATE '…'`, `TIMESTAMP '…'` and `DATETIME '…'` are the same as
`CAST('…' AS date)` and `CAST('…' AS datetime)`. They take the text forms above. This means that
`TIMESTAMP '2026-03-15 12:00:00'`, with a space in place of the `T`, raises `InvalidInput`. A typed
literal gives the value its type where an untyped string cannot take a column type, for example
`SELECT DATE '2026-03-15' + 1`. In a column default, write it in parentheses:
`DEFAULT(DATE '2026-03-15')`. A decimal number
can group its digits with underscores, `200_000`; see
[PostgreSQL expression syntax](sql-expression-syntax.md#digit-separators).

#### Comparing dates, datetimes and strings

A comparison (`=`, `<>`, `<`, `BETWEEN`, `IN`, a simple `CASE`, a `CHECK`) between a `date` and a
`datetime` compares the date as midnight UTC of that day, as in PostgreSQL. A string compared with a
`date` or a `datetime` is parsed into that type with the literal forms above. You don't need a `CAST`.

```sql
SELECT * FROM events WHERE day = '2026-03-15';                 -- date column
SELECT * FROM events WHERE created >= '2026-03-01'             -- datetime column, from midnight UTC
                       AND created <  '2026-04-01';
SELECT * FROM events WHERE created = day;                      -- true only at midnight
SELECT * FROM events WHERE day < make_timestamp(2026, 3, 15, 10, 0, 0);  -- includes 2026-03-15
```

A string that does not parse is an `InvalidInput` error. With a silent non-match,
`created >= '2026-13-01'` would look like a valid empty result. A `date` takes only
the `yyyy-MM-dd` form. This means that `day = '2026-03-15T00:00:00Z'` is an error too. An index on a
`date` or `datetime` column serves these comparisons. The planner converts the constant into the
column's type before it builds the index key.

#### String literals

There are two forms, following PostgreSQL.

**Plain — `'…'` or `"…"`.** No escape processing at all. A backslash is an ordinary character, and
the only special sequence is a doubled delimiter. You should use this form for almost all strings,
including regex patterns and Windows paths.

```sql
SELECT * FROM t WHERE name ~ '(\d+)';        -- pattern is (\d+), no doubling
INSERT INTO t (path) VALUES ('C:\Users');    -- stores C:\Users
SELECT 'it''s';                              -- stores it's
SELECT 'say "hi"';                           -- the other delimiter needs no escaping
```

**Escape — `E'…'` or `E"…"`.** A backslash introduces an escape. This is how you write control
characters.

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

An unrecognized escape yields the character itself. For example, `E'\d'` is `d`. For this reason, write regex
patterns in the plain form. A truncated numeric escape (`E'\x4'`) or an unpaired surrogate
raises `InvalidInput`.

Each value has a literal form. This means that any value that can be stored can also be emitted by
`SHOW CREATE TABLE` and read back unchanged. The server emits the plain form. It uses `E'…'` if the
value contains a control character.

A bytes literal is written `X'4D5A'` (or lowercase `x'…'`); `X''` is the empty byte string, and an
odd number of hex digits is an error. A bare `0xFF` is an **integer** literal, and bytes use a
separate syntax. The legacy form of passing bytes as a
string whose text starts with `0x` continues to be converted on insert, but only where the target column type
is known. `X'…'` carries its type by itself.

An array literal is written `ARRAY[a, b, c]`. Each element can be any expression: a literal, a column,
a cast, a function call or arithmetic, as in `ARRAY[n::string, upper(name), n + 1]`. Its element type
is the common type of its elements, by the rule in
[Result types of CASE, COALESCE, IFNULL and IF](#result-types-of-case-coalesce-ifnull-and-if). Every
element is converted to it. For example, `ARRAY[1, 2.5]` is an `array(float64)`, and `ARRAY[1, true]`
and `ARRAY[1, 'two']` are errors. See
[PostgreSQL expression syntax → Array elements](sql-expression-syntax.md#array-elements). Elements
coerce to the column's declared element type the same way scalars do. For example, you can write
`ARRAY[1, 2]` to an `array(float64)` column. `ARRAY[]` is empty and adopts the column's element type. Nested arrays
(`ARRAY[ARRAY[1]]`) are rejected.

`CAST` works for all scalar types, e.g. `CAST('2026-01-01' AS date)`, `CAST(x AS float32)`,
`CAST('0xFF00' AS bytes)`. The PostgreSQL shorthand `x::type` is the same as `CAST(x AS type)`; see
[PostgreSQL expression syntax](sql-expression-syntax.md#the--cast).

#### Cast to `int64`

`CAST(x AS int64)`, `x::int64` and `to_int64(x)` use one rule. A number with a fraction rounds half
away from zero, for all numeric types, as the table shows.

| Value | `float64`, `float32`, `numeric`, `jsonb` number |
|-------|-------------------------------------------------|
| `0.4` | `0` |
| `0.5` | `1` |
| `-0.5` | `-1` |
| `1.5` | `2` |
| `2.5` | `3` |
| `-2.5` | `-3` |

- This is the GoogleSQL rule, and `round(x)` gives the same integer. PostgreSQL uses it for `numeric`
  and `jsonb`. However, PostgreSQL rounds `float8::bigint` half to even, and `2.5::float8::bigint` is
  `2` there.
- The cast reads the exact binary value of a float. For example, `0.49999999999999994` gives `0`. A value that
  the float type cannot hold is already rounded when it is stored: `9007199254740993.0` is the `float64`
  value `9007199254740992`.
- `NaN`, `Infinity`, `-Infinity` and a result past the `int64` range fail with `CADB0420`
  (`IntegerValueOutOfRange`): HTTP 400, gRPC `OUT_OF_RANGE`.
- A string must spell an integer. `CAST('2.5' AS int64)` returns `CADB0400`, and it does not round.
  `doc ->> 'a'` gives a string. To round a `jsonb` number, cast `doc -> 'a'`.
- There is no implicit conversion. An `int64` column, an integer function argument, an array subscript,
  a bitwise operator and a `LIMIT` or `OFFSET` parameter refuse a float. Write the `CAST` where you want
  the rounded value.

#### Text form of each type

Each scalar type has one text form. `CAST(x AS string)`, `x::string`, `to_string(x)`,
`concat(…)` and the `||` operator all use it, and they always give the same text.

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

The text form uses the invariant culture and depends on the value only. This means that it is safe in
a column `DEFAULT` and a stored `CHECK`. `concat` skips a NULL argument and returns NULL when all
arguments are NULL. The `||` operator gives NULL for a NULL operand; see
[Concatenation](sql-expression-syntax.md#concatenation-).

The string cast reads each text form back. For `date`, `datetime`, `bytes`, `float32`, `float64`,
`uuid` and `numeric`, `CAST(CAST(x AS string) AS T)` returns `x`. A string cast to `bytes` reads the
hex after either a `\x` or a `0x` prefix. Write `'\xdeadbeef'` as a plain string literal. This form
does no escape processing, and the backslash is kept in the text.

#### Bytes functions

To convert `bytes` to text and back with an explicit format (`hex`, `base64`, `escape`), and to read
or change single bytes and bits, see [bytes-functions.md](bytes-functions.md).

### Over HTTP / JSON

The HTTP API exchanges values as JSON. Each type has the JSON form shown in the table.

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
| `jsonb` | the document itself as native JSON; numbers keep all their digits. With `"jsonbAsText": true` on the request, a JSON string that contains the text form; JSON `null` then differs from SQL `NULL` (see [json.md](json.md#json-null-over-rest)). A parameter is `{"type": 13, "strValue": "<json text>"}` |

> **Bytes format differs by path:** SQL literals use `X'…'` hex, JSON uses base64. A round-trip through
> each path is lossless, but the encodings are not interchangeable.

#### Array columns in REST query results

The REST result endpoints (`/execute-sql-query`, `/execute-sql-query-stream`, and the
`RETURNING` result of `/execute-sql-non-query`) write rows positionally. The cells carry no type.
Each entry of `columns` gives the column's `type` as a `ColumnType` number. An array is written as a
JSON array of its element values: a `date` element as ticks, a `bytes` element as base64, a
`float64` zero as `0`. To decode the elements, a client reads `elementType`. This field is present on an array column
(`type` 10).

```json
{"name": "ad", "type": 10, "elementType": 8}
```

`elementType` uses the same numbers as `type`. A non-array column does not include an `elementType`
field. A table column's element type comes from its declaration. This means that the field is present
even when the result is empty. For a computed array, such as a function result, it comes from the first non-null array in
the result. If the result has none, the field is omitted.

gRPC does not need such a field, because each array value carries its element type.

### Parameters of type `id`

A parameter of type `id` must hold an object id: 24 hex characters. The server converts upper-case hex
to lower case. Any other text returns `CADB0400` (invalid input), and the message names the
parameter. The usual cause is a client that sends a GUID as an `id` parameter. A GUID has 16 bytes and
an object id has 12. This means that a GUID cannot be an object id. Send it as a `uuid` parameter.

### Comparing values of different types

CamusDB follows GoogleSQL: two values can be compared if their types have a comparison rule, and
cannot be compared otherwise. The following pairs have a rule.

- Two values of the same type.
- Two numbers of different types: `int64` against `numeric` compares exactly, and a float compares
  with any other number as a double.
- A `date` against a `datetime`, or a `string` against either. The string is parsed. A string that
  does not parse returns `CADB0400` (see
  [Comparing dates, datetimes and strings](#comparing-dates-datetimes-and-strings)).
- A `string` against a `uuid`, an `id` or a `jsonb`. The string is parsed. A string that does not
  parse equals no value of that type. It is not an error.
- Two arrays with the same element type, or two arrays where one is `ARRAY[]` or contains only `NULL`
  elements. The elements compare with the rule for one type. This means that the element types must be the same:
  `ARRAY[1] = ARRAY['a']` and `ARRAY[1] = ARRAY[1.0]` return `CADB0400`. The message names both
  element types: `… argument types: ARRAY<Integer64>, ARRAY<String>`.

All other pairs cannot be compared. Examples are `int64` against `string` (`5 = '5'`), `float64` against
`string` (`0.0 = '0.0'`) and `bool` against `int64`. The same is true for `bytes` against `string`, `uuid`
against `id`, and `numeric` against `string`. **Any comparison of such a pair returns `CADB0400`**, and the message names the operator and
both types, as in this example.

```
No matching signature for operator = for argument types: Integer64, String
```

This applies to `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN` and `NOT IN`, the simple `CASE`,
`NULLIF`, `= ANY (…)`, `array_contains`, `JOIN … ON` and `HAVING`. It applies to a column, a literal
and a typed parameter alike. A CHECK constraint that compares such a pair returns `CADB0303` for the row at
write time (see [CHECK constraints](check-constraints.md)). To compare the values, write a CAST:
`CAST(s AS INT64) = a`.

The error is deliberate. Before, such an equality was silently FALSE. A statement such as
`DELETE FROM t WHERE text_col = int_col` returned success and did not delete any row.

The check has the following details.

- `x IN (a, b)` means `x = a OR x = b`, and `OR` evaluates both sides. An item that cannot be compared
  with `x` fails the statement even when another item matches: `1 IN (1, 'a')` fails.
- A simple `CASE` stops at the first `WHEN` that matches, and a later `WHEN` is not evaluated. However,
  its type is checked when the statement shows it: `CASE 5 WHEN 5 THEN 1 WHEN 'a' THEN 2 END` fails.
- A NULL operand makes the comparison UNKNOWN before any type check. This means that `NULL = 'a'` is
  NULL and does not return an error.
- The check is made when the statement is bound, for each pair whose types the statement shows. See
  [Errors that do not depend on the rows](#errors-that-do-not-depend-on-the-rows).
- An index is not used to answer a comparison of two types that cannot be compared. The filter evaluates it and raises
  the error. This means that an index seek and a table scan give the same result.
- A set operation (`UNION`, `INTERSECT`, `EXCEPT`) returns an error for two array columns whose element
  types are known and are not the same. It does the same for two scalar types that cannot be combined. When an array expression's
  element type is known only from its value, this check does not apply.

### Errors that do not depend on the rows

A mistake that the statement alone shows fails before any row is read. The error is the same over an empty
table, under `LIMIT 0` or under `WHERE FALSE` as over a table with rows. It is also the same in a branch that is not evaluated, such
as the right side of `a = 'x' AND FALSE`. An unknown column has always worked this way, and the following
mistakes now work the same way.

- A function name that does not exist (`CADB0407`), or a wrong argument count (`CADB0400`).
- A comparison of two types that cannot be compared: `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`, `NOT IN`, a simple `CASE`,
  `NULLIF`, `ANY`, `SOME`, `ALL`.
- An operator with a wrong operand type: `+ - * /`, unary minus, `||`, the bitwise operators, `AND`, `OR`,
  `NOT`, `IS TRUE`, `IS FALSE`, a subscript, and `LIKE`, `ILIKE` and the regex operators (`CADB0407`).
- A string literal that is not a valid value beside a `date` or a `datetime`: `day = 'not a date'`.
- A `WHEN` of a searched `CASE`, or the condition of `IF`, that is not a `bool`.
- A `WHERE`, `HAVING` or `ON` condition that is not a `bool`. See below.
- A value of a type that the column cannot take (`CADB0401`): `UPDATE t SET a = 'x' WHERE FALSE` on an `int64`
  column, or `INSERT INTO t (a) SELECT s FROM other WHERE FALSE` from a `string` column. A string literal that
  does not convert fails too. A table with a `BEFORE` trigger is skipped. The trigger may replace the
  value. The write checks each row after the trigger ran.

The check covers all expressions of a `SELECT`, its subqueries, derived tables, set operation operands and
the arguments of a window call or a table function. It also covers `CREATE VIEW` and `CREATE TABLE AS SELECT`,
the `SET` values, `WHERE` and `RETURNING` of a write, `ON CONFLICT DO UPDATE`, and `INSERT … SELECT`.

The statement shows the type of a literal, a `CAST`, a column, and an operator over those. It also shows the type
of a subquery whose column is one of those, even when the subquery returns no row. For example,
`id = (SELECT name FROM empty)` and `id IN (SELECT name FROM empty)` fail on an `int64` `id`.

A **parameter** and a **function result** have a type that only the value shows. They are checked when a value
reaches the operator. Thus `a = @p` with a `string` `@p` fails on a table with rows and returns no rows on an
empty table. This is also true for a subquery whose column is a function or an aggregate. It is true for any
derived `string` column too, since a derived table also declares `string` for a column whose type it does not know. The
check does not read such a column as `string`, even when it comes from a real `string` column. The arguments of a function are also
checked when it is called: `length(v)` with a number fails for the first row. The check does not guess a type. This
means that a correct statement does not fail.

#### Division by zero

A division by zero whose operands are both constant is a statement mistake too. It returns `CADB0400`
before any row is read. `SELECT 1 / 0 FROM t WHERE FALSE`, `1 / (1 - 1)` and `1.5 / 0` all fail, in any
position above. This follows PostgreSQL.

A constant condition removes a branch. The following expressions do not fail: `CASE WHEN FALSE THEN 1 / 0 ELSE 1 END`,
`IF(true, 1, 1 / 0)`, `FALSE AND 1 / 0 = 1` and `TRUE OR 1 / 0 = 1`. The order counts, as it does when a row is
evaluated: `1 / 0 = 1 AND FALSE` fails. A subquery is checked separately. For example, `CASE WHEN FALSE THEN (SELECT 1 / 0 FROM t)
ELSE 1 END` fails, which is [a difference from PostgreSQL](postgresql-differences.md#statement-checks).

A condition that reads a row does not remove a branch. For example, `CASE WHEN v > 0 THEN 1 / 0 ELSE 1 END`
fails, even on a table where no row selects that branch. To guard a division, read the divisor from the row:
`CASE WHEN v <> 0 THEN 1 / v END` succeeds.

A division that uses one of the following operands is not constant. It fails only when a row is evaluated.

- A column (`v / 0`, `1 / v`).
- A parameter (`1 / @p`).
- A subquery (`1 / (SELECT 0 FROM t)`).
- A function call (`length('a') / 0`, `mod(1, 0)`). This includes `1 % 0`, since the parser builds `%` as a
  call of `mod`.

A constant division is allowed in `CREATE VIEW` and in a `CHECK` constraint. The statement that reads the view, or
writes to the table, fails. `CREATE TABLE AS SELECT` and `CREATE MATERIALIZED VIEW` execute their `SELECT`. This
means that they fail at once. `EXISTS` discards its select list, `GROUP BY`, `DISTINCT` and `ORDER BY`. For this
reason, `EXISTS (SELECT 1 / 0 FROM t)` does not fail, whether the table is empty or not. With an aggregate in the select
list, `HAVING`, `OFFSET` or `LIMIT 0`, `EXISTS` checks the body as written. This means that
`EXISTS (SELECT 1 / 0 FROM t LIMIT 0)` fails.

`COALESCE(1, 1 / 0)` does not fail, whether the table is empty or not. The first argument decides the result. An
argument that reads a row does not remove a later division before the statement is executed. This means that
`COALESCE(v, 1 / 0)` fails. A division that depends on a row is evaluated when `COALESCE` reaches it:
`COALESCE(n, 1 / v)` divides only in a row where `n` is NULL. See [Result types of CASE, COALESCE, IFNULL and IF](#result-types-of-case-coalesce-ifnull-and-if).

Other value errors need a row: `SELECT 9223372036854775807 + 1 FROM t WHERE FALSE` returns no rows.

**A condition must be a `bool`.** A `WHERE`, `HAVING` or `ON` condition must be a `bool` or NULL. With `TRUE`
the row is selected, and with `FALSE` or NULL it is dropped. A number is not a truth value. This means that
`WHERE 1` and `UPDATE t SET a = 0 WHERE 1` return `CADB0400`, as in PostgreSQL. Write `WHERE TRUE` to select all
rows on purpose, and `WHERE a <> 0` to test a number.

### Result types of CASE, COALESCE, IFNULL and IF

A `CASE`, `COALESCE`, `IFNULL` (and its alias `NVL`) and `IF` return one of several inputs: the
`THEN` and `ELSE` results of a `CASE`, the arguments of `COALESCE` and `IFNULL`, and the two result
arguments of `IF`. CamusDB follows GoogleSQL: the inputs must have a common type, and each value is
converted to it. The elements of an `ARRAY[...]` literal follow the same rule (see
[Array elements](sql-expression-syntax.md#array-elements)). The table shows how two types combine.

| Inputs | Result type |
|---|---|
| A `NULL` input, or an omitted `ELSE` | the type of the other inputs |
| One type | that type |
| Two numbers | the type that `+` gives for them: `int64` with `float64` gives `float64`, `int64` with `numeric` gives `numeric` |
| A `date` with a `datetime` | `datetime`; the date becomes its midnight |
| A quoted string literal with a `date` or a `datetime` | that temporal type; the literal is parsed |
| Two arrays | an array, when the element types are the same or one array is `ARRAY[]` |
| Any other pair | error `CADB0400` |

The error names the construct and both types, as in this example.

```
No common type for the results of CASE: Integer64 and String; write a CAST to make the types agree
```

The following examples apply the rule.

- `CASE WHEN c THEN 1 ELSE 1.5 END` is `float64` in all rows, also in a row that returns `1`.
- `COALESCE(int_col, float_col)` is `float64`, also in a row where `float_col` is `NULL`. This holds
  in a query, an `UPDATE`, a `DELETE`, a CHECK constraint, an expression index and a view's
  `WITH CHECK OPTION` alike.
- `CASE WHEN c THEN 1 ELSE 'a' END` and `COALESCE(1, 'a')` fail. `IFNULL(true, 1)` fails.
- `CASE WHEN c THEN 1 ELSE '2' END` fails. A quoted literal is a `string`, and it does not become a
  number. PostgreSQL reads `'2'` as an integer here; GoogleSQL does not. Write `CAST('2' AS INT64)`.
- `COALESCE(date_col, '2026-01-01')` is a `date`: a quoted literal does become a `date` or a
  `datetime`, as in GoogleSQL. The literal is parsed when the statement is bound. This means that
  `COALESCE(date_col, 'soon')` returns `CADB0400` before any row is read.

**Only a literal becomes a date.** A `string` column with a `date` column fails:
`CASE WHEN c THEN date_col ELSE text_col END` is an error. A `CASE`, `COALESCE`, `IFNULL` or `IF`
whose inputs are all literals is not a literal itself. For example,
`COALESCE(date_col, CASE WHEN c THEN '2026-01-01' ELSE '2026-01-02' END)` fails, as in GoogleSQL. A
`string` value whose type only its value shows, such as a parameter, is parsed when it is returned.
A `string` parameter with the value `'2026-01-01'` becomes that date, and one with other text fails.

**When the check runs.** The check is made when the statement is bound, before any row is read. It
covers each input whose type the statement shows: a literal, a `CAST`, a column, arithmetic over
those, and a nested `CASE`, `COALESCE`, `IFNULL` or `IF`. This means that a branch that is not
evaluated fails, and a statement over an empty table fails too. In a `SELECT`, the check covers the
select list and the `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY` and `JOIN … ON` clauses. In an `UPDATE`
or a `DELETE`, it covers the `SET` values and the `WHERE` clause.

An input whose type only its value shows, such as a parameter or a function call, is checked when
the value is returned. A branch that does not run is not checked then:
`CASE WHEN false THEN @p ELSE 1 END` with a `string` `@p` returns `1`. A function call in a `COALESCE`
argument that is not evaluated is not checked either. For example, `COALESCE(1, upper(s))` returns `1`.
Also, `COALESCE(1, sqrt(f))` is an `int64`. A parameter in a `COALESCE` argument is read also when the
argument does not win. This means that `COALESCE(id, @p)` with a `string` `@p` fails.

**Evaluation order.** A `CASE` stops at the first `WHEN` that matches, and an `IF` evaluates only the
result that its condition selects. `COALESCE`, `IFNULL` and `NVL` evaluate their arguments in order and stop
at the first that is not NULL. An argument after it raises no error, and a `nextval` in it does not draw
a value. `NULLIF` always evaluates both arguments. A subquery, an aggregate or a window function is
computed before the row is evaluated. For this reason, the early stop does not stop them.

#### IF

`IF(condition, true_result, else_result)` returns `true_result` when `condition` is `TRUE`, and
`else_result` when it is `FALSE` or `NULL`. It is the same as
`CASE WHEN condition THEN true_result ELSE else_result END`, and the same rules apply.

- A `condition` that is not a boolean returns `CADB0400`.
- Only the selected result is evaluated. For example, `IF(true, 1, 1 / 0)` returns `1`. A condition that reads a
  row does not remove a result before the statement is executed. This means that `IF(v > 0, 1 / 0, 1)` fails. See
  [Division by zero](#division-by-zero).
- The two results follow the rule above.

`IF` is a reserved word. `IF NOT EXISTS` and `IF EXISTS` in DDL do not change. `EXPLAIN` shows
`IF(…)`.

#### GREATEST and LEAST

`GREATEST(a, b, …)` returns the largest of its arguments, and `LEAST(a, b, …)` the smallest, as in
PostgreSQL. Both functions follow these rules.

- A call takes one argument or more. A call with zero arguments fails.
- A NULL argument is left out. The result is NULL if all arguments are NULL, and not otherwise. For
  example, `GREATEST(NULL, 2, 1)` is `2`. MySQL returns NULL when any argument is NULL; CamusDB does not.
- The result has the common type of the arguments, by the rule above for `CASE` and `COALESCE`. For
  example, `GREATEST(3, 2.5)` is the `float64` `3`. `GREATEST(int_col, float_col)` is a `float64` also in
  a row where `float_col` is NULL. Two arguments that do not have a common type, such as `GREATEST(1, 'a')`,
  return `CADB0400` when the statement is bound.
- The values compare as `<` and `>` compare them: `NaN` is above `Infinity`, and strings compare in the
  string order. For example, `LEAST('apple', 'Cherry')` is `'Cherry'`. Of two equal values the first
  one wins.
- All arguments are evaluated, because each one can be the result.

To declare `string(N)` / `array(T)` over HTTP, the create-table column carries `maxLength` and
`arrayElementType` fields alongside `type`.

---

## Temporal functions

The built-in date/time functions return engine-typed `date` or `datetime` values. The
JSON wire representation is unchanged (ISO-8601 string via the `isoValue` field). Since the engine
type is `date` or `datetime`, function results work with typed columns. For example, you can compare
them with a typed column, insert them with no `CAST`, and use them in an indexed comparison.

| Function | Return type | Notes |
|----------|-------------|-------|
| `NOW()` / `CURRENT_TIMESTAMP()` / `TRANSACTION_TIMESTAMP()` | `datetime` | Start of the transaction, UTC. See [Current time](#current-time) |
| `STATEMENT_TIMESTAMP()` | `datetime` | Start of the statement, UTC |
| `CLOCK_TIMESTAMP()` / `SYSDATE()` | `datetime` | The clock at each call, UTC |
| `CURRENT_DATE()` | `date` | UTC date of the start of the transaction |
| `DATE_ADD(temporal, n, unit)` | `datetime` | See promotion rule below |
| `DATE_ADD(temporal, interval)`, `DATE_SUB(temporal, interval)` | `datetime` | MySQL form: `DATE_ADD(d, INTERVAL 1 DAY)`. See [Intervals](#intervals) |
| `DATE_SUB(temporal, n, unit)` | `datetime` | `DATE_ADD` with `-n` |
| `DATE_TRUNC(unit, temporal [, zone])` | `datetime` | Always yields datetime (Postgres-compatible). With a zone, the instant the unit starts on that zone's clock. See [Time zones](#time-zones) |
| `FROM_UNIXTIME(seconds)` | `datetime` | Unix epoch seconds → UTC datetime. A fraction rounds to the nearest microsecond |
| `DATE_DIFF(a, b, unit)` | `int64` | Difference in `unit` units (unchanged) |
| `DATE_PART(unit, temporal [, zone])` | `int64` | Extracts calendar component. With a zone, from that zone's clock. See [Time zones](#time-zones) |
| `EXTRACT(unit FROM temporal)` | `int64` | SQL-standard form of `DATE_PART(unit, temporal)` |
| `AGE(a, b)`, `AGE(a)` | `interval` | PostgreSQL. Years, months and days from `b` to `a`. See [Intervals](#intervals) |
| `MAKE_INTERVAL(years, months, weeks, days, hours, mins, secs)` | `interval` | PostgreSQL. All parts are optional, by position |
| `JUSTIFY_DAYS(i)`, `JUSTIFY_HOURS(i)`, `JUSTIFY_INTERVAL(i)` | `interval` | PostgreSQL. Move 30 days into a month, 24 hours into a day |
| `DATE_BIN(stride, temporal, origin)` | `datetime` | PostgreSQL. The start of the bin that contains the value |
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
| `TO_CHAR(temporal, pattern [, zone])`, `DATE_FORMAT(temporal, pattern)` | `string` | Format with a PostgreSQL template or MySQL `%` specifiers; see [date-patterns.md](date-patterns.md). `TO_CHAR` with a zone prints that zone's clock |
| `TO_TIMESTAMP(text, pattern)`, `STR_TO_DATE(text, pattern)` | `datetime` | Strict parse with a pattern; see [date-patterns.md](date-patterns.md) |
| `TO_DATE(text, pattern)` | `date` | Strict parse with a PostgreSQL template, date only. `TO_DATE(x)` with one argument is the cast to `date`, as before |
| `TO_TIMESTAMP(seconds)` | `datetime` | The PostgreSQL spelling of `FROM_UNIXTIME(seconds)` |

### Date arithmetic

The `+` and `-` operators accept a `date` with the PostgreSQL rules. The integer is a number of days.

| Expression | Result |
|------------|--------|
| `date + int64`, `int64 + date` | `date` |
| `date - int64` | `date` |
| `date - date` | `int64`: the number of days from the right date to the left date |

```sql
SELECT current_date() + 30                       -- 30 days from today
SELECT due - current_date() FROM invoices        -- days until each invoice is due
SELECT * FROM events WHERE day > current_date() - 7
```

- `CAST('2024-02-28' AS date) + 1` is `2024-02-29`, and `CAST('2024-03-01' AS date) - CAST('2024-02-01' AS date)`
  is `29`. 2024 is a leap year.
- A NULL operand gives NULL.
- A result before `0001-01-01` or after `9999-12-31` is an error (`CADB0400`, `date out of range`). The
  date does not wrap.

As in PostgreSQL, the other pairs of operands are errors.

- `*` and `/` with a date, `int64 - date`, and `date + date`.
- A date with a `float64`, a `numeric` or a `bool`. The number of days must be an `int64`.
- A `datetime` with an integer. A datetime moves by an interval: `ts + INTERVAL '1 day'`. See
  [Intervals](#intervals).
- A quoted string: `'2024-01-01' + 1` is an error. Write `CAST('2024-01-01' AS date) + 1`.

However, a date and an integer do not have a common type for `CASE`, `COALESCE` and `UNION`.

### Intervals

An `interval` is a span of time with three parts: months, days and microseconds. The parts are stored
separately, as in PostgreSQL. The length of a month or a day depends on the date that it is added to.
`2024-01-31 + 1 month` is `2024-02-29`.

**Literals and casts**

| Form | Example | Value |
|------|---------|-------|
| PostgreSQL text | `INTERVAL '1 year 2 mons 3 days 04:05:06'` | as written |
| ISO-8601 text | `INTERVAL 'P1DT2H'` | 1 day 2 hours |
| SQL standard, one unit | `INTERVAL '3' HOUR` | 3 hours |
| MySQL, one unit | `INTERVAL 3 HOUR`, `INTERVAL @n DAY` | 3 hours, `@n` days |
| Cast of text | `CAST('1 day' AS interval)`, `'1 day'::interval` | 1 day |

- The units are `microsecond`, `millisecond`, `second`, `minute`, `hour`, `day`, `week`, `month`,
  `quarter`, `year`, `decade`, `century` and `millennium`, with their plurals and the PostgreSQL short
  forms (`mons`, `hrs`, `us`). Case does not matter.
- The text can have signs and `ago`: `'-1 day +2 hours'`, `'3 hours ago'`. A number with no unit is a
  number of seconds.
- A fraction moves to the smaller parts, as in PostgreSQL: `'1.5 days'` is 1 day 12 hours, and
  `'1.5 months'` is 1 month 15 days. A fraction of a year rounds to whole months: `'1.1 years'` is 1 year
  1 month.
- `INTERVAL '3' HOUR` takes a number only. `INTERVAL '1 day' HOUR` is an error.
- `INTERVAL n unit` is `n * INTERVAL '1 unit'`. This means that `n` can be a column or a parameter.
- Only `CAST(text AS interval)` makes an interval from another type. A number needs a unit.

**Operators**

| Expression | Result |
|------------|--------|
| `datetime ± interval`, `interval + datetime` | `datetime` |
| `date ± interval`, `interval + date` | `datetime` (the date is midnight UTC) |
| `datetime - datetime`, `date - datetime`, `datetime - date` | `interval` of days and time, no months |
| `interval ± interval`, `-interval` | `interval` |
| `interval * number`, `number * interval`, `interval / number` | `interval` |
| `=`, `<>`, `<`, `<=`, `>`, `>=` on two intervals | `bool` |

```sql
SELECT * FROM events WHERE created > now() - INTERVAL '7 days'
SELECT created + INTERVAL 1 MONTH FROM invoices
SELECT finished - started AS duration FROM jobs
SELECT DATE_ADD(created, INTERVAL 2 HOUR), DATE_SUB(created, INTERVAL '1 day') FROM events
```

- A datetime plus an interval adds the months, the days and the time, in that order. A day of the
  month that does not exist moves back to the last day of the month: `2024-01-31 + INTERVAL '1 month'`
  is `2024-02-29`, and `2024-02-29 + INTERVAL '1 year'` is `2025-02-28`. All values are UTC. This means
  that a day is always 24 hours.
- `datetime - datetime` puts whole days in the days part: `2024-01-02 03:00 - 2024-01-01 00:00` is
  1 day 3 hours. `DATE_DIFF` gives the same span as a number.
- `interval * number` and `interval / number` use the PostgreSQL rule: a fraction of a month moves into
  days (30 a month), and a fraction of a day moves into the time. `INTERVAL '1 month' * 0.5` is 15 days.
- Two intervals compare by one total, where a month is 30 days and a day is 24 hours. Thus
  `INTERVAL '1 month' = INTERVAL '30 days'` is true, and `GROUP BY` and `DISTINCT` put the two in one
  group.
- A NULL operand gives NULL. A part past its range (32-bit months and days, 64-bit microseconds) is an
  error (`CADB0400`, `interval out of range`). A datetime result before year 1 or after year 9999 is an
  error (`CADB0400`, `timestamp out of range`).
- Other pairs are errors, for example an interval plus a number (`INTERVAL '1 day' + 1`) or an interval
  minus a date or a datetime (`INTERVAL '1 day' - d`). `number / interval` and `interval * interval`
  are also errors.

**Text form and the wire.** The text of an interval is its ISO-8601 form, in the PostgreSQL `iso_8601`
style: `P1Y2M3DT4H5M6.5S`. The years and months share the sign of the months part, and the hours,
minutes and seconds share the sign of the time part. For example, `-1 day +2 hours` is `P-1DT2H`. A zero
interval is `PT0S`. `CAST(x AS string)` and `||` give this text. A query that returns an interval sends
this text on all transports (REST JSON and gRPC), and the column type in the result metadata is `string`. Thus a
client needs no change.

**No table stores an interval.** An interval is a value of an expression only. Each of the following is
an error.

- a column of type `interval` in `CREATE TABLE` or `ALTER TABLE … ADD COLUMN`;
- an interval column in `CREATE TABLE … AS SELECT` or `CREATE MATERIALIZED VIEW`;
- an INSERT or an UPDATE that writes an interval into a column.

A view that is not materialized can return an interval. It does not store rows. To keep an interval in a table, store
`CAST(x AS string)` in a string column, and read it back with `CAST(s AS interval)`. An interval also
cannot be an array element.

**Index use.** A bound such as `created > now() - INTERVAL '7 days'` is a constant for the statement.
This means that an index on `created` gives a range scan, and `EXPLAIN` shows `index-range-scan` with that index. This is also true for
`DATE_SUB(now(), INTERVAL 1 WEEK)` and `current_date() - 7`. When the column is part of the expression
(`created + INTERVAL '7 days' > now()`), the bound is not a constant, and the query reads all rows
through the primary key.

**Interval functions**

| Function | Result | Example |
|----------|--------|---------|
| `age(a, b)` | `interval` | `age(CAST('2001-04-10' AS date), CAST('1957-06-13' AS date))` is 43 years 9 months 27 days |
| `age(a)` | `interval` | the same as `age(current_date(), a)` |
| `make_interval(years, months, weeks, days, hours, mins, secs)` | `interval` | `make_interval(0, 0, 0, 1, 2)` is 1 day 2 hours |
| `justify_days(i)` | `interval` | `'35 days'` gives 1 month 5 days |
| `justify_hours(i)` | `interval` | `'27 hours'` gives 1 day 3 hours |
| `justify_interval(i)` | `interval` | `'1 mon -1 hour'` gives 29 days 23 hours |
| `date_bin(stride, x, origin)` | `datetime` | 15:44:17 in 15-minute bins from midnight gives 15:30:00 |
| `EXTRACT(unit FROM i)`, `date_part(unit, i)` | `int64` | `EXTRACT(hour FROM INTERVAL '36 hours')` is 36 |

- `age(a, b)` subtracts each calendar field of `b` from the same field of `a`. A negative field borrows
  from the next larger field, and a day borrows the length of the month of the earlier value, as in
  PostgreSQL. `age(a)` measures from midnight UTC of the current date of the transaction. `age` reads the
  statement clock. This means that it is volatile with one or two arguments: a `CHECK` cannot call it, and a query
  that calls it does not use the result cache. PostgreSQL marks the two-argument form immutable.
- `make_interval` takes its parts by position only. CamusDB does not support named arguments
  (`make_interval(days => 10)`). A missing part is zero. All parts except `secs` must
  be an `int64`. The seconds round to a whole microsecond. A part that does not fit is an error
  (`CADB0400`, `interval out of range`). PostgreSQL 16 wraps such a part and does not return an error.
- The `justify_*` functions use the PostgreSQL sign rules: after the move, a part that has the opposite
  sign of a larger part gives one unit to it. `justify_days(INTERVAL '1 month -1 day')` is 29 days.
- `date_bin` puts `x` in bins of width `stride` that start at `origin`. A value before the origin goes to
  the earlier bin. The stride must be greater than zero and must not have a month part (`CADB0400`).
  `origin` is required, as in PostgreSQL.
- `EXTRACT` from an interval reads each field as stored, and does not move one field into another:
  `EXTRACT(day FROM INTERVAL '36 hours')` is 0. The years are `months / 12`, and `month` is `months % 12`.
  Each field has the sign of the part that it comes from. As for a datetime, the result is an `int64`:
  `second` is the whole seconds, `millisecond` and `microsecond` are the fraction of the current second
  only, and `epoch` is whole seconds, rounded down, where a year is 365.25 days and a month is 30 days.
  PostgreSQL returns a `numeric` with the fraction. `week`, `dow`, `isodow`, `doy` and `isoyear` name a
  position in a calendar and are errors for an interval (`CADB0400`).

**Not supported yet:** `SUM` and `AVG` of intervals, a string literal compared with an interval without
the `INTERVAL` keyword (`x > '1 day'`), and the MySQL compound units (`'1:30' HOUR_MINUTE`).

### Date constructors

`MAKE_DATE`, `MAKE_TIMESTAMP`, `MAKEDATE` and `LAST_DAY` are deterministic. A `CHECK` can call them, and
a `DEFAULT` that calls them is evaluated once, when the table is created. A NULL argument gives NULL.

An invalid part raises `InvalidInput` (`CADB0400`), and the message names the part. The table shows
valid and invalid calls.

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

The constructors follow these rules.

- The year must be 1-9999. PostgreSQL reads a negative year as a year BC. CamusDB does not support BC dates,
  and a negative year is an error.
- In `MAKE_TIMESTAMP`, the second can be an integer, float or NUMERIC value that is at least 0 and less than 60. The
  fraction rounds to the nearest microsecond, as in PostgreSQL. A second that rounds up to 60 moves into
  the next minute.
- `MAKEDATE` follows MySQL when the day of the year is past the end of the year: the date moves into the
  next year. MySQL returns NULL for a day of the year below 1, and CamusDB raises an error. MySQL reads
  a two-digit year as 1970-2069, and CamusDB reads each year as written.
- `LAST_DAY` reads the month of the UTC value. This means that an offset can move the value into another
  month first.

`UNIX_TIMESTAMP_MS` gives the millisecond epoch that JavaScript uses (`Date.now()`). Neither PostgreSQL
nor MySQL has this function. The name adds the unit to `UNIX_TIMESTAMP`, and the arity and clock rule are
the same. `FROM_UNIXTIME(UNIX_TIMESTAMP_MS(t) / 1000.0)` equals `DATE_TRUNC('millisecond', t)` up to about
the year 2255. After that year, a float cannot hold each microsecond. Use a NUMERIC divisor for later
dates: `FROM_UNIXTIME(UNIX_TIMESTAMP_MS(t) / NUMERIC '1000')`.

### Current time

Each function that returns the current time reads one of three clocks. The table shows which clock
each function reads.

| Function | Value | Same value for |
|----------|-------|----------------|
| `NOW()`, `CURRENT_TIMESTAMP()`, `TRANSACTION_TIMESTAMP()`, `CURRENT_DATE()`, `UNIX_TIMESTAMP()`, `UNIX_TIMESTAMP_MS()` | Start of the transaction | All rows and statements of the transaction |
| `STATEMENT_TIMESTAMP()` | Start of the statement | All rows of the statement |
| `CLOCK_TIMESTAMP()`, `SYSDATE()` | The clock at the time of the call | Not shared. Each call reads the clock again |

For example, `SELECT NOW() = NOW()` is true, and `INSERT INTO t SELECT id, NOW() FROM s` writes one time
into all rows. Two statements in one `BEGIN ... COMMIT` see the same `NOW()`. A statement outside an
explicit transaction is executed in a separate transaction. This means that its `NOW()` is the start of
that statement. `STATEMENT_TIMESTAMP()` is never earlier than `NOW()`.

**Behavior change.** Before this change, `NOW()`, `CURRENT_TIMESTAMP()`, `CURRENT_DATE()` and
`UNIX_TIMESTAMP()` read the clock on each call. One statement could write a different time into
each row. They now return the start of the transaction, as in PostgreSQL. To read the clock on each
call, use `CLOCK_TIMESTAMP()`.

**Comparison with PostgreSQL and MySQL.** CamusDB follows PostgreSQL. MySQL fixes `NOW()` at the start
of the statement. CamusDB needs the parentheses: write `CURRENT_TIMESTAMP()`, because
`CURRENT_TIMESTAMP` alone is not valid.

| CamusDB | PostgreSQL | MySQL |
|---------|------------|-------|
| `NOW()`, `CURRENT_TIMESTAMP()`, `TRANSACTION_TIMESTAMP()` | The same: start of the transaction | `NOW()` is the start of the statement. There is no transaction-start function |
| `STATEMENT_TIMESTAMP()` | The same | `NOW()` |
| `CLOCK_TIMESTAMP()`, `SYSDATE()` | `clock_timestamp()` | `SYSDATE()` |
| `CURRENT_DATE()` | The same: date of the start of the transaction | `CURDATE()`: date of the start of the statement |
| `UNIX_TIMESTAMP()` | `extract(epoch FROM now())`, which includes the fraction | `UNIX_TIMESTAMP()`: start of the statement |

**Precision and clocks.** The value has the full precision of the server clock (100 ns ticks). The
start of a transaction is the later of the wall clock and the hybrid logical clock (HLC) of the
transaction. The HLC is the clock the cluster uses to order events. Its physical part is in
milliseconds and is never earlier than a clock the node heard from. This means that when a transaction
follows another one on a different node, its `NOW()` is not earlier, to the millisecond, even when the clocks
of the two nodes differ. When the clock of a node is behind the HLC, `NOW()` can be later than
`CLOCK_TIMESTAMP()` on that node.

**Column defaults.** A `DEFAULT NOW()` gives all rows of one `INSERT` the same time, which also
equals an explicit `NOW()` in that `INSERT`. A `DEFAULT STATEMENT_TIMESTAMP()` gives each row the
start of its statement. `ALTER TABLE ... ADD COLUMN ... DEFAULT NOW()` gives all existing rows one
time: the time the fill of the column started. The fill commits in batches, and it cannot use the time
of one batch. When a new leader resumes a fill, the rows that remain get the start time of the resumed
fill.

**Other rules.** The following rules also apply to the clock functions.

- A retried transaction is a new transaction, and it has a new `NOW()`, as in PostgreSQL.
- A query that calls any of these functions does not use the shared query result cache.
- `WHERE col > NOW()` can use an index on `col`, also on a table of a join, as a literal bound
  can. The index range and the row filter read the same time, the start of the transaction.
- In a distributed query, the node that coordinates the statement evaluates all clock calls (`NOW()`,
  `CURRENT_DATE()` and the others). A clock call is not sent to another node. A filter that calls
  only functions of the row, such as `DATE_PART` with a zone, can be evaluated on the node that stores
  the rows.
- With `AS OF SYSTEM TIME`, `NOW()` is the start of the transaction. It is not the time of the
  snapshot.

### Date units

Each unit is computed on the UTC value, unless the call has a zone argument (see
[Time zones](#time-zones)). The unit is a string (`'week'`), or a bare word in
`EXTRACT(week FROM x)`. A unit that is not meaningful in a function returns the same
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
the unit literal. PostgreSQL returns `numeric` with fractions. This gives different results for two
units.

- `epoch` is whole seconds, rounded down. Half a second before 1970-01-01 gives `-1`.
- `millisecond` and `microsecond` are the fraction of the current second only, as MySQL's
  `MICROSECOND()`. For `12.345` seconds CamusDB returns `345` milliseconds; PostgreSQL returns `12345`.

`DATE_DIFF` truncates toward zero. `day` and `week` count calendar days and ignore the time of day;
`month`, `quarter` and the year-based units count complete anniversaries.

`DATE(x)` is not a function. `date` is a reserved type keyword. Use `CAST(x AS date)`.

**DATE_ADD promotion rule:** the return type is always `datetime`. The evaluator promotes any `date`
input to `datetime` before applying the arithmetic, consistent with Postgres `date + interval`
behavior. The unit argument is a string literal, such as `'day'` or `'hour'`.

**Typed temporal arguments:** you can pass `date` or `datetime` column references directly to the
functions that take a temporal argument (`DATE_ADD`, `DATE_DIFF`, `DATE_PART`, `DATE_TRUNC`,
`UNIX_TIMESTAMP`), in addition
to string literals. Passing a typed temporal column no longer requires `CAST` to string.

**Wire format unchanged:** `NOW()` serializes as an ISO-8601 string in JSON responses (via
`isoValue`), as before. Only the engine-internal type changes.

The following queries use typed temporal values.

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

### Time zones

A `datetime` is an instant, stored and shown in UTC. `DATE_TRUNC`, `DATE_PART` and `TO_CHAR` take an
optional time zone as their last argument. With a zone, they work on the clock of that zone.

```sql
-- Orders per local day in Bogota
SELECT DATE_TRUNC('day', created, 'America/Bogota') AS day, count(*)
FROM orders
GROUP BY DATE_TRUNC('day', created, 'America/Bogota')

-- The local hour of each event in India
SELECT DATE_PART('hour', created, 'Asia/Kolkata') FROM events

-- A local timestamp, with the zone abbreviation and offset
SELECT TO_CHAR(created, 'YYYY-MM-DD HH24:MI TZ OF', 'America/New_York') FROM events
-- 2026-11-01 01:30 EST -05
```

**Zone names.** The zone argument is a name. Numeric offsets are not allowed.

- A zone is an IANA name or one of its aliases: `America/New_York`, `US/Eastern`, `Asia/Kolkata`,
  `UTC`. Case does not matter. An unknown name raises `InvalidInput`, and the message quotes the name.
  A `NULL` zone gives `NULL`.
- A numeric offset such as `'+05:30'` raises `InvalidInput`. PostgreSQL reads a numeric zone as a
  POSIX zone, where `+05` is five hours west of UTC, and ISO-8601 reads `+05` as east of UTC. A zone
  name has one meaning.
- If you omit the zone, the functions use UTC. There is no session time zone: `SET TIME ZONE`,
  `AT TIME ZONE`, `timezone(zone, x)` and `CONVERT_TZ` are not supported.

**Results.** Each function uses the zone in a different way.

- `DATE_TRUNC` with a zone returns the UTC instant at which the unit starts on the local clock. The
  result is a `datetime` instant. `DATE_TRUNC('day', '2026-03-15T20:10:00Z', 'Asia/Kolkata')` is
  `2026-03-15T18:30:00Z`, which is midnight of March 16 in India.
- `DATE_PART` with a zone reads the part from the local clock. `epoch` counts the seconds of the
  instant, and the zone does not change it. An interval takes no zone.
- `TO_CHAR` with a zone prints the local clock. `TZ` and `OF` print the zone abbreviation and the
  offset in force at that instant. See [date-patterns.md](date-patterns.md).
- A `date` argument is midnight UTC of that day, as in the other date functions. For example,
  `DATE_PART('day', DATE '2026-03-15', 'America/Bogota')` is `14`: midnight UTC is 19:00 of March 14
  in Bogota.
- PostgreSQL 16 has the zone argument for `date_trunc` only. The zone argument of `date_part` and
  `to_char` is a CamusDB extension.

**Daylight saving changes.** A change of the clock affects truncation.

- When the clock moves forward, the skipped local times do not exist. A unit that would start in the
  gap starts at the instant of the change. In `America/Havana` the clock moves from 00:00 to 01:00 on
  2026-03-08. That day starts at 01:00 local time, which is `2026-03-08T05:00:00Z`.
- When the clock moves back, some local times repeat. A day, week, month or longer unit starts at the
  first occurrence of its start time. This means that a 25-hour day is one group. A shorter unit is
  truncated in the occurrence that contains the value: on 2026-11-01 in `America/New_York`, `01:30 EDT` truncates to
  `01:00 EDT` (`05:00Z`) and `01:30 EST` truncates to `01:00 EST` (`06:00Z`). Truncating to the minute
  does not move a value by an hour.
- When the clock moves back by less than the unit, the second occurrence of the unit starts at the
  change. `Australia/Lord_Howe` moves from 02:00 back to 01:30 on 2026-04-05, at `2026-04-04T15:00:00Z`.
  `01:45` before the change truncates to the hour `01:00` (`14:00Z`), and `01:45` after the change
  truncates to the change (`15:00Z`). `01:00` at the later offset does not occur.
- `DATE_TRUNC` with a zone is not later than its argument, and truncating its result again gives the
  same result.

**Zone data.** The zone rules are part of the release.

- The zone rules come from the IANA time zone database that is embedded in the NodaTime library
  shipped with CamusDB. The operating system's zone data is not used. All nodes on the same CamusDB
  version use the same rules on all platforms, including the [browser playground](browser-playground.md). A
  release that updates NodaTime can update the rules.
- A query fragment whose filter, aggregates or join predicate takes a zone argument carries the zone
  data version of the node that coordinates the statement. A node with a different version does not
  execute that fragment, and the coordinator reads those rows and evaluates the expression itself. This
  means that one statement uses one set of rules, also during a rolling upgrade to a release with new
  rules. A fragment that takes no zone argument does not carry a version, and it is executed on the
  other node as before.
- A `CHECK` constraint, a view or a stored result that uses a zone argument is computed with the rules
  of the release that evaluates it. When a country changes its rules, a new release can give a different
  answer for the dates the change covers.
- An index expression cannot take a zone argument. An index stores the computed value in its keys. New
  rules would leave keys that no longer match their rows. See
  [expression-indexes.md](expression-indexes.md).

---

## Reserved keywords

The type keywords (and their aliases) are **reserved words** and cannot be used as column or table names
in SQL. The block lists all reserved type words.

```
oid  object_id  smallint  int  int64  integer  string  bool  boolean  float32  float64  real
date  datetime  timestamp  bytes  blob  array  numeric  decimal
```

`id` is **not** a SQL type keyword and remains usable as an identifier (column/table name) —
the ObjectId type is spelled `oid`/`object_id` in SQL. `bigint` and `precision` are also usable as names.

Only the exact keyword is reserved — identifiers that merely *start with* a keyword (e.g. `internal`,
`dates`) are fine. `interval` is not reserved either. It is a keyword only right before a quote, a digit
or a parameter (`INTERVAL '1 day'`, `INTERVAL 3 HOUR`). A column named `interval` works as before. Tables created through the HTTP / programmatic API are unaffected by SQL
keyword reservation, since they bypass the SQL parser.

---

## Indexing

All scalar types are range-indexable: index keys use an order-preserving, pure-ASCII encoding so that
key order matches value order in both the in-memory and on-disk paths. `array` is the only type that
cannot be indexed.

`string` and `oid` share the same key encoding. This means that a quoted string literal in a query matches an
`oid`-typed stored key (e.g. `WHERE id = '652b...'`, where `id` is an `oid`-typed column).

A float key encodes the value in the SQL order: `-0` has the key of `0`, and all NaNs have one key
above `Infinity`. An index seek and a table scan therefore return the same rows for any float
predicate (see [Float values](#order-and-equality)).

---

## Backward compatibility

Rows are self-describing per schema version: each stored row records the schema version it was written
under and decodes against that version's column layout. Adding a column of a new type bumps the schema
version. Existing rows are decoded against their original layout, and no migration is needed. This makes adding
new-typed columns to an existing table a safe, online operation.
