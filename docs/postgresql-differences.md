# Differences from PostgreSQL

CamusDB follows PostgreSQL for most SQL behavior, and GoogleSQL for its numeric types. This page lists
the places where a statement gives a different answer from PostgreSQL on purpose. Each row is a decision.
A difference that this page does not list is a defect, so please report it.

The PostgreSQL column shows what PostgreSQL 16 did in the C locale when the behavior was measured.
`CamusDB.Tests/SqlFuzz/DecidedDivergences.cs` is the same list as data, and the test
`TestPostgreSqlDivergences` runs each statement, so a row cannot go out of date without a test failure.

## Writes

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `UPDATE d SET v = 0` | `CADB0400` | Updates every row | A safety rule: one statement cannot change every row by accident. Write `WHERE TRUE` to do it on purpose. |
| `DELETE FROM d` | `CADB0400` | Deletes every row | The same safety rule. |
| `UPDATE d SET id = 3 WHERE id = 2`, where `id` is the primary key | `CADB0400` | Updates the row | Row identity, the row id caches and foreign keys depend on a key that does not change. Delete the row, and insert it with the new key. |

## Strings

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `c = 'ab '`, where the `CHAR(3)` column holds `'ab'` | `FALSE` | `TRUE` | `CHAR(n)` is a maximum length with no blank padding. Padding would change equality and every index key of the column. |
| Insert `'abc '` into `CHAR(3)` | `CADB0302` | Stores `'abc'` | A trailing blank counts toward the length, because nothing is trimmed from a value without an error. |
| `'😀' > '�'` | `FALSE` | `TRUE` | Strings compare by UTF-16 code unit, not by UTF-8 byte. |
| `'a' COLLATE "C"`, and any other `COLLATE` clause | `CADB0406` | Accepted | Only one collation exists, and a clause that is accepted but ignored would be worse than none. |

The one string order is binary, so `'a' < 'B'` is `FALSE`, and `_aa` sorts between `B` and `a`. A full
sort, a top-N, `LIMIT` with `OFFSET`, a range predicate, `MIN`, `MAX` and every index use this order, so a
plan cannot change a result. PostgreSQL in the C locale gives the same answers for ASCII text. A
linguistic locale gives different ones. Use `LOWER(v)` for a comparison that ignores case.

## Numbers

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 9223372036854775808` | `CADB0420` | A `numeric` value | An integer literal is `int64`, the GoogleSQL rule. `CAST('9223372036854775808' AS numeric)` writes the larger value. |
| `SELECT 1.0 + 1E-39 = 1.0` | `TRUE` | `FALSE` | A decimal literal is `float64`, the GoogleSQL rule. `NUMERIC '1.0'` writes an exact value. |
| `CAST('1E-39' AS numeric) = CAST('0' AS numeric)` | `TRUE` | `FALSE` | `numeric` has the fixed precision and scale of GoogleSQL: 38 digits, 9 after the point. |
| `SELECT log(3, 81)` | `float64` | `numeric` | Every math function that is not exact returns `float64`, as `pow` and `sqrt` do. |
| `SELECT 5 = '5'` | `CADB0400` | `TRUE` | A string and a number have no comparison, the GoogleSQL rule. A quoted literal does not take the type of the other operand. `CAST('5' AS INT64) = 5` writes the conversion. |

## SELECT with no FROM

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 5 WHERE 'true'` | `CADB0400` | One row | A literal has its own type, and no implicit conversion from string to boolean exists. |
| `SELECT 1 / 0 WHERE FALSE` | No rows | Division by zero | A value that nothing reads is not computed. |
| `SELECT COUNT(*)` | `CADB0400` | `1` | An aggregate needs rows to read. |
| `SELECT 5 ORDER BY 1` | `CADB0400` | One row | One row has no order, so the clause can only be a mistake. |
| `SELECT nextval('s') WHERE TRUE` | `CADB0547` | One row | A statement must know how many values it draws before it runs. |

See [FROM-less SELECT](fromless-select.md).

## Aggregates

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COUNT(*) FROM d ORDER BY (SELECT 1)` | `CADB0400` | One row | `ORDER BY` holds no subquery in any `SELECT`. |
