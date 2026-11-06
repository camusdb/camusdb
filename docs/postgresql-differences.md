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
| `regexp_count('🙂', '.')` | `2` | `1` | The regular expression engine matches UTF-16 units and has no code point mode. Translating a pattern needs a parser for its whole syntax. A pattern that names the character matches it. |
| `regexp_like('🙂', '^.$')` | `FALSE` | `TRUE` | The same reason. |
| `'🙂' ~ '^.$'` | `FALSE` | `TRUE` | The same reason. |
| `regexp_replace('🙂', '.', 'x', 'g')` | `xx` | `x` | The same reason. |
| `cardinality(regexp_split_to_array('🙂', '.'))` | `3` | `2` | The same reason. Each element of the result is empty, so no element holds half of a pair. |
| `regexp_substr('🙂x', '.')` | `CADB0400` | `🙂` | The engine matches UTF-16 units, so the result would hold half of a pair, which has no UTF-8 form. An error is better than a damaged value. |
| `regexp_replace('🙂', '.', 'x')` | `CADB0400` | `x` | The same reason. |
| `regexp_instr('🙂x', '.', 1, 2)` | `CADB0400` | `2` | The same reason: the position is inside a pair. |

The one string order is binary, so `'a' < 'B'` is `FALSE`, and `_aa` sorts between `B` and `a`. A full
sort, a top-N, `LIMIT` with `OFFSET`, a range predicate, `MIN`, `MAX` and every index use this order, so a
plan cannot change a result. PostgreSQL in the C locale gives the same answers for ASCII text. A
linguistic locale gives different ones. Use `LOWER(v)` for a comparison that ignores case.

Text functions and a `CHAR(n)` limit count code points, as PostgreSQL does. See
[Characters and code points](data-types.md#characters-and-code-points).

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
| `SELECT COUNT(*)` | `CADB0400` | `1` | An aggregate needs rows to read. |
| `SELECT 5 ORDER BY 1` | `CADB0400` | One row | One row has no order, so the clause can only be a mistake. |
| `SELECT nextval('s') WHERE TRUE` | `CADB0547` | One row | A statement must know how many values it draws before it runs. |

See [FROM-less SELECT](fromless-select.md).

## Statement checks

CamusDB refuses a statement mistake before it reads a row, as PostgreSQL does. See
[Errors that do not depend on the rows](data-types.md#errors-that-do-not-depend-on-the-rows). These are the
differences that stay. `em` is an empty table.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT id FROM d WHERE 'true'` | `CADB0400` | Every row | A literal has its own type, and no implicit conversion from string to boolean exists. |
| `SELECT id FROM em WHERE length(s)` | No rows, and `CADB0400` when a row reaches it | `argument of WHERE must be type boolean, not type integer` | A function has no static result type in the bind-time check, so only its value shows the type. |
| `SELECT id FROM em WHERE s = length(s)` | No rows, and `CADB0400` when a row reaches it | `operator does not exist: text = integer` | A function has no static result type in the bind-time check, so only its value shows the type. |
| `SELECT id FROM em WHERE id = (SELECT upper(s) FROM em)` | No rows, and `CADB0400` when a row reaches it | `operator does not exist: bigint = text` | A subquery column has the static type of its expression, and a function has none. |
| `SELECT d.id FROM (SELECT id, s FROM em) AS d WHERE d.id = d.s` | No rows, and `CADB0400` when a row reaches it | `operator does not exist: bigint = text` | A derived table declares `string` also for a column whose type it does not know, so the check reads every derived `string` column as unknown. |
| `SELECT length('a') / 0 FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | A function call is not a constant here, because the engine has no list of deterministic functions. |
| `SELECT 1 / length('') FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | The same reason. |
| `SELECT mod(1, 0) FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | The same reason. |
| `SELECT 1 % 0 FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | The parser builds `a % b` as `mod(a, b)`, so the rule of a function call applies. |
| `SELECT CASE WHEN FALSE THEN (SELECT 1 / 0 FROM em) ELSE 1 END FROM em` | `CADB0400` | No rows | A subquery binds before the statement around it, so its check runs before anything shows that its branch is dead. Such a branch can never run, so the refusal rejects no useful statement. |
| `SELECT 9223372036854775807 + 1 FROM em` | No rows, and `CADB0420` when a row reaches it | `bigint out of range` | A value error other than a constant division by zero is not a statement mistake, and the engine does not evaluate a constant at bind time. |
| `SELECT CAST(70000 AS SMALLINT) FROM em` | No rows | `smallint out of range` | The same reason. |
| `UPDATE em SET small = 70000 WHERE FALSE` | No row written | `smallint out of range` | The declared range is a write rule, so it applies to a row that the statement writes. |
| `SELECT length(v) FROM em` | No rows, and `CADB0400` when a row reaches it | `function length(bigint) does not exist` | Functions have no signature table. The name and the argument count are checked at bind time, and each function checks its argument types when it runs. |

## Aggregates

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COUNT(*) FROM d ORDER BY (SELECT 1)` | `CADB0400` | One row | `ORDER BY` holds no subquery in any `SELECT`. |
