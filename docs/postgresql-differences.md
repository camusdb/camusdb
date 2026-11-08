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
| `'É' ILIKE 'é'` | `TRUE` | `FALSE` | `ILIKE` uses ordinal case folding, which covers every cased letter. PostgreSQL in the C locale folds only ASCII, and a linguistic locale gives the same answer as CamusDB. |

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
| `SELECT (SELECT COUNT(*))` | `CADB0400` | `1` | A subquery, a derived table and a view body follow the rule of the statement. |
| `SELECT (SELECT 1 ORDER BY 1)` | `CADB0400` | `1` | The same reason. |
| `SELECT (SELECT nextval('s'))` | `CADB0547` | One row | A nested `LIMIT 0`, `WHERE FALSE` or discarded `EXISTS` list decides only when it runs whether the call runs. |

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
| `SELECT id FROM em WHERE s LIKE 'a' ESCAPE 'xy'` | No rows, and `CADB0400` when a row reaches it | `invalid escape string` | A value error is not a statement mistake, and the engine does not evaluate a constant at bind time. |
| `SELECT length(v) FROM em` | No rows, and `CADB0400` when a row reaches it | `function length(bigint) does not exist` | Functions have no signature table. The name and the argument count are checked at bind time, and each function checks its argument types when it runs. |
| `SELECT id FROM d WHERE EXISTS (SELECT length(v) FROM d)` | Every row | `function length(bigint) does not exist` | Functions have no signature table, so only a call shows a wrong argument type, and `EXISTS` does not evaluate its output. |

## Operator precedence

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 1 BETWEEN 0 AND 2 BETWEEN FALSE AND TRUE` | `true` | Syntax error | The form has one reading, left to right. A refusal needs grammar rules that exist only to refuse it. |
| `SELECT 1 BETWEEN 0 AND 2 IN (TRUE)` | `true` | Syntax error | The same reason. |
| `SELECT ARRAY[1, 2][1] + 1 IS NULL` | `false` | Syntax error | The subscript applies to the array constructor, the one reading of the form. PostgreSQL needs `(ARRAY[1, 2])[1]`. |

## COALESCE and CASE

`COALESCE`, `IFNULL` and `NVL` stop at the first argument that is not NULL, as in PostgreSQL. A `CASE` stops at the first `WHEN` that matches. A type that the statement shows is still checked before any row is read, for each argument and branch, as in PostgreSQL. A function call shows its declared type when the types of its arguments are known, so `COALESCE(1, upper('a'))` is refused and `COALESCE(n, sqrt(4.0))` is a `float64` in every row. A call with a parameter argument, and `json_value`, whose type comes from the document, are checked when the call returns a value. `z` has one row with `v = 0` and `n` NULL, and `d` has two rows.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COALESCE(1, upper(x)) FROM (SELECT CAST(v AS STRING) AS x FROM d) q` | `1` | Error | A function call shows its declared type only when the type of each argument is known. A derived `STRING` column, like a parameter, shows its type only through its value, so the call is checked when it returns one. PostgreSQL also refuses `COALESCE(1, upper($1))` when it prepares the statement. |
| `SELECT COALESCE(id, (SELECT 1 / b.v FROM z b)) FROM z` | `CADB0400` | Every row | A subquery runs before the row is evaluated, so the expression around it cannot stop it. |
| `SELECT COALESCE(1, (SELECT id FROM d))` | `CADB0400` | `1` | The same reason. |
| `SELECT CASE WHEN TRUE THEN 1 ELSE (SELECT 1 / b.v FROM z b) END FROM z` | `CADB0400` | `1` | The same reason. |
| `SELECT COALESCE(1, SUM(1 / v)) FROM z` | `CADB0400` | `1` | The same reason, for an aggregate. |
| `SELECT COALESCE(1, SUM(1 / v) OVER (ORDER BY id)) FROM z` | `CADB0400` | `1` | The same reason, for a window function. |
| `SELECT COALESCE(nextval('s'), nextval('s'))`, then `SELECT nextval('s')` | `3` | `2` | A statement reserves its values before it runs, and only a literal or a parameter shows its value then. `COALESCE(1, nextval('s'))` and `COALESCE(@p, nextval('s'))` with a value in `@p` draw nothing. Any other argument before the call leaves a gap. The result of the statement stays correct. |
| `SELECT COALESCE(n, nextval('s')) FROM z` | `CADB0547` in a row where `n` is NULL | A new value | A statement over a relation cannot know how many values it draws. A call that no row reaches draws nothing, so it needs no refusal. |
| `SELECT COALESCE(v, 9223372036854775807 + 1) FROM z` | The value of `v` | Error | A value error is not a statement mistake, and the engine does not evaluate a constant at bind time. Only a constant division is refused before the rows are read. |
| `SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM z` | `1` | Error | The same reason. |
| `SELECT COALESCE(v, sqrt(-1.0)) FROM z` | The value of `v` | Error | The same reason. |
| `SELECT COALESCE(v, 1 % 0) FROM z` | The value of `v` | Error | The parser builds `a % b` as `mod(a, b)`, so the rule of a function call applies. |
| `SELECT CASE WHEN id > 0 THEN 1 ELSE 9223372036854775807 + 1 END FROM z` | `1` | `bigint out of range` | A value error is not a statement mistake, and a `CASE` branch that does not run raises nothing. |
| `SELECT CASE WHEN id > 0 THEN 1 ELSE CAST('x' AS BIGINT) END FROM z` | `1` | Error | The same reason. |
| `SELECT CASE WHEN id > 0 THEN 1 ELSE sqrt(-1.0) END FROM z` | `1` | Error | The same reason. |
| `SELECT COALESCE(COALESCE(NULL, NULL), nextval('s'))` | `1` | `COALESCE types text and bigint cannot be matched` | A `NULL` literal has no type, and it takes the type that the expression needs. PostgreSQL types it as `text`. |

## NULL from a subquery

A `NULL` literal has no type, and it takes the type that the operator needs, the GoogleSQL rule. PostgreSQL gives an output column of `NULL` the type `text`, so it refuses these forms.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT (SELECT NULL) + 1` | `NULL` | `operator does not exist: text + integer` | An untyped `NULL` takes the type of the other operand. |
| `SELECT x + 1 FROM (SELECT NULL AS x) d` | `NULL` | The same error | The same reason, for a derived column. |
| `SELECT 1 IN (SELECT NULL)`, and the `NOT IN` form | `NULL` | `operator does not exist: integer = text` | The same reason. |
| `SELECT id FROM d WHERE id IN (SELECT NULL)`, and the `NOT IN` form | No rows | `operator does not exist: bigint = text` | The same reason. |
| `SELECT id FROM d WHERE (SELECT NULL)` | No rows | `argument of WHERE must be type boolean, not type text` | The same reason. |

## Derived tables and views

A derived table and a view compute every column of each of their rows before the outer query reads or filters that row. PostgreSQL computes only the columns it reads, and can filter first. So a filter outside a view cannot guard a division inside it. `t` has the rows `(1, v = 0)` and `(2, v = 2)`.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 1 FROM (SELECT v / 0 AS q FROM z) d` | `CADB0400` | One row | The column `q` is computed, although the outer query does not read it. |
| `SELECT x FROM (SELECT id, 1 / v AS x FROM t) d WHERE id = 2` | `CADB0400` | `0` | The column `x` is computed for the row with `id = 1` before the outer `WHERE` removes it. |
| `SELECT x FROM c WHERE id = 2`, where the view `c` is `SELECT id, 1 / v AS x FROM t` | `CADB0400` | `0` | The same reason, for a view. |
| `SELECT id FROM c`, where `c` is `SELECT id, (SELECT 1 / v FROM t WHERE id = 1) AS x FROM t` | `CADB0400` | Every row | The same reason, for a column that the outer query does not read. |

`CREATE VIEW` and `CREATE MATERIALIZED VIEW` bind the body and run none of its subqueries, as PostgreSQL does. So `CREATE VIEW c AS SELECT (SELECT 1 / 0) AS x` succeeds, and a read of `x` fails. A scalar subquery column has the declared type of the subquery column, also when the subquery returns no row.

## LIMIT and OFFSET

A negative `LIMIT` or `OFFSET` fails with `CADB0400` and the message of PostgreSQL, `LIMIT must not be negative`. A literal is refused when the statement binds, and a parameter when the clause is read.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `CREATE VIEW c AS SELECT id FROM d LIMIT -1` | `CADB0400` | Accepted | A negative literal is a statement mistake, so it is refused before any row is read. PostgreSQL reads the clause only when its node runs. |
| `SELECT id FROM em o WHERE EXISTS (SELECT 1 FROM em i WHERE i.id = o.id LIMIT -1)` | `CADB0400` | No rows | The same reason. No outer row reaches the body, so PostgreSQL never reads its `LIMIT`. |

## Sequences

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT nextval('s') LIMIT 0`, then `SELECT nextval('s')` | `2` | `1` | A `SELECT` with no `FROM` reserves one value for each call site before it evaluates. An unused value is a gap, which a sequence is allowed to have, and no value repeats. |
| `SELECT CASE WHEN FALSE THEN nextval('s') ELSE 1 END`, then `SELECT nextval('s')` | `2` | `1` | The same reason. |
| `SELECT COALESCE(1, setval('s', 100))` | `CADB0547` | `1` | `setval` moves a counter, so it is allowed only as a whole projection of a `SELECT` with no `FROM`. |

## EXISTS

`EXISTS` does not evaluate a select list, `GROUP BY`, `DISTINCT` or `ORDER BY`, as in PostgreSQL. An unknown name or a known type mistake in those clauses still fails. An aggregate, `HAVING`, `OFFSET` and `LIMIT 0` can change the number of rows, so they keep their meaning. A correlated `EXISTS` runs its whole body for each outer row, so these clauses keep their meaning there too. No difference from PostgreSQL is known here.

## Correlated subqueries

A name in a subquery resolves to the nearest query that has it, as in PostgreSQL, so a bare name can read the outer query. CamusDB runs a correlated `EXISTS`, scalar, `IN`, `NOT IN` and `ANY`, `SOME` or `ALL` subquery, with any body, in these clauses:

- The `WHERE` and the select list of a `SELECT`.
- The `WHERE` of an `UPDATE` or a `DELETE`.
- The `SET` values of an `UPDATE`.

A subquery of an `UPDATE` or a `DELETE` reads the rows as the statement found them, also when it reads the target table, as in PostgreSQL. A subquery in a `CASE` branch that does not run, or in a `COALESCE` argument after the first value, still runs, so it costs time, but its error does not appear, as in PostgreSQL.

A select list that has a window function runs a correlated subquery too, but a window that orders by a NULL can rank the rows in another order, see [window functions](window-functions.md).

The clauses below have no stage that runs once for each row. CamusDB refuses a correlated subquery there with `CADB0533`, and the message names the clause and the outer name, so no answer is wrong. `r` has the rows `(1, v = 0)`, `(2, 2)` and `(3, 0)`, `r2` has `(1, w = 10)` and `(2, 20)`, and `d` has `(1, v = 1)`, `(2, 2)` and `(3, 3)`.

| Statement | CamusDB | PostgreSQL |
|---|---|---|
| `SELECT v FROM r GROUP BY v HAVING (SELECT COUNT(*) FROM d WHERE d.v > r.v) > 0` | `CADB0533` | `0` and `2` |
| `SELECT a.id FROM r a JOIN d b ON b.id = (SELECT MIN(r2.id) FROM r2 WHERE r2.w > a.v * 6)` | `CADB0533` | Every row |
| `SELECT v, (SELECT COUNT(*) FROM d WHERE d.v > r.v) FROM r GROUP BY v` | `CADB0533` | `0, 3` and `2, 1` |
| `SELECT COUNT(*), (SELECT COUNT(*) FROM d WHERE d.v > MAX(r.v)) FROM r` | `CADB0533` | `3, 1` |
| `INSERT INTO r (id, v, s) VALUES (1, 5, 'z') ON CONFLICT (id) DO UPDATE SET v = (SELECT w FROM r2 WHERE r2.id = r.id)` | `CADB0533` | Sets `v` of row 1 to `10` |

## Aggregates

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COUNT(*) FROM d ORDER BY (SELECT 1)` | `CADB0400` | One row | `ORDER BY` holds no subquery in any `SELECT`. |

## Forms that CamusDB does not have yet

CamusDB refuses these forms with a clear error, so no answer is wrong. `d` has the rows `(1, 10)` and `(2, 20)`, and `em` is empty.

| Statement | CamusDB | PostgreSQL |
|---|---|---|
| `SELECT v + 1 FROM d GROUP BY v` | `CADB0400` | `11`, `21` |
| `SELECT id, v FROM d GROUP BY id`, where `id` is the primary key | `CADB0400` | Both rows |
| `SELECT v AS q FROM d GROUP BY q` | `CADB0404` | `10`, `20` |
| `SELECT v FROM d GROUP BY v ORDER BY COUNT(*), v` | `CADB0400` | `10`, `20` |
| `SELECT 1 FROM em HAVING COUNT(*) = 0` | `CADB0400` | `1` |
| `SELECT id FROM d LIMIT NULL` | `CADB0406` | Both rows |
| `SELECT d.* FROM d` | `CADB0406` | Both rows |
| `WITH c AS (SELECT 5 AS x) SELECT x FROM c` | `CADB0406` | `5` |
| `SELECT NULL IS UNKNOWN`, and `IS NOT UNKNOWN` | `CADB0406` | `true` |
| `SELECT 1 IS DISTINCT FROM 2`, and `IS NOT DISTINCT FROM` | `CADB0406` | `true` |
| `SELECT 1 ISNULL`, and `NOTNULL` | `CADB0406` | `false` |
| `SELECT COUNT(*) FILTER (WHERE v > 10) FROM d` | `CADB0406` | `1` |
| `SELECT POSITION('a' IN 'x' \|\| 'a')` | `CADB0406` | `2` |
| `CREATE TABLE dd (id INT64 PRIMARY KEY, v INT64 DEFAULT 1)` | `CADB0406`; write `DEFAULT (1)` | Accepted |
| `SELECT generate_series(1, 2) FROM d` | `CADB0533`; use `generate_series` in `FROM` | 4 rows |
| `CREATE VIEW vv AS SELECT 1` | `CADB0400`; name the column with `AS` | Accepted, with the column `?column?` |
| `SELECT x FROM (SELECT 5 AS x) d FOR UPDATE` | `CADB0400` | `5` |
| `SELECT (SELECT 1 FOR UPDATE)` | `CADB0406` | `1` |
| `INSERT INTO em (id, v) VALUES (1, 1) RETURNING (SELECT 7)` | `CADB0400` | `7` |
