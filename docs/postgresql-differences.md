# Differences from PostgreSQL

CamusDB follows PostgreSQL for most SQL behavior, and GoogleSQL for its numeric types. This page lists
the places where a statement gives a different answer from PostgreSQL on purpose. Each row is a decision.
A difference that this page does not list is a defect. Please report it.

The PostgreSQL column shows what PostgreSQL 16 did in the C locale when the behavior was measured.
`CamusDB.Tests/SqlFuzz/DecidedDivergences.cs` is the same list as data. The test
`TestPostgreSqlDivergences` executes each statement. This means that a row that goes out of date will make the test fail.

## Writes

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `UPDATE d SET v = 0` | `CADB0400` | Updates all rows | A safety rule: one statement cannot change all rows by accident. Write `WHERE TRUE` to do it on purpose. |
| `DELETE FROM d` | `CADB0400` | Deletes all rows | The same safety rule. |
| `UPDATE d SET id = 3 WHERE id = 2`, where `id` is the primary key | `CADB0400` | Updates the row | Row identity, the row id caches and foreign keys depend on a key that does not change. Delete the row, and insert it with the new key. |
| `UPDATE d SET v = s.v FROM s WHERE s.id = d.id`, where two rows of `s` have `id = 1` | `CADB0400` | Updates the row one time, with one of the two values | The new value would depend on which match the statement used, which the plan decides. PostgreSQL does not say which match it uses. Make the `FROM` list match one row for each target row, for example with `GROUP BY` in a derived table. See [UPDATE … FROM and DELETE … USING](update-from-delete-using.md). |

## Strings

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `c = 'ab '`, where the `CHAR(3)` column contains `'ab'` | `FALSE` | `TRUE` | `CHAR(n)` is a maximum length, and values are not padded with blanks. Padding would change equality and each index key of the column. |
| Insert `'abc '` into `CHAR(3)` | `CADB0302` | Stores `'abc'` | A trailing blank counts toward the length. A value is not trimmed silently: an error is returned. |
| `'😀' > '�'` | `FALSE` | `TRUE` | Strings compare by UTF-16 code unit, not by UTF-8 byte. |
| `'a' COLLATE "C"`, and any other `COLLATE` clause | `CADB0406` | Accepted | Only one collation exists, and a clause that is accepted but ignored would be worse than none. |
| `regexp_count('🙂', '.')` | `2` | `1` | The regular expression engine matches UTF-16 units, and it doesn't have a code point mode. Translating a pattern would need a full parser for the pattern syntax. A pattern that names the character matches it. |
| `regexp_like('🙂', '^.$')` | `FALSE` | `TRUE` | The same reason. |
| `'🙂' ~ '^.$'` | `FALSE` | `TRUE` | The same reason. |
| `regexp_replace('🙂', '.', 'x', 'g')` | `xx` | `x` | The same reason. |
| `cardinality(regexp_split_to_array('🙂', '.'))` | `3` | `2` | The same reason. Each element of the result is empty. No element contains half of a pair. |
| `regexp_substr('🙂x', '.')` | `CADB0400` | `🙂` | The engine matches UTF-16 units. The result would contain half of a pair, which can't be encoded as UTF-8. An error is better than a damaged value. |
| `regexp_replace('🙂', '.', 'x')` | `CADB0400` | `x` | The same reason. |
| `regexp_instr('🙂x', '.', 1, 2)` | `CADB0400` | `2` | The same reason: the position is in the middle of a pair. |
| `'É' ILIKE 'é'` | `TRUE` | `FALSE` | `ILIKE` uses ordinal case folding, which covers all cased letters. PostgreSQL in the C locale folds only ASCII, and a linguistic locale gives the same answer as CamusDB. |

There is one string order, and it is binary. For example, `'a' < 'B'` is `FALSE`, and `_aa` sorts
between `B` and `a`. All operations that order strings use this order, including a full sort, a top-N,
`LIMIT` with `OFFSET`, a range predicate, `MIN`, `MAX` and indexes. This means that the plan cannot
change a result. PostgreSQL in the C locale gives the same answers for ASCII text. A
linguistic locale gives different ones. Use `LOWER(v)` for a comparison that ignores case.

Text functions and a `CHAR(n)` limit count code points, as PostgreSQL does. See
[Characters and code points](data-types.md#characters-and-code-points).

## Numbers

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 9223372036854775808` | `CADB0420` | A `numeric` value | An integer literal is `int64`, the GoogleSQL rule. `CAST('9223372036854775808' AS numeric)` writes the larger value. |
| `SELECT 1.0 + 1E-39 = 1.0` | `TRUE` | `FALSE` | A decimal literal is `float64`, the GoogleSQL rule. `NUMERIC '1.0'` writes an exact value. |
| `CAST('1E-39' AS numeric) = CAST('0' AS numeric)` | `TRUE` | `FALSE` | `numeric` has the fixed precision and scale of GoogleSQL: 38 digits, 9 after the point. |
| `SELECT log(3, 81)` | `float64` | `numeric` | Each math function that is not exact returns `float64`, as `pow` and `sqrt` do. |
| `SELECT 5 = '5'` | `CADB0400` | `TRUE` | A string and a number have no comparison, the GoogleSQL rule. A quoted literal does not take the type of the other operand. `CAST('5' AS INT64) = 5` writes the conversion. |

## SELECT with no FROM

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 5 WHERE 'true'` | `CADB0400` | One row | A quoted literal has the type `string`, and no implicit conversion from string to boolean exists. |
| `SELECT COUNT(*)` | `CADB0400` | `1` | An aggregate needs rows to read. |
| `SELECT 5 ORDER BY 1` | `CADB0400` | One row | A single row doesn't have an order. The clause can only be a mistake. |
| `SELECT nextval('s') WHERE TRUE` | `CADB0547` | One row | A statement must know how many values it draws before execution starts. |
| `SELECT (SELECT COUNT(*))` | `CADB0400` | `1` | A subquery, a derived table and a view body follow the rule of the statement. |
| `SELECT (SELECT 1 ORDER BY 1)` | `CADB0400` | `1` | The same reason. |
| `SELECT (SELECT nextval('s'))` | `CADB0547` | One row | With a nested `LIMIT 0`, `WHERE FALSE` or discarded `EXISTS` list, whether the call is executed is known only at execution time. |

See [FROM-less SELECT](fromless-select.md).

## Statement checks

A statement mistake returns an error before any row is read, as in PostgreSQL. See
[Errors that do not depend on the rows](data-types.md#errors-that-do-not-depend-on-the-rows). This table
lists the differences that remain. `em` is an empty table.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT id FROM d WHERE 'true'` | `CADB0400` | All rows | A quoted literal has the type `string`, and no implicit conversion from string to boolean exists. |
| `SELECT id FROM em WHERE length(s)` | No rows, and `CADB0400` when a row reaches it | `argument of WHERE must be type boolean, not type integer` | In the bind-time check, a function doesn't have a static result type. Only its value shows the type. |
| `SELECT id FROM em WHERE s = length(s)` | No rows, and `CADB0400` when a row reaches it | `operator does not exist: text = integer` | In the bind-time check, a function doesn't have a static result type. Only its value shows the type. |
| `SELECT id FROM em WHERE id = (SELECT upper(s) FROM em)` | No rows, and `CADB0400` when a row reaches it | `operator does not exist: bigint = text` | A subquery column has the static type of its expression, and a function has none. |
| `SELECT d.id FROM (SELECT id, s FROM em) AS d WHERE d.id = d.s` | No rows, and `CADB0400` when a row reaches it | `operator does not exist: bigint = text` | A derived table declares `string` also for a column whose type it does not know. For this reason, the check treats each derived `string` column as unknown. |
| `SELECT length('a') / 0 FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | A function call is not a constant here. The engine doesn't have a list of deterministic functions. |
| `SELECT 1 / length('') FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | The same reason. |
| `SELECT mod(1, 0) FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | The same reason. |
| `SELECT 1 % 0 FROM em` | No rows, and `CADB0400` when a row reaches it | `division by zero` | The parser builds `a % b` as `mod(a, b)`. This means that the rule of a function call applies. |
| `SELECT CASE WHEN FALSE THEN (SELECT 1 / 0 FROM em) ELSE 1 END FROM em` | `CADB0400` | No rows | A subquery binds before the statement around it. Its check is done before anything shows that its branch is dead. Such a branch can never be executed. This means that the error affects no useful statement. |
| `SELECT 9223372036854775807 + 1 FROM em` | No rows, and `CADB0420` when a row reaches it | `bigint out of range` | A value error other than a constant division by zero is not a statement mistake, and the engine does not evaluate a constant at bind time. |
| `SELECT CAST(70000 AS SMALLINT) FROM em` | No rows | `smallint out of range` | The same reason. |
| `UPDATE em SET small = 70000 WHERE FALSE` | No row written | `smallint out of range` | The declared range is a write rule. It applies to a row that the statement writes. |
| `SELECT id FROM em WHERE s LIKE 'a' ESCAPE 'xy'` | No rows, and `CADB0400` when a row reaches it | `invalid escape string` | A value error is not a statement mistake, and the engine does not evaluate a constant at bind time. |
| `SELECT length(v) FROM em` | No rows, and `CADB0400` when a row reaches it | `function length(bigint) does not exist` | Functions have no signature table. The name and the argument count are checked at bind time, and each function checks its argument types when it is called. |
| `SELECT id FROM d WHERE EXISTS (SELECT length(v) FROM d)` | All rows | `function length(bigint) does not exist` | Functions have no signature table. Only a call shows a wrong argument type, and `EXISTS` does not evaluate its output. |

## Operator precedence

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 1 BETWEEN 0 AND 2 BETWEEN FALSE AND TRUE` | `true` | Syntax error | The form has one reading, left to right. A refusal needs grammar rules that exist only to refuse it. |
| `SELECT 1 BETWEEN 0 AND 2 IN (TRUE)` | `true` | Syntax error | The same reason. |
| `SELECT ARRAY[1, 2][1] + 1 IS NULL` | `false` | Syntax error | The subscript applies to the array constructor, the one reading of the form. PostgreSQL needs `(ARRAY[1, 2])[1]`. |

## COALESCE and CASE

`COALESCE`, `IFNULL` and `NVL` stop at the first argument that is not NULL, as in PostgreSQL. A `CASE` stops at the first `WHEN` that matches. However, a type that the statement shows is checked before any row is read, for each argument and branch, as in PostgreSQL. A function call shows its declared type when the types of its arguments are known. For example, `COALESCE(1, upper('a'))` returns an error, and `COALESCE(n, sqrt(4.0))` is a `float64` in all rows. A call with a parameter argument, and `json_value`, whose type comes from the document, are checked when the call returns a value. `z` has one row with `v = 0` and `n` NULL, and `d` has two rows.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COALESCE(1, upper(x)) FROM (SELECT CAST(v AS STRING) AS x FROM d) q` | `1` | Error | A function call shows its declared type only when the type of each argument is known. A derived `STRING` column, like a parameter, shows its type only through its value. The call is checked when it returns a value. PostgreSQL also returns an error for `COALESCE(1, upper($1))` when it prepares the statement. |
| `SELECT COALESCE(id, (SELECT 1 / b.v FROM z b)) FROM z` | `CADB0400` | All rows | A subquery is executed before the row is evaluated. The expression around it cannot stop it. |
| `SELECT COALESCE(1, (SELECT id FROM d))` | `CADB0400` | `1` | The same reason. |
| `SELECT CASE WHEN TRUE THEN 1 ELSE (SELECT 1 / b.v FROM z b) END FROM z` | `CADB0400` | `1` | The same reason. |
| `SELECT COALESCE(1, SUM(1 / v)) FROM z` | `CADB0400` | `1` | The same reason, for an aggregate. |
| `SELECT COALESCE(1, SUM(1 / v) OVER (ORDER BY id)) FROM z` | `CADB0400` | `1` | The same reason, for a window function. |
| `SELECT COALESCE(nextval('s'), nextval('s'))`, then `SELECT nextval('s')` | `3` | `2` | A statement reserves its values before execution starts, and at that point only a literal or a parameter shows its value. `COALESCE(1, nextval('s'))` and `COALESCE(@p, nextval('s'))` with a value in `@p` don't draw a value. Any other argument before the call leaves a gap. The result of the statement is correct. |
| `SELECT COALESCE(n, nextval('s')) FROM z` | `CADB0547` in a row where `n` is NULL | A new value | A statement over a relation cannot know how many values it draws. A call that no row reaches doesn't draw a value, and it doesn't need an error. |
| `SELECT COALESCE(v, 9223372036854775807 + 1) FROM z` | The value of `v` | Error | A value error is not a statement mistake, and the engine does not evaluate a constant at bind time. Only a constant division is refused before the rows are read. |
| `SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM z` | `1` | Error | The same reason. |
| `SELECT COALESCE(v, sqrt(-1.0)) FROM z` | The value of `v` | Error | The same reason. |
| `SELECT COALESCE(v, 1 % 0) FROM z` | The value of `v` | Error | The parser builds `a % b` as `mod(a, b)`. This means that the rule of a function call applies. |
| `SELECT CASE WHEN id > 0 THEN 1 ELSE 9223372036854775807 + 1 END FROM z` | `1` | `bigint out of range` | A value error is not a statement mistake, and a `CASE` branch that is not executed doesn't raise an error. |
| `SELECT CASE WHEN id > 0 THEN 1 ELSE CAST('x' AS BIGINT) END FROM z` | `1` | Error | The same reason. |
| `SELECT CASE WHEN id > 0 THEN 1 ELSE sqrt(-1.0) END FROM z` | `1` | Error | The same reason. |
| `SELECT COALESCE(COALESCE(NULL, NULL), nextval('s'))` | `1` | `COALESCE types text and bigint cannot be matched` | A `NULL` literal is untyped, and it takes the type that the expression needs. PostgreSQL types it as `text`. |

## NULL from a subquery

A `NULL` literal is untyped, and it takes the type that the operator needs, the GoogleSQL rule. PostgreSQL gives an output column of `NULL` the type `text`, and returns an error for these forms.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT (SELECT NULL) + 1` | `NULL` | `operator does not exist: text + integer` | An untyped `NULL` takes the type of the other operand. |
| `SELECT x + 1 FROM (SELECT NULL AS x) d` | `NULL` | The same error | The same reason, for a derived column. |
| `SELECT 1 IN (SELECT NULL)`, and the `NOT IN` form | `NULL` | `operator does not exist: integer = text` | The same reason. |
| `SELECT id FROM d WHERE id IN (SELECT NULL)`, and the `NOT IN` form | No rows | `operator does not exist: bigint = text` | The same reason. |
| `SELECT id FROM d WHERE (SELECT NULL)` | No rows | `argument of WHERE must be type boolean, not type text` | The same reason. |

## Derived tables and views

A derived table and a view compute all columns of each row before the outer query reads or filters that row. PostgreSQL computes only the columns it reads, and can filter first. This means that in CamusDB, a filter outside a view cannot guard a division in the view body. `t` has the rows `(1, v = 0)` and `(2, v = 2)`.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT 1 FROM (SELECT v / 0 AS q FROM z) d` | `CADB0400` | One row | The column `q` is computed, although the outer query does not read it. |
| `SELECT x FROM (SELECT id, 1 / v AS x FROM t) d WHERE id = 2` | `CADB0400` | `0` | The column `x` is computed for the row with `id = 1` before the outer `WHERE` removes it. |
| `SELECT x FROM c WHERE id = 2`, where the view `c` is `SELECT id, 1 / v AS x FROM t` | `CADB0400` | `0` | The same reason, for a view. |
| `SELECT id FROM c`, where `c` is `SELECT id, (SELECT 1 / v FROM t WHERE id = 1) AS x FROM t` | `CADB0400` | All rows | The same reason, for a column that the outer query does not read. |

`CREATE VIEW` and `CREATE MATERIALIZED VIEW` bind the body and run none of its subqueries, as PostgreSQL does. So `CREATE VIEW c AS SELECT (SELECT 1 / 0) AS x` succeeds, and a read of `x` fails. A scalar subquery column has the declared type of the subquery column, also when the subquery returns no row.

## WITH queries

`d` has the rows `(1, 10)` and `(2, 20)`. See [WITH queries](common-table-expressions.md).

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `WITH c AS (SELECT id FROM nowhere) SELECT v FROM d WHERE id = 1` | `10` | `relation "nowhere" does not exist` | The parser writes out each CTE where the statement reads it. A CTE that the statement doesn't read is not checked. If the statement reads it, the error names the unknown table. |
| `SELECT same FROM (WITH r AS (SELECT random() AS x) SELECT a.x = b.x AS same FROM r a, r b) q` | `false` | `true` | A `WITH` in parentheses can read the columns of the query around it. For this reason, each reader computes a separate copy. A `WITH` at the start of the statement computes a CTE that it reads two or more times one time, as PostgreSQL does. |
| `WITH r AS NOT MATERIALIZED (SELECT random() AS x) SELECT a.x = b.x AS same FROM r a, r b` | `false` | `true` | `NOT MATERIALIZED` always inlines. PostgreSQL computes a CTE that calls a volatile function one time, whatever the hint says. |
| `WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 20000) SELECT COUNT(*) FROM r` | `CADB0560` | `20000` | By default, a statement doesn't have a time limit. A recursion that doesn't end would continue until the disk is full. The cluster settings `max_recursive_cte_iterations` (10,000 steps) and `max_recursive_cte_rows` (10,000,000 rows) stop it. Both can be raised, and `0` turns a limit off. |
| `WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT a.n + b.n FROM (SELECT n FROM r LIMIT 1) a, (SELECT n FROM r LIMIT 1) b` | `CADB0560` | `2` | A CTE that the statement reads two times is computed one time, and that computation makes all rows before a reader gets one. PostgreSQL gives each reader rows when it asks for them. A recursive CTE that the statement reads one time streams. A `LIMIT` over it stops it, as in PostgreSQL. |

A view stores its body with each CTE written out as a derived table. For this reason, `SHOW CREATE VIEW`
doesn't show a `WITH` at the start, where PostgreSQL shows the `WITH` that the body had. A recursive CTE is shown
where it is read, as `(WITH RECURSIVE name AS (…) SELECT * FROM name) AS alias`.

## LATERAL

`d` has the rows `(1, 10)` and `(2, 20)`. See [Joins](joins.md#lateral).

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT a.id FROM d a RIGHT JOIN LATERAL (SELECT 1 AS w) s ON true ORDER BY a.id` | `CADB0400` | `1`, `2` | A `RIGHT JOIN` returns all rows of its right side. For a right row with no match, a `LATERAL` right side that reads the left side has no left row to read. PostgreSQL returns an error for that form only when the right side reads the left side. CamusDB returns an error for the keyword itself. This is because a `RIGHT JOIN` is executed as a `LEFT JOIN` with its sides swapped. When the derived table reads no left item, remove `LATERAL`. |

The T-SQL forms `CROSS APPLY` and `OUTER APPLY` are not supported. Write `CROSS JOIN LATERAL (…)` and
`LEFT JOIN LATERAL (…) ON true`.

## Functions in FROM

See [set-returning functions](set-returning-functions.md#how-it-runs).

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COUNT(*) FROM generate_series(1, 5000001)` | `CADB0561` | `5000001` | By default, a statement doesn't have a time limit. One short statement over a series of a trillion rows would take hours. The cluster setting `max_table_function_rows` (5,000,000 rows) stops one read of a function in `FROM`. A read that a `LIMIT` stops early doesn't reach the limit. The setting can be raised, and `0` turns the limit off. |

## Relations in one statement

See [Tables per statement](request-resource-limits.md#tables-per-statement).

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COUNT(*) FROM generate_series(1, 1) g1, generate_series(1, 1) g2, …` with 65 functions | `CADB0564` | `1` | Planning time grows with the square of the relations in one join, and a statement can hold many joins. With the cluster setting `max_tables_per_statement` (64 relations), a statement that reads more returns an error before it is planned. Each join, subquery, derived table, CTE and view body counts. The setting can be raised, and `0` turns the limit off. |

## NULL in ORDER BY

NULL sorts as the smallest value, the GoogleSQL rule: it comes first in ascending order and last in
descending order. PostgreSQL sorts NULL as the largest value, which gives the opposite order. The rule is the
same for all ordered results: a sort, a top-k, `DISTINCT`, `GROUP BY`, a set operation and a window
`ORDER BY`. Index keys are stored in the same order. This means that an `ORDER BY` that an index supplies
doesn't need a sort. `NULLS FIRST` and `NULLS LAST` are not supported. `nl` has the rows `(1, 10)` and `(2, NULL)`.

| Statement | CamusDB | PostgreSQL |
|---|---|---|
| `SELECT v FROM nl ORDER BY v` | `NULL`, `10` | `10`, `NULL` |
| `SELECT v FROM nl ORDER BY v DESC` | `10`, `NULL` | `NULL`, `10` |

## LIMIT and OFFSET

A negative `LIMIT` or `OFFSET` returns `CADB0400` and the message of PostgreSQL, `LIMIT must not be negative`. For a literal, the error is returned when the statement binds. For a parameter, it is returned when the clause is read.

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `CREATE VIEW c AS SELECT id FROM d LIMIT -1` | `CADB0400` | Accepted | A negative literal is a statement mistake. It returns an error before any row is read. PostgreSQL doesn't read the clause until its node is executed. |
| `SELECT id FROM em o WHERE EXISTS (SELECT 1 FROM em i WHERE i.id = o.id LIMIT -1)` | `CADB0400` | No rows | The same reason. No outer row reaches the body. This means that PostgreSQL doesn't read its `LIMIT`. |

## Sequences

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT nextval('s') LIMIT 0`, then `SELECT nextval('s')` | `2` | `1` | A FROM-less `SELECT` reserves one value for each call site before it evaluates. An unused value is a gap, which a sequence is allowed to have, and no value repeats. |
| `SELECT CASE WHEN FALSE THEN nextval('s') ELSE 1 END`, then `SELECT nextval('s')` | `2` | `1` | The same reason. |
| `SELECT COALESCE(1, setval('s', 100))` | `CADB0547` | `1` | `setval` moves a counter. It is allowed only as a top-level select-list item of a FROM-less `SELECT`. |

## EXISTS

`EXISTS` does not evaluate a select list, `GROUP BY`, `DISTINCT` or `ORDER BY`, as in PostgreSQL. However, an unknown name or a known type mistake in those clauses returns an error. An aggregate, `HAVING`, `OFFSET` and `LIMIT 0` can change the number of rows. For this reason, they keep their meaning. A correlated `EXISTS` evaluates its body for each outer row, and these clauses keep their meaning there too. No difference from PostgreSQL is known here.

## Correlated subqueries

A name in a subquery resolves to the nearest query that has it, as in PostgreSQL. This means that a bare name can read the outer query. A correlated `EXISTS`, scalar, `IN`, `NOT IN` and `ANY`, `SOME` or `ALL` subquery, with any body, is supported in the clauses in this list.

- The `WHERE` and the select list of a `SELECT`.
- The `WHERE` of an `UPDATE` or a `DELETE`.
- The `SET` values of an `UPDATE`.

A subquery of an `UPDATE` or a `DELETE` reads the rows as the statement found them, also when it reads the target table, as in PostgreSQL. A subquery in a `CASE` branch that is not taken, or in a `COALESCE` argument after the first value, is executed too. This costs time, but its error does not appear, as in PostgreSQL.

A correlated subquery is also supported in a select list that has a window function. However, a window that orders by a NULL can rank the rows in another order, see [window functions](window-functions.md).

The clauses below have no stage that is executed once for each row. A correlated subquery there returns `CADB0533`, and the message names the clause and the outer name. This means that no answer is wrong. `r` has the rows `(1, v = 0)`, `(2, 2)` and `(3, 0)`, `r2` has `(1, w = 10)` and `(2, 20)`, and `d` has `(1, v = 1)`, `(2, 2)` and `(3, 3)`.

| Statement | CamusDB | PostgreSQL |
|---|---|---|
| `SELECT v FROM r GROUP BY v HAVING (SELECT COUNT(*) FROM d WHERE d.v > r.v) > 0` | `CADB0533` | `0` and `2` |
| `SELECT a.id FROM r a JOIN d b ON b.id = (SELECT MIN(r2.id) FROM r2 WHERE r2.w > a.v * 6)` | `CADB0533` | All rows |
| `SELECT v, (SELECT COUNT(*) FROM d WHERE d.v > r.v) FROM r GROUP BY v` | `CADB0533` | `0, 3` and `2, 1` |
| `SELECT COUNT(*), (SELECT COUNT(*) FROM d WHERE d.v > MAX(r.v)) FROM r` | `CADB0533` | `3, 1` |
| `INSERT INTO r (id, v, s) VALUES (1, 5, 'z') ON CONFLICT (id) DO UPDATE SET v = (SELECT w FROM r2 WHERE r2.id = r.id)` | `CADB0533` | Sets `v` of row 1 to `10` |

## Aggregates

| Statement | CamusDB | PostgreSQL | Why |
|---|---|---|---|
| `SELECT COUNT(*) FROM d ORDER BY (SELECT 1)` | `CADB0400` | One row | A subquery is not allowed in `ORDER BY` in any `SELECT`. |

## Names of expression columns

An unaliased expression column takes its position in the select list as its name, starting at
`0`. So `SELECT MAX(v) FROM d` has the column `0`. PostgreSQL takes the name of the function, such as
`max`. For other expressions it uses `?column?`. A scalar subquery column takes the name of its inner column, as in PostgreSQL, but
over an expression or a `*` it has its position as its name too. PostgreSQL names a `*` of one column
after that column. Write `AS` to give a column a name. `d` has the rows `(1, 10)` and `(2, 20)`.

| Statement | CamusDB | PostgreSQL |
|---|---|---|
| `SELECT max FROM (SELECT MAX(v) FROM d) q` | `CADB0404` | `20` |
| `SELECT abs FROM (SELECT abs(v) FROM d) q ORDER BY 1` | `CADB0404` | `10`, `20` |
| `SELECT id, (SELECT MAX(v) FROM d) FROM d ORDER BY max, id` | `CADB0404` | Both rows |
| `SELECT v FROM (SELECT (SELECT * FROM (SELECT v FROM d WHERE id = 1) s)) q` | `CADB0404` | `10` |

## Forms that CamusDB does not have yet

These forms return a clear error. This means that no answer is wrong. `d` has the rows `(1, 10)` and `(2, 20)`, and `em` is empty.

| Statement | CamusDB | PostgreSQL |
|---|---|---|
| `SELECT v + 1 FROM d GROUP BY v` | `CADB0400` | `11`, `21` |
| `SELECT id, v FROM d GROUP BY id`, where `id` is the primary key | `CADB0400` | Both rows |
| `SELECT * FROM d GROUP BY 1`, where `id` is the primary key | `CADB0400` | Both rows |
| `SELECT v AS q FROM d GROUP BY q` | `CADB0404` | `10`, `20` |
| `SELECT id, (SELECT 5) FROM d GROUP BY id` | `CADB0400` | Both rows |
| `SELECT v FROM d GROUP BY v ORDER BY COUNT(*), v` | `CADB0400` | `10`, `20` |
| `SELECT 1 FROM em HAVING COUNT(*) = 0` | `CADB0400` | `1` |
| `SELECT id FROM d LIMIT NULL` | `CADB0406` | Both rows |
| `SELECT d.* FROM d` | `CADB0406` | Both rows |
| `SELECT * FROM d a JOIN d b USING (id)` | `CADB0406` | Both rows, with one `id` column |
| `SELECT * FROM d a NATURAL JOIN d b` | `CADB0406` | Both rows, with one column for each common name |
| `SELECT * FROM d a FULL JOIN d b ON a.id = b.id` | `CADB0533`; use `LEFT JOIN` or `RIGHT JOIN` | Both rows |
| `WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SEARCH DEPTH FIRST BY n SET ord SELECT n FROM r ORDER BY ord`, and `CYCLE` | `CADB0406` | `1`, `2`, `3` |
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
| `UPDATE d SET v = 0 FROM d e WHERE e.id = d.id AND d.id = 1 RETURNING e.v` | `CADB0400`; `RETURNING` reads the target only | `10` |
| `UPDATE d SET v = 0 FROM d e WHERE e.id = d.id AND d.id = 1 RETURNING *` | `CADB0400`; list the target columns | The columns of `d` and of `e` |
| `WITH c (n) AS (VALUES (1), (2)) SELECT SUM(n) FROM c`, and a top-level `VALUES (1), (2)` | `CADB0406`; write `SELECT * FROM (VALUES (1), (2)) AS v (n)` | `3` |
| `INSERT INTO em (id, v) VALUES (1, 1) RETURNING (SELECT 7)` | `CADB0400` | `7` |
