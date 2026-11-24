# FROM-less SELECT

A `SELECT` can omit the **`FROM` clause**, both as a statement and as part of a larger statement.
The projection list is evaluated against a single synthetic row, and the query returns exactly one
row (or zero, under `LIMIT 0` / `OFFSET`). This covers the common ORM "probe" and utility queries
that don't need a table, such as function calls and existence checks.

## Supported forms

```sql
-- Scalar / function expressions (one column per projection)
SELECT 1 + 1;
SELECT upper('abc');
SELECT regexp_split_to_array('one,two,three', ',');
SELECT CAST('5' AS int64);
SELECT @param;                          -- bound parameter

-- Column naming and multiple projections
SELECT 41 + 1 AS answer;                -- aliased
SELECT 1, 'a', 2 + 3;                   -- ordinal names "0", "1", "2"

-- Row gating with LIMIT / OFFSET (the row count is 0 or 1)
SELECT 1 LIMIT 0;                       -- returns no rows
SELECT 1 LIMIT 1 OFFSET 1;              -- OFFSET past the only row → no rows

-- Row gating with WHERE
SELECT 5 WHERE TRUE;                    -- one row
SELECT 5 WHERE FALSE;                   -- no rows
SELECT 5 WHERE NULL;                    -- no rows
SELECT 'seed' WHERE NOT EXISTS (SELECT 1 FROM accounts);

-- Existence checks via uncorrelated projection subqueries
SELECT EXISTS (SELECT 1 FROM accounts WHERE email = @email);
SELECT (SELECT COUNT(*) FROM (
    SELECT 1 FROM accounts WHERE email = @email) AS _e) > 0;
```

> **String literals:** CamusDB's lexer treats **both** single- and double-quoted text as string
> literals. Escaped identifiers use backticks. This means that `regexp_split_to_array("a,b", ",")`
> and the single-quoted form are equivalent.

## Projection subqueries

A subquery that appears as a **projected value** in a FROM-less `SELECT` is pre-materialized
into a literal before the projection is evaluated. The kind of literal depends on the subquery form.

- `EXISTS (…)` → a boolean.
- A scalar `(SELECT …)` → its single value (then any outer operator such as `> 0` applies).
- `IN (…)` / `NOT IN (…)` → membership against the materialized list.

A FROM-less `SELECT` has no outer row. This means that **all** of these subqueries are uncorrelated by construction and need no correlation handling.
A `SELECT` that *does* have a `FROM` folds an uncorrelated projection subquery the same way. For the
positions a subquery can take and the rule for correlated ones, see
[Subqueries in expressions](./sql-expression-syntax.md#subqueries-in-expressions).

## WHERE

A `WHERE` decides whether the one row exists, and it follows the rule of the `WHERE` of a table. When the
condition is `TRUE`, the row is returned. `FALSE` and `NULL` drop it. Any other type is `InvalidInput` (`CADB0400`): `SELECT 5 WHERE 1`
fails, and so does `SELECT 5 WHERE 'true'`.

The condition can contain a subquery. When the condition drops the row, the projections are not evaluated.
However, a division by zero over constants is an error whether or not the row is dropped:
`SELECT 1 / 0 WHERE FALSE` returns `CADB0400`, as it does in PostgreSQL. For more information, see
[Errors that do not depend on the rows](data-types.md#errors-that-do-not-depend-on-the-rows).

This form is executed as a query over one row. Its plan is a `function-scan` of `*single_row*`, and
`EXPLAIN (ANALYZE)` works on it. It works as an `INSERT ... SELECT` or a `CREATE TABLE AS` source, and
as an operand of a set operation.

A sequence call in this form is not allowed (`CADB0547`). A statement must know how many values it draws
before execution starts. `SELECT nextval('s') WHERE FALSE` returns no rows and does not draw a value.

## Inside a statement

A FROM-less `SELECT` works as a subquery, as a derived table and as a stored view body. It is evaluated
over one row, and `WHERE`, `LIMIT` and `OFFSET` can remove that row.

```sql
SELECT (SELECT 1);                                     -- 1
SELECT (SELECT 1 WHERE FALSE);                         -- NULL
SELECT id FROM t WHERE id IN (SELECT 1);
SELECT t.id, d.x FROM t JOIN (SELECT 1 AS x) d ON t.id = d.x;
UPDATE t SET v = (SELECT 9) WHERE id = 1;
CREATE VIEW five AS SELECT 5 AS x;
```

`CREATE VIEW` binds the body and does not evaluate it. This means that `CREATE VIEW v AS SELECT 1 / 0 AS x`
succeeds and a read of `v` fails, as in PostgreSQL. A subquery in the body is also bound and not evaluated,
which means that `CREATE VIEW v AS SELECT (SELECT 1 / 0) AS x` succeeds too. However, an unknown name in a
subquery makes the `CREATE VIEW` fail.

The rejected shapes below apply in each of these positions. A sequence call is also not allowed there
(`CADB0547`). As in the `WHERE` form, a statement must know how many values it draws before execution starts.

A correlated FROM-less subquery reads a column of the statement around it, as in PostgreSQL:
`SELECT x.id FROM t x WHERE EXISTS (SELECT 1 WHERE x.id = 1)` returns `1`, and so does
`SELECT id FROM t WHERE (SELECT v) = 10` when only the row with `id = 1` has `v = 10`. The scalar, `IN` and
`EXISTS` forms are all evaluated for each outer row. For more information, see
[Subqueries in expressions](sql-expression-syntax.md#subqueries-in-expressions).

## Rejected shapes

| Query | Result |
|-------|--------|
| `SELECT *` | `InvalidInput` — `*` requires a `FROM` clause |
| `SELECT COUNT(*)` (or any aggregate) | `InvalidInput` — aggregates require a `FROM` clause |
| `SELECT COUNT(*) WHERE …` and `SELECT * WHERE …` | `InvalidInput`, the same errors as the two rows above |
| `SELECT 1 GROUP BY …` / `HAVING` | Rejected at parse time — the FROM-less grammar admits only a projection list plus optional `WHERE`, `LIMIT` and `OFFSET` |
| `SELECT 1 ORDER BY …` | `InvalidInput` — `ORDER BY` requires a `FROM` clause |
| `SELECT foo` (unresolved bare identifier) | Parses, then errors at execution with `UnknownColumn` (PostgreSQL-like) |

## EXPLAIN

A FROM-less `SELECT` with a `WHERE` clause is planned as a query over one row, as described in
[WHERE](#where). For the other FROM-less forms, `EXPLAIN` renders a fixed, uncosted `constant-source` → `project` shape (plus a
`limit` row when `LIMIT`/`OFFSET` is present). `EXPLAIN (ANALYZE)`
is rejected — there is no table access to measure. `EXPLAIN` does **not**
pre-materialize projection subqueries; they appear only as their output column names. See
[`docs/explain.md`](./explain.md).
