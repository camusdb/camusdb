# FROM-less SELECT

CamusDB supports a top-level `SELECT` with **no `FROM` clause**. It evaluates a projection
list against a single synthetic row and returns exactly one row (or zero, under `LIMIT 0` /
`OFFSET`). This covers the common ORM "probe" and utility queries — arithmetic, scalar/regex/
date functions, `CAST`, bound parameters, and existence checks — without a table.

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
> literals (escaped identifiers use backticks), so `regexp_split_to_array("a,b", ",")` and the
> single-quoted form are equivalent.

## Projection subqueries

A subquery that appears as a **projected value** in a FROM-less `SELECT` is pre-materialized
into a literal before the projection is evaluated:

- `EXISTS (…)` → a boolean.
- A scalar `(SELECT …)` → its single value (then any outer operator such as `> 0` applies).
- `IN (…)` / `NOT IN (…)` → membership against the materialized list.

Because a FROM-less `SELECT` has no outer row, **every** such subquery is uncorrelated by
construction, so no correlation handling is needed. A `SELECT` that *does* have a `FROM` folds an
uncorrelated projection subquery the same way; see
[Subqueries in expressions](./sql-expression-syntax.md#subqueries-in-expressions) for the
positions a subquery can take and the rule for correlated ones.

## WHERE

A `WHERE` decides whether the one row exists, and it follows the rule of the `WHERE` of a table. `TRUE`
keeps the row. `FALSE` and `NULL` drop it. Any other type is `InvalidInput` (`CADB0400`): `SELECT 5 WHERE 1`
fails, and so does `SELECT 5 WHERE 'true'`.

The condition can hold a subquery. When it drops the row, the projections do not run, so
`SELECT 1 / 0 WHERE FALSE` returns no rows and no error. PostgreSQL reports the division by zero there.

This form runs as a query over one row, so its plan is a `function-scan` of `*single_row*`, and
`EXPLAIN (ANALYZE)` works on it. It works as an `INSERT ... SELECT` or a `CREATE TABLE AS` source, and
as an operand of a set operation.

A sequence call that runs in this form is refused (`CADB0547`), because a statement must know how many
values it draws before it runs. `SELECT nextval('s') WHERE FALSE` returns no rows and draws nothing.

## Rejected shapes

| Query | Result |
|-------|--------|
| `SELECT *` | `InvalidInput` — `*` requires a `FROM` clause |
| `SELECT COUNT(*)` (or any aggregate) | `InvalidInput` — aggregates require a `FROM` clause |
| `SELECT COUNT(*) WHERE …` and `SELECT * WHERE …` | `InvalidInput`, the same refusals as without `WHERE` |
| `SELECT 1 GROUP BY …` / `HAVING` | Rejected at parse time — the FROM-less grammar admits only a projection list plus optional `WHERE`, `LIMIT` and `OFFSET` |
| `SELECT 1 ORDER BY …` | `InvalidInput` — `ORDER BY` requires a `FROM` clause |
| A FROM-less `SELECT` as a subquery, a derived table or a view body that is read | `InvalidInput` — `Invalid SELECT statement`, with or without `WHERE` |
| `SELECT foo` (unresolved bare identifier) | Parses, then errors at execution with `UnknownColumn` (PostgreSQL-like) |

## EXPLAIN

`EXPLAIN` of a FROM-less `SELECT` renders a fixed `constant-source` → `project` shape (plus a
`limit` row when `LIMIT`/`OFFSET` is present) rather than a costed plan tree. `EXPLAIN (ANALYZE)`
is rejected — there is no table access to measure. Note that `EXPLAIN` does **not**
pre-materialize projection subqueries; they appear only as their output column names. See
[`docs/explain.md`](./explain.md).
