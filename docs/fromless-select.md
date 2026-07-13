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
construction, so no correlation handling is needed. (Projection subqueries in a `SELECT` that
*does* have a `FROM` are a separate, not-yet-supported case.)

## Rejected shapes

| Query | Result |
|-------|--------|
| `SELECT *` | `InvalidInput` — `*` requires a `FROM` clause |
| `SELECT COUNT(*)` (or any aggregate) | `InvalidInput` — aggregates require a `FROM` clause |
| `SELECT 1 WHERE …` / `GROUP BY` / `HAVING` / `ORDER BY` | Rejected at parse time — the FROM-less grammar admits only a projection list plus optional `LIMIT`/`OFFSET` |
| `SELECT foo` (unresolved bare identifier) | Parses, then errors at execution with `UnknownColumn` (PostgreSQL-like) |

## EXPLAIN

`EXPLAIN` of a FROM-less `SELECT` renders a fixed `constant-source` → `project` shape (plus a
`limit` row when `LIMIT`/`OFFSET` is present) rather than a costed plan tree. `EXPLAIN (ANALYZE)`
is rejected — there is no table access to measure. Note that `EXPLAIN` does **not**
pre-materialize projection subqueries; they appear only as their output column names. See
[`docs/explain.md`](./explain.md).
