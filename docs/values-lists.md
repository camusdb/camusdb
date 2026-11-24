# VALUES lists in FROM

A `VALUES` list is a relation of constant rows. It can appear in any place where a table can appear in
`FROM`. Use it to join a small list of constants, to apply a batch of changes with `UPDATE … FROM`, or,
after `LATERAL`, to split one row into several rows. The rules are the rules of PostgreSQL.

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v (id, k);

SELECT t.id, v.label
  FROM t JOIN (VALUES (1, 'one'), (3, 'three')) AS v (id, label) ON v.id = t.id;

UPDATE input_state s
   SET down = v.d
  FROM (VALUES ('forward', true), ('back', false)) AS v (k, d)
 WHERE v.k = s.k;

SELECT t.id, s.name, s.value
  FROM t CROSS JOIN LATERAL (VALUES ('a', t.a), ('b', t.b)) AS s (name, value);
```

## Names

The alias is required, as in PostgreSQL 16. If you omit the column list, the columns are named `column1`
and `column2` onward. A column list names the first columns. When the list is shorter than the row, the
later columns get the default names. A list with more names than columns returns an error.

```sql
SELECT * FROM (VALUES (1, 'a')) AS v;           -- columns column1, column2
SELECT * FROM (VALUES (1, 'a')) AS v (id);      -- columns id, column2
```

## Rows and items

- All rows must have the same number of items. Otherwise the statement returns the error
  `VALUES lists must all be the same length`.
- An item is an expression: a literal, a parameter, a function call, an operator, a `CASE`, or a scalar
  subquery.
- An item cannot be `DEFAULT`, an aggregate call such as `COUNT(*)`, or a window call. The aggregate
  check knows the built-in aggregates. An aggregate of a C# extension is allowed. It is evaluated over
  the one row of its item.
- The rows come in the written order. As for any relation, only `ORDER BY` fixes the order of a
  result.

## Column types

A column type follows the rule of `UNION` ([set operations](set-operations.md#columns)), as in
PostgreSQL. `(VALUES (1), (2.5))` gives a `float64` column. `(VALUES (1), ('x'))` is refused with
`CADB0400`. The message names `UNION ALL`. This is because the list is executed as a `UNION ALL` (see
[How it runs](#how-it-runs)). A column whose items are all `NULL` is untyped, as in a `UNION` of `NULL`s.

## LATERAL

A `VALUES` list can read the columns of the items to its left after `LATERAL`, as in PostgreSQL.

```sql
SELECT i.id, s.k, s.n
  FROM items i CROSS JOIN LATERAL (VALUES ('id', i.id), ('v', i.v)) AS s (k, n);
```

The list is then evaluated once for each left row (see [LATERAL](joins.md#lateral)). If `LATERAL` is
omitted, a name of a left item is unknown. A `VALUES` list in a subquery can read the columns of the
outer query. Those columns belong to a different `FROM`. This means that `LATERAL` is not needed for them.

## Where a VALUES list can appear

- `FROM` and any join form of a `SELECT`. This includes the `SELECT` of a derived table or a CTE body. The
  same is true for subqueries and view bodies.
- `UPDATE … FROM` and `DELETE … USING` ([UPDATE … FROM and DELETE … USING](update-from-delete-using.md)).
- The source query of `INSERT … SELECT` and `CREATE TABLE … AS`.

The `VALUES` text is stored with the view. `SHOW CREATE VIEW` writes the list back, and that text parses
again to the same text.

## How it runs

The parser writes the list as a derived table over a `UNION ALL` of FROM-less `SELECT`s, one for each
row. This means that the list uses the join planner, the column list, and the `LATERAL` code of a derived table, and
its column types follow the `UNION` rule. A long list is rebalanced like any long `UNION ALL` chain
([long chains](set-operations.md#long-chains)). This means that 10,000 rows do not reach the
[nesting limit](statement-nesting-limit.md).

The cost is about 40 µs for each row on a laptop, measured on 1,000 rows. A list of thousands of rows
works, but a table is faster for a large batch.

A `VALUES` list reads no table. It takes no lock and needs no privilege. It also adds no dependency to
the [query result cache](query-result-cache.md).

## EXPLAIN

A `VALUES` list that is the only source of a query shows as `values(alias=v, rows=N)`. On the right
side of a join, a `VALUES` list does not get a separate line. The join line names the other side. A
derived table that executes a separate query shows that query there (see
[Derived tables and CTEs](explain.md#derived-tables-and-ctes)).

## Not supported

- A `VALUES` statement at the top level (`VALUES (1), (2);`), and a CTE or subquery whose body is only a
  `VALUES` list (`WITH c AS (VALUES (1))`). Write `SELECT * FROM (VALUES …) AS v` instead.
- `ORDER BY`, `LIMIT` and `OFFSET` in the parentheses of a `VALUES` list.
