# Joins

CamusDB supports four join forms in the `FROM` clause of a `SELECT`, and `LATERAL` derived tables
(see [LATERAL](#lateral)):

| Form | Syntax | Rows returned |
|------|--------|---------------|
| Inner | `A JOIN B ON p` or `A INNER JOIN B ON p` | Every pair `(a, b)` for which `p` is true. |
| Left outer | `A LEFT JOIN B ON p` or `A LEFT OUTER JOIN B ON p` | Every inner-join pair, plus one row for each `a` that has no pair, with every `B` column set to NULL. |
| Right outer | `A RIGHT JOIN B ON p` or `A RIGHT OUTER JOIN B ON p` | The same rows as `B LEFT JOIN A ON p`. |
| Cross | `A CROSS JOIN B` | Every pair `(a, b)`. No `ON` clause is allowed. |

Joins chain left to right, and a chain may mix the forms:

```sql
SELECT a.id, b.id, c.id, d.id
FROM a
JOIN b ON b.a_id = a.id
LEFT JOIN c ON c.b_id = b.id
JOIN d ON d.a_id = a.id;
```

A set-returning function such as `generate_series` may also stand on either side of any join, as
a relation with one column. See [`set-returning-functions.md`](./set-returning-functions.md).

A derived table may stand on either side of any join:

```sql
SELECT o.id, t.n
FROM orders o
LEFT JOIN (SELECT order_id, COUNT(*) AS n FROM line_items GROUP BY order_id) t
  ON t.order_id = o.id;
```

## The left outer join contract

These rules hold for every execution strategy the planner can pick.

- **Every left row is emitted at least once.** A left row with one or more right rows that
  satisfy `ON` is emitted once per such right row. A left row with no such right row is emitted
  once, with every right output column set to NULL. These are the *padded* rows.
- **A NULL join key never matches.** A left row whose join key is NULL is padded. It is never
  matched to a right row whose key is also NULL, because `NULL = NULL` is unknown. A composite
  key with one NULL component follows the same rule.
- **`ON` decides matching; `WHERE` filters the output.** A conjunct in `ON` decides which right
  rows pair with a left row. A conjunct in `WHERE` runs after padding and sees the padded rows.

The last rule makes the queries below differ:

```sql
-- Environments and their game, or NULL when the game is missing: three rows for three environments.
SELECT e.id, g.studio_id FROM environments e LEFT JOIN games g ON g.id = e.game_id;

-- Only the environments whose game is missing: the padded rows survive the IS NULL test.
SELECT e.id FROM environments e LEFT JOIN games g ON g.id = e.game_id WHERE g.id IS NULL;

-- Only matched rows with studio 5: an equality on the right side removes every padded row.
SELECT e.id FROM environments e LEFT JOIN games g ON g.id = e.game_id WHERE g.studio_id = 5;
```

A padded row carries every right column a matched row carries, each holding NULL. `SELECT *`
over a left join with an empty right table therefore returns the same column set as the same
query over a populated one. `COUNT(*)` counts padded rows; `COUNT(b.id)` does not, because the
cell is NULL.

## RIGHT JOIN

A right join is executed as a left join with the operands swapped: `A RIGHT JOIN B ON p` and
`B LEFT JOIN A ON p` return the same multiset of rows. The swap does not change the columns:
`SELECT * FROM a RIGHT JOIN b ON ...` lists the columns of `a` first, as the `FROM` clause writes
them and as PostgreSQL does.

One limit follows from the rewrite:

- **A `RIGHT JOIN` whose left operand is itself a join is not supported.** The right operand of
  a join must be one table or one derived table, so there is nowhere to put the joined subtree
  after the swap. `A JOIN B ON p RIGHT JOIN C ON q` is refused with `CADB0533`. Rewrite it with
  the preserved table on the left: `C LEFT JOIN (...)` through a derived table, or reorder the
  chain.

## Output column names

A join names its output columns as PostgreSQL does:

- `SELECT *` gives each column its bare name, in the order that the `FROM` clause writes the
  tables: `SELECT * FROM a JOIN b ON a.id = b.id` has the columns `id, v, id, w`.
- An explicit list gives each column reference its bare name: `SELECT a.v, b.v` has the columns
  `v, v`.
- Two columns can therefore have one name. A client reads the columns by position; the HTTP and
  gRPC results carry every column, in order, with its type.
- An outer query that reads a derived table with two columns of one name refuses that name as
  ambiguous (`CADB0400`), also when it is qualified: `SELECT d.v FROM (SELECT a.v, b.v FROM a JOIN b
  ON ...) d`. Give the columns different names with `AS`.
- `CREATE TABLE ... AS`, `CREATE VIEW` and `CREATE MATERIALIZED VIEW` refuse two columns of one
  name. A `*` over a join whose tables share no column name is accepted.

## CROSS JOIN

`A CROSS JOIN B` returns `|A| × |B|` rows and gets exactly the plan of the comma form
`FROM A, B`. When every join of the `FROM` clause is a cross join, the planner runs the same
equi-join hoist the comma form uses, so a `WHERE a.x = b.x` still becomes an `ON` predicate. A
cross join that sits among `ON` joins becomes an inner join on the literal `TRUE`.

## LATERAL

A derived table with the keyword `LATERAL` can read the columns of the `FROM` items to its left. It runs
one time for each row of those items, as in PostgreSQL:

```sql
-- The two most expensive orders of each customer.
SELECT c.id, o.id, o.total
FROM customers c
CROSS JOIN LATERAL (
    SELECT id, total FROM orders WHERE orders.customer_id = c.id ORDER BY total DESC LIMIT 2
) o;

-- Name a computed value once, and read it in later expressions.
SELECT p.id, v.area, v.area * p.price AS cost
FROM plots p, LATERAL (SELECT p.width * p.depth AS area) v;

-- Keep a customer with no order: a LEFT JOIN pads the row with NULL.
SELECT c.id, n.orders
FROM customers c
LEFT JOIN LATERAL (SELECT COUNT(*) AS orders FROM orders WHERE customer_id = c.id) n ON true;
```

The rules:

- **Join forms.** `LATERAL` can follow a comma, `CROSS JOIN`, `JOIN … ON` and `LEFT JOIN … ON`. A
  `RIGHT JOIN LATERAL` is refused with `CADB0400`, because the right side of a right join has no left row
  to run for (see [PostgreSQL differences](postgresql-differences.md#lateral)). There is no `CROSS APPLY`
  or `OUTER APPLY`: write `CROSS JOIN LATERAL (…)` and `LEFT JOIN LATERAL (…) ON true`.
- **Names.** A name in the derived table resolves first to the derived table's own `FROM`, then to the
  items to its left, then to the query around the statement part, as for a subquery. A `LATERAL` derived
  table cannot read an item to its right. A second `LATERAL` derived table can read the first one.
- **Without `LATERAL`.** A derived table without the keyword cannot read the items of its own `FROM`; a
  name of such an item is an unknown alias. A `LATERAL` derived table that reads no item to its left runs
  one time, as a plain derived table.
- **Functions in `FROM`.** A set-returning function whose arguments name a column is `LATERAL` with or
  without the keyword, as in PostgreSQL: `FROM t, generate_series(1, t.n) AS g` gives each row of `t`
  the numbers 1 to `t.n`. A function whose arguments name no column runs one time.
- **`VALUES` lists.** `CROSS JOIN LATERAL (VALUES ('a', t.a), ('b', t.b)) AS s (name, value)` splits each
  row of `t` into two rows. A `VALUES` list is not `LATERAL` without the keyword, as in PostgreSQL. See
  [VALUES lists in FROM](values-lists.md).
- **The body.** The derived table can be any `SELECT`: a join, an aggregate, `GROUP BY`, `ORDER BY` and
  `LIMIT`, a window function, a set operation, a subquery, or no `FROM`.
- **Views and writes.** A view body, `INSERT … SELECT`, `CREATE TABLE … AS`, `UPDATE … FROM` and
  `DELETE … USING` can hold a `LATERAL` derived table. `SHOW CREATE VIEW` shows the `LATERAL` keyword.
- **A reserved word.** `LATERAL` is reserved, as in PostgreSQL. A column or table named `lateral` must be
  quoted with backticks.

How it runs:

- The join keeps the order written in the `FROM` clause, because a reorder could put the derived table
  before an item that it reads. The plan cache keeps no join order for such a query.
- The join is always a nested loop: for each left row, the derived table runs with the left values that
  it reads. `EXPLAIN` shows `nested-loop-join(…, right=o, lateral)`.
- Left rows with the same values that the derived table reads share one run in a statement. A run of at
  most 256 rows is kept for later rows with the same values. A derived table that calls a volatile
  function, such as `random()`, runs again for each left row.

## What EXPLAIN shows

Every join operator serves a left outer join, because in each of them the left input is the
preserved side: the outer loop of a nested loop, the probe of a hash join, the left stream of a
merge join. `EXPLAIN` appends `kind=left-outer` to the join node; an inner join renders as before.

```text
hash-join(on=e.game_id=id, build=g, kind=left-outer)
  table-scan(table=environments)
```

Two planner rules are specific to the kind:

- **The hash build side is the right table.** For an inner join the planner builds the smaller
  side. For a left outer join the preserved side must be the probe, so an unmatched probe row can
  be padded at once, in stream order, with no per-build-row matched flag. The smaller-side rule
  is not consulted.
- **`WHERE` pushdown follows the preserved side.** A single-table conjunct on the preserved side
  (`WHERE e.id = @p`) is pushed to that scan, exactly as for an inner join. A conjunct on the
  null-extended side (`WHERE g.studio_id = 5`) stays in the post-join filter, because pushing it
  below the join would filter before padding and turn the join back into an inner join.

On a cluster, the broadcast hash join declines a left outer join and runs the local probe: a
remote probe span returns matched rows only and cannot pad.

## Limits

- `FULL [OUTER] JOIN`, `NATURAL JOIN` and `JOIN ... USING (...)` are not supported. `FULL JOIN`
  and `FULL OUTER JOIN` are refused with `CADB0533`; the other forms are syntax errors. `FULL`
  stays a plain identifier, because the foreign-key clause `MATCH FULL` needs it. So a table whose
  bare, unquoted alias is `full` cannot sit directly before a bare `JOIN`, where it would read as
  `FULL JOIN`. Write the alias as `AS full`, quote it, or use it before `INNER JOIN`, `LEFT JOIN`,
  `RIGHT JOIN` or `CROSS JOIN`; those forms are always read as an alias.
- A query that contains an outer join keeps its declared join order. Neither the heuristic nor
  the cost-based join-order enumeration reorders a tree with an outer join.
- A `WHERE` conjunct on the null-extended side runs after the join, even when it is
  null-rejecting and the join could have been simplified to an inner join.
- A `RIGHT JOIN` after another join is not supported (see above).
- A query with a `LATERAL` derived table keeps its declared join order, and its `LATERAL` join is
  always a nested loop (see [LATERAL](#lateral)).
- An `ON` equality between columns of two types (`INT64 = FLOAT64`, `INT64 = NUMERIC`) is not a key
  for a hash join, a merge join or an index probe, so that join always runs as a nested loop. The
  rows are correct: `1 = 1.0` matches and `2 = 2.5` does not. See
  [`query-planner.md`](./query-planner.md) for the reason.
- A join leaf whose pushed filter no index serves is a table scan with a residual filter. Under
  Serializable read-write that leaf holds a shared lock over the whole table until the transaction
  ends, so every writer into that table is held back meanwhile; an index leaf (a literal or a
  parameter on an indexed column, a full-key equality on a unique index included) locks only the
  keys it reads. See `transactions-locking-and-isolation.md` §6.2 and §6.3 for the locks every join
  leaf and probe takes.
