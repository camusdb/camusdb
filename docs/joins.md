# Joins

CamusDB supports four join forms in the `FROM` clause of a `SELECT`, and `LATERAL` derived tables
(see [LATERAL](#lateral)). Each form returns a different set of rows.

| Form | Syntax | Rows returned |
|------|--------|---------------|
| Inner | `A JOIN B ON p` or `A INNER JOIN B ON p` | All pairs `(a, b)` for which `p` is true. |
| Left outer | `A LEFT JOIN B ON p` or `A LEFT OUTER JOIN B ON p` | All inner-join pairs, plus one row for each `a` that matches no `b`. In that row, all `B` columns are NULL. |
| Right outer | `A RIGHT JOIN B ON p` or `A RIGHT OUTER JOIN B ON p` | The same rows as `B LEFT JOIN A ON p`. |
| Cross | `A CROSS JOIN B` | All pairs `(a, b)`. No `ON` clause is allowed. |

Joins chain left to right, and a chain may mix the forms.

```sql
SELECT a.id, b.id, c.id, d.id
FROM a
JOIN b ON b.a_id = a.id
LEFT JOIN c ON c.b_id = b.id
JOIN d ON d.a_id = a.id;
```

A set-returning function such as `generate_series` may also stand on either side of any join, as
a relation with one column. See [`set-returning-functions.md`](./set-returning-functions.md).

A derived table may stand on either side of any join.

```sql
SELECT o.id, t.n
FROM orders o
LEFT JOIN (SELECT order_id, COUNT(*) AS n FROM line_items GROUP BY order_id) t
  ON t.order_id = o.id;
```

## The left outer join contract

These rules apply to all execution strategies the planner can pick.

- **Each left row is emitted at least once.** A left row with one or more right rows that
  satisfy `ON` is emitted once per such right row. A left row that matches no right row is emitted
  once, with all right output columns set to NULL. These are the *padded* rows.
- **A NULL join key never matches.** A left row whose join key is NULL is padded. `NULL = NULL` is
  unknown, which means that the row is not matched to a right row whose key is also NULL. A
  composite key with one NULL component follows the same rule.
- **`ON` decides matching; `WHERE` filters the output.** A conjunct in `ON` decides which right
  rows pair with a left row. A conjunct in `WHERE` is applied after padding and sees the padded rows.

Because of the last rule, the three queries below return different rows.

```sql
-- Environments and their game, or NULL when the game is missing: three rows for three environments.
SELECT e.id, g.studio_id FROM environments e LEFT JOIN games g ON g.id = e.game_id;

-- Only the environments whose game is missing: the padded rows survive the IS NULL test.
SELECT e.id FROM environments e LEFT JOIN games g ON g.id = e.game_id WHERE g.id IS NULL;

-- Only matched rows with studio 5: an equality on the right side removes every padded row.
SELECT e.id FROM environments e LEFT JOIN games g ON g.id = e.game_id WHERE g.studio_id = 5;
```

A padded row carries the same right columns as a matched row, each set to NULL. `SELECT *` over a
left join with an empty right table therefore returns the same column set as the same query over a
populated one. `COUNT(*)` counts padded rows. `COUNT(b.id)` skips them because the cell is NULL.

## RIGHT JOIN

A right join is executed as a left join with the operands swapped: `A RIGHT JOIN B ON p` and
`B LEFT JOIN A ON p` return the same multiset of rows. The swap does not change the columns:
`SELECT * FROM a RIGHT JOIN b ON ...` lists the columns of `a` first, as the `FROM` clause writes
them and as PostgreSQL does.

The rewrite leads to one limit.

- **A `RIGHT JOIN` whose left operand is itself a join is not supported.** The right operand of
  a join must be one table or one derived table. After the swap, there is nowhere to put the joined
  subtree. `A JOIN B ON p RIGHT JOIN C ON q` returns `CADB0533`. Rewrite it with
  the preserved table on the left: `C LEFT JOIN (...)` through a derived table, or reorder the
  chain.

## Column names in join predicates

A join predicate can use a bare column name when exactly one table of the statement has a column
of that name. The planner treats the two statements below as the same statement, and they get the
same plan.

```sql
SELECT o_orderkey FROM customer, orders WHERE c_custkey = o_custkey AND c_mktsegment = 'BUILDING';
SELECT o_orderkey FROM customer, orders
WHERE customer.c_custkey = orders.o_custkey AND customer.c_mktsegment = 'BUILDING';
```

- The binder gives each bare name of an `ON` or a `WHERE` its table and writes it as
  `alias.column`. The planner reads the table of a column from that qualifier. This means that a
  bare name can serve as a hash, merge or index lookup key, a range seek bound and an expression key,
  as a qualified name can.
- `EXPLAIN` shows the qualified name, also when the statement has the bare name.
- A name in an `ON` is a column of the tables of that join only, as in PostgreSQL. In
  `a JOIN b ON x = y JOIN c ON …` the name `x` is a column of `a` or of `b`, also when `c` has a
  column `x`.
- A name that two tables in scope have is refused as ambiguous (`CADB0400`), and a name that no
  table has is an unknown column (`CADB0404`).
- The select list, `GROUP BY`, `HAVING` and `ORDER BY` keep the names as written; see
  [Output column names](#output-column-names).

## Output column names

A join names its output columns as PostgreSQL does.

- `SELECT *` gives each column its bare name, in the order that the `FROM` clause writes the
  tables: `SELECT * FROM a JOIN b ON a.id = b.id` has the columns `id, v, id, w`.
- An explicit list gives each column reference its bare name: `SELECT a.v, b.v` has the columns
  `v, v`.
- Two columns can therefore have one name. A client reads the columns by position; the HTTP and
  gRPC results carry all columns, in order, with their types.
- If an outer query reads a derived table that has two columns of one name, that name is ambiguous
  (`CADB0400`), also when it is qualified: `SELECT d.v FROM (SELECT a.v, b.v FROM a JOIN b
  ON ...) d`. Give the columns different names with `AS`.
- `CREATE TABLE ... AS`, `CREATE VIEW` and `CREATE MATERIALIZED VIEW` refuse two columns of one
  name. A `*` over a join whose tables share no column name is accepted.

## CROSS JOIN

`A CROSS JOIN B` returns `|A| × |B|` rows and gets exactly the plan of the comma form
`FROM A, B`. When all joins of the `FROM` clause are cross joins, the planner applies the same
equi-join hoist that the comma form uses. This means that a `WHERE a.x = b.x` becomes an `ON`
predicate in that case too. A cross join that sits among `ON` joins becomes an inner join on the
literal `TRUE`.

The hoist moves each `WHERE` conjunct `left.column = right.column` into the `ON` of the join where
the later of its two tables enters. It needs the table of each column. Query creation performs the
hoist when all column comparisons of the `WHERE` are qualified. When one has a bare name, the binder
performs it after it gives each bare name its table (see
[Column names in join predicates](#column-names-in-join-predicates)). Both paths apply the same hoist
over the conjuncts in `WHERE` order. The tree therefore does not depend on how the names are written.

## LATERAL

A derived table with the keyword `LATERAL` can read the columns of the `FROM` items to its left. It is
evaluated one time for each row of those items, as in PostgreSQL.

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

Several rules apply to a `LATERAL` derived table.

- **Join forms.** `LATERAL` can follow a comma, `CROSS JOIN`, `JOIN … ON` and `LEFT JOIN … ON`. A
  `RIGHT JOIN LATERAL` returns `CADB0400`. Because the right side of a right join is preserved, there
  is no left row to evaluate it for (see [PostgreSQL differences](postgresql-differences.md#lateral)).
  `CROSS APPLY` and `OUTER APPLY` are not supported. Write `CROSS JOIN LATERAL (…)` and
  `LEFT JOIN LATERAL (…) ON true`.
- **Names.** A name in the derived table resolves first to the derived table's own `FROM`, then to the
  items to its left, then to the query around the statement part, as for a subquery. A `LATERAL` derived
  table cannot read an item to its right. A second `LATERAL` derived table can read the first one.
- **No `LATERAL` keyword.** A derived table that is not marked `LATERAL` cannot read the items of the
  `FROM` that contains it; a name of such an item is an unknown alias. A `LATERAL` derived table that
  reads no item to its left is evaluated one time, like a regular derived table.
- **Functions in `FROM`.** A set-returning function whose arguments name a column is `LATERAL` whether
  or not the keyword is written, as in PostgreSQL: `FROM t, generate_series(1, t.n) AS g` gives each
  row of `t` the numbers 1 to `t.n`. A function whose arguments name no column is evaluated one time.
- **`VALUES` lists.** `CROSS JOIN LATERAL (VALUES ('a', t.a), ('b', t.b)) AS s (name, value)` splits each
  row of `t` into two rows. A `VALUES` list is `LATERAL` only with the keyword, as in PostgreSQL. See
  [VALUES lists in FROM](values-lists.md).
- **The body.** The derived table can be any `SELECT`, such as a join, an aggregate or a set operation.
  `GROUP BY`, `ORDER BY`, `LIMIT`, window functions and subqueries are allowed, and the body may omit
  `FROM`.
- **Views and writes.** A view body, `INSERT … SELECT`, `CREATE TABLE … AS`, `UPDATE … FROM` and
  `DELETE … USING` can hold a `LATERAL` derived table. `SHOW CREATE VIEW` shows the `LATERAL` keyword.
- **A reserved word.** `LATERAL` is reserved, as in PostgreSQL. A column or table named `lateral` must be
  quoted with backticks.

Execution of a `LATERAL` join differs from other joins in several ways.

- The join uses the order written in the `FROM` clause. A reorder could put the derived table before
  an item that it reads. The plan cache stores no join order for such a query.
- The join is always a nested loop: for each left row, the derived table is evaluated with the left
  values that it reads. `EXPLAIN` shows `nested-loop-join(…, right=o, lateral)`.
- For each left row, the join reads only the left columns that the derived table names. Each name
  resolves to its place in the left row one time. This means that a wide left row costs no more than
  a narrow one.
- Left rows with the same values that the derived table reads share one evaluation in a statement.
  The result of an evaluation is kept for later rows with the same values when it has at most 256
  rows. A derived table that calls a volatile function, such as `random()`, is evaluated again for
  each left row.
- A derived table that only names expressions of the left row, such as `LATERAL (SELECT p.width *
  p.depth AS area) v`, gives one row for each left row. The join evaluates its expressions on the left
  row and does not evaluate the body as a subquery. The cost is therefore close to the cost of the
  same expressions in a select list. The body must be one `SELECT`. It cannot have a `FROM`, `WHERE`,
  `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET` or `DISTINCT` clause, or a volatile call. Each
  item can hold literals, parameters, left columns, operators, `CAST`, `CASE`, scalar functions and
  uncorrelated subqueries, which are evaluated one time for the statement.
- A `VALUES` list, such as `LATERAL (VALUES ('ceil', p.top), ('floor', p.bottom)) AS s (slot, h)`, is
  evaluated in the same way: each row of the list gives one row for each left row, in list order. A
  `UNION ALL` of such `SELECT`s is the same body. Each cell takes the type of its column, as in a
  `UNION ALL`.
- In both forms, an item can also hold a scalar subquery that reads the left row, such as
  `COALESCE((SELECT t.height FROM texture t WHERE t.name = p.tex), 128)`. The subquery binds one time
  and is evaluated for each left row. A left row whose values the subquery read for an earlier row
  gets the earlier value. This means that the subquery is evaluated one time for each distinct value.
  A subquery that returns no row gives NULL, and a subquery with two rows fails the statement. Each
  subquery of the body is evaluated before an item is evaluated, also a subquery in a `CASE` branch
  that the row does not take. The run for each left row behaves the same way. With an `EXISTS` or
  `IN` subquery that reads the left row, or a subquery whose nested subquery reads it, the join uses
  the run for each left row.
- The rows, column types, NULLs and errors are the same in the evaluated form and in the run for each
  left row. `EXPLAIN` shows `lateral=expressions(area)` with the column names in place of `lateral`.

## Expression keys

A hash join keys on an `ON` equality whose two sides each read only one side of the join. A side
can be a column or an expression over the columns of that side.

```sql
SELECT s.id, sd.tex
  FROM seg s
  JOIN linedef l ON l.id = s.linedef_id
  JOIN sidedef sd ON sd.id = CASE WHEN s.side = 0 THEN l.right_sidedef ELSE l.left_sidedef END;
```

- The hash join computes the expression once for each row of its side and looks the value up in the
  hash table. If there is no key, such a join is executed as a nested loop that tests each pair, or
  as a hash join on its other keys that tests the expression for each pair.
- An expression can use literals, columns of the same side named with their aliases, arithmetic,
  `CAST`, `CASE` and functions that are not volatile. A parameter, a subquery or a column of the
  other side gives no key. An equality of two columns is a key as before.
- The static type of the expression must be the type of the other side, by the rule for two columns
  (see [Limits](#limits)). `t.f = n.n * 0.5` is a key for a `FLOAT64` column `f`. `t.k = n.n * 1.0`
  is no key for an `INT64` column `k`.
- The `ON` predicate is also tested on each pair. This means that the rows are the rows of the
  nested loop. A NULL key value matches no row; a left join pads its row.
- The hash join computes the key for each row of its side. An error in the expression, such as a
  division by zero, can therefore come from a row that the nested loop would not test, for example a
  left row when the right side is empty.
- Only the hash join takes an expression key, also when it spills to the Grace hash join. A merge
  join, an index nested-loop join and the broadcast hash join read a key by its column name. They
  serve column keys only. On a cluster, a hash join with an expression key performs its probe
  locally.
- `EXPLAIN` shows the expression in parentheses:
  `hash-join(on=(CASE WHEN s.side = 0 THEN l.right_sidedef ELSE l.left_sidedef END)=id, build=sd)`.
- The join-order search uses the key to choose the operator, but its row estimate treats the
  equality as a condition that is not a key.

## Range seek

A join that does not have an equality key is normally executed as a nested loop that tests each
right row against `ON` for each left row. When `ON` bounds the first column of an ascending
right-side index from below and from above with values of the left row, the join reads, for each
left row, only that index range. A "numbers table" join is the common case.

```sql
SELECT s.id, n.n
  FROM spans s
  JOIN numbers n ON n.n BETWEEN s.lo AND s.hi;
```

- The bounds can be `BETWEEN`, `<`, `<=`, `>` and `>=`, with the right column on either side. Several
  bounds of one side combine into the tightest one.
- The right column must be named with its alias (`n.n`). A bound can use literals, parameters,
  columns of the left side named with their aliases, arithmetic, `CAST`, `CASE` and functions that are
  not volatile. A bound that reads a right column, or a subquery, gives no seek.
- Both a lower and an upper bound are needed. A range open on one side gets no seek, and neither
  does a descending first index column or a right table with fewer than 64 rows. Such a join uses a
  nested loop that reads the right table once and replays it from memory.
- A unique index of several columns serves the seek if its other columns are `NOT NULL`, and does
  not serve it otherwise. A unique index does not store an entry for a row with a NULL key column.
- The `ON` predicate is also tested on each pair. This means that the rows are the same as those of
  a nested loop. A NULL bound gives no right row for that left row (a left join pads it), and a range
  whose lower end is above its upper end gives none either. A bound whose type cannot be represented
  exactly in the key's type is left out for that row, which widens the range. A fractional bound on
  an `INT64` key moves to the integer that selects the same rows (`n.n >= 2.5` reads from 3).
- Under Serializable read-write, each seek takes a shared lock on the range it reads, as an index
  range scan does (see `transactions-locking-and-isolation.md` §6.2). An insert into a range the
  join read is held back; an insert outside all of the ranges is not.
- `EXPLAIN` shows `nested-loop-join(…, range-seek=<index>(<bounds>))`, and `EXPLAIN (ANALYZE)` counts
  the index entries of all the ranges in `kv_scan_entries`.
- The join-order search does not price the seek yet. It prices the join as if it had no seek.

## What EXPLAIN shows

All join operators can serve a left outer join. This works because in each of them the left input
is the preserved side: the outer loop of a nested loop, the probe of a hash join, the left stream of
a merge join. `EXPLAIN` appends `kind=left-outer` to the join node; an inner join renders as before.

```text
hash-join(on=e.game_id=id, build=g, kind=left-outer)
  table-scan(table=environments)
```

Two planner rules apply only to left outer joins.

- **The hash build side is the right table.** For an inner join, the planner builds the smaller
  side. For a left outer join, the preserved side must be the probe. This means that an unmatched
  probe row can be padded at once, in stream order, and no matched flag per build row is needed. The
  smaller-side rule is not consulted.
- **`WHERE` pushdown follows the preserved side.** A single-table conjunct on the preserved side
  (`WHERE e.id = @p`) is pushed to that scan, exactly as for an inner join. A conjunct on the
  null-extended side (`WHERE g.studio_id = 5`) is applied in the post-join filter. Pushing it below
  the join would filter before padding and turn the join back into an inner join.

On a cluster, the broadcast hash join declines a left outer join, and the local probe is used. A
remote probe span returns matched rows only and cannot pad.

## Limits

- `FULL [OUTER] JOIN`, `NATURAL JOIN` and `JOIN ... USING (...)` are not supported. `FULL JOIN`
  and `FULL OUTER JOIN` return `CADB0533`; the other forms are syntax errors. `FULL` is lexed as an
  ordinary identifier because the foreign-key clause `MATCH FULL` needs it. This means that a table
  whose bare, unquoted alias is `full` cannot sit directly before a bare `JOIN`, where it would read
  as `FULL JOIN`. Write the alias as `AS full` or quote it. You can also use it before `INNER JOIN`,
  `LEFT JOIN`, `RIGHT JOIN` or `CROSS JOIN`; those forms are always read as an alias.
- A query that contains an outer join uses its declared join order. Neither the heuristic nor
  the cost-based join-order enumeration reorders a tree with an outer join.
- A `WHERE` conjunct on the null-extended side is applied after the join, even when it is
  null-rejecting and the join could have been simplified to an inner join.
- A `RIGHT JOIN` after another join is not supported (see above).
- A query with a `LATERAL` derived table uses its declared join order, and its `LATERAL` join is
  always a nested loop (see [LATERAL](#lateral)).
- An `ON` equality between columns of two types (`INT64 = FLOAT64`, `INT64 = NUMERIC`) is not a key
  for a hash join, a merge join or an index probe. That join is therefore always executed as a
  nested loop. The rows are correct: `1 = 1.0` matches and `2 = 2.5` does not. See
  [`query-planner.md`](./query-planner.md) for the reason.
- A join leaf whose pushed filter no index serves is a table scan with a residual filter. Under
  Serializable read-write, that leaf takes a table-wide shared lock until the transaction ends. This
  means that all writers into that table are held back meanwhile. An index leaf (a literal or a
  parameter on an indexed column, a full-key equality on a unique index included) locks only the
  keys it reads. See `transactions-locking-and-isolation.md` §6.2 and §6.3 for the locks that each
  join leaf and probe takes.
