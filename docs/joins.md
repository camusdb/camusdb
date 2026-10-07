# Joins

CamusDB supports four join forms in the `FROM` clause of a `SELECT`:

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

The last rule is what makes the two queries below differ:

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
`B LEFT JOIN A ON p` return the same multiset of rows. Two consequences follow from the rewrite:

- **`SELECT *` lists the preserved table's columns first.** The row layout follows the executed
  order, so `SELECT * FROM a RIGHT JOIN b ON ...` lists `b`'s columns before `a`'s. An explicit
  column list is unaffected.
- **A `RIGHT JOIN` whose left operand is itself a join is not supported.** The right operand of
  a join must be one table or one derived table, so there is nowhere to put the joined subtree
  after the swap. `A JOIN B ON p RIGHT JOIN C ON q` is refused with `CADB0533`. Rewrite it with
  the preserved table on the left: `C LEFT JOIN (...)` through a derived table, or reorder the
  chain.

## CROSS JOIN

`A CROSS JOIN B` returns `|A| × |B|` rows and gets exactly the plan of the comma form
`FROM A, B`. When every join of the `FROM` clause is a cross join, the planner runs the same
equi-join hoist the comma form uses, so a `WHERE a.x = b.x` still becomes an `ON` predicate. A
cross join that sits among `ON` joins becomes an inner join on the literal `TRUE`.

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
