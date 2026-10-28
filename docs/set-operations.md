# Set Operations: UNION, INTERSECT, EXCEPT

CamusDB combines the rows of two queries with `UNION`, `INTERSECT` and `EXCEPT`, each with an
optional `ALL`. The semantics are the SQL standard's and PostgreSQL's.

```sql
q1 UNION [ALL | DISTINCT] q2
q1 INTERSECT [ALL | DISTINCT] q2
q1 EXCEPT [ALL | DISTINCT] q2

-- chains, parentheses, and ORDER BY / LIMIT / OFFSET on the combined result
(SELECT a FROM t ORDER BY a LIMIT 5) UNION ALL SELECT b FROM u ORDER BY 1 LIMIT 10 OFFSET 2;
```

## What each form returns

Two rows are the same row when their values are equal position by position. Two NULLs are equal
for this purpose. This is the rule that `DISTINCT` and `GROUP BY` use, not the rule of `=`.

| Form | Rows returned, for a row that the left query returns m times and the right query n times |
|---|---|
| `UNION ALL` | m + n copies |
| `UNION` | one copy if m + n > 0 |
| `INTERSECT ALL` | min(m, n) copies |
| `INTERSECT` | one copy if m > 0 and n > 0 |
| `EXCEPT ALL` | max(m − n, 0) copies |
| `EXCEPT` | one copy if m > 0 and n = 0 |

The default is `DISTINCT`: a plain `UNION`, `INTERSECT` or `EXCEPT` removes duplicate rows.
`UNION DISTINCT` is accepted and means the same as `UNION`.

Without `ORDER BY`, the order of the rows is not defined. Do not depend on it.

## Precedence

- `INTERSECT` binds tighter than `UNION` and `EXCEPT`.
- `UNION` and `EXCEPT` have the same precedence and group from left to right.
- Parentheses override both rules.

```sql
a UNION b INTERSECT c      -- a UNION (b INTERSECT c)
a EXCEPT b EXCEPT c        -- (a EXCEPT b) EXCEPT c
a UNION b EXCEPT c         -- (a UNION b) EXCEPT c
```

## Columns

- Both operands must return the same number of columns. If they do not, the statement fails with
  `InvalidInput` before it reads a row.
- Columns match by **position**, not by name. The output column names come from the leftmost
  operand.
- Each output column gets one type, from the types of the two operands at that position:

| Left and right | Output type |
|---|---|
| The same type | that type |
| A `NULL` literal or a placeholder on one side | the type of the other side |
| Two numeric types | the type that `+` gives for them, for example `int64` with `float64` gives `float64`, and `int64` with `numeric` gives `numeric` |
| `date` with `datetime` | `datetime` |
| Any other pair | error `InvalidInput`, which names the column position and both types |

Each value is converted to its output type before rows are compared. So `SELECT 1 UNION SELECT 1.0`
returns one row, `1.0`, and a `date` equals the `datetime` at its midnight.

A column that is `NULL` in both operands has the declared type `string`, but it stays untyped for a
set operation that uses it: in `SELECT 1 UNION ALL (SELECT NULL UNION ALL SELECT NULL)` the column
is `int64`.

## ORDER BY, LIMIT and OFFSET

`ORDER BY`, `LIMIT` and `OFFSET` after the last operand apply to the combined result.

- An `ORDER BY` item must be an output column name or an output position (`ORDER BY 1`).
- An expression is refused with `InvalidInput`, also an expression over output columns
  (`ORDER BY a + 1`), as in PostgreSQL. To sort by an expression, put the set operation in a derived
  table: `SELECT * FROM (q1 UNION q2) AS s ORDER BY s.a + 1`.

An operand can have its own `ORDER BY`, `LIMIT` and `OFFSET` only in parentheses. Without the
parentheses, `SELECT a FROM t ORDER BY a UNION SELECT b FROM u` is a syntax error.

A parenthesized query takes one set of these clauses. `(SELECT a FROM t LIMIT 5) ORDER BY a` is
refused, because it would sort before the limit instead of after it. Put all of the clauses inside
the parentheses, or wrap the query in a derived table.

## Where a set operation can appear

- As a statement, and under `EXPLAIN`.
- In a derived table: `SELECT … FROM (q1 UNION q2) AS s`.
- In a subquery: `x IN (q1 UNION q2)`, `EXISTS (q1 EXCEPT q2)`, and a scalar subquery
  `(q1 INTERSECT q2)`. A scalar subquery that returns more than one row is an error, as for any
  scalar subquery.
- As the source of `INSERT INTO t SELECT … UNION SELECT …` and of `CREATE TABLE … AS`. A source that
  starts with a parenthesis is also accepted: `INSERT INTO t (SELECT …) UNION SELECT …`.

An operand can be a view. It expands as it does in any `FROM`.

`WITH` (common table expressions) and `CORRESPONDING BY` are not supported.

## Views

A view or a materialized view can have a set operation as its body:

```sql
CREATE VIEW all_ids (id) AS SELECT id FROM a UNION SELECT id FROM b ORDER BY id;
CREATE MATERIALIZED VIEW shared AS SELECT x FROM a INTERSECT SELECT y FROM b;
```

When the view is created:

- The `*` of each operand is expanded to that operand's columns. A column added later to a table
  that an operand reads does not change the view.
- The view's column names are stored as aliases on the leftmost operand, which names the columns.
  An unnamed expression there needs an alias, as in any view body.
- An `ORDER BY` name of the set operation is stored as its position, so a column-alias list does
  not break it.
- The columns that every operand reads are recorded. Dropping one of them, or a table that an
  operand reads, is refused with `DependentObjectsExist`. A renamed table or column is followed.

`SHOW CREATE VIEW` prints the body with each nested set operation in parentheses, and with each
operand in parentheses that has its own `ORDER BY`, `LIMIT` or `OFFSET`.

A set operation can also be inside a derived table or a subquery of a view body. There, its
operands cannot use `*`: the view could not expand it at creation, and a column added to one table
would give the operands different column counts. Such a body is refused with `InvalidInput`.

A view can be an operand of a set operation in a query, like any relation.

## AS OF SYSTEM TIME

All operands read in the same snapshot of the statement. An operand with its own
`AS OF SYSTEM TIME` is refused with `InvalidAsOfSystemTime`.

## Privileges

Each operand needs `SELECT` on the tables that it reads. A grant on the tables of one operand gives
nothing on the tables of the other. A subquery in an operand's `WHERE` reads its table with the same
privileges as the operand: the caller's in a statement, the view owner's in a view body.

## Execution

| Form | How it runs |
|---|---|
| `UNION ALL` | Streams the rows of the left operand, then the rows of the right operand. Nothing is held in memory, so a `LIMIT` stops the operands early. |
| `UNION` | Streams both operands through the `DISTINCT` operator. |
| `INTERSECT [ALL]`, `EXCEPT [ALL]` | Counts the rows of the right operand in a hash table, then streams the left operand and returns each row by the count rules. |

Memory for `INTERSECT` and `EXCEPT` is the distinct rows of the right operand. `EXCEPT` also keeps
the distinct left rows that it returned, so it does not return one twice.

### Spill to disk

With `spill_enabled` on, the hash tables spill to disk by the rules of
[spill-to-disk.md](./spill-to-disk.md):

- `UNION` spills as `DISTINCT` does.
- `INTERSECT` and `EXCEPT` spill when the hash table of the right operand reaches
  `spill_threshold_rows` distinct rows or `spill_threshold_bytes` bytes. Both operands are then
  written to `spill_merge_fan_in` partition files by row hash, and each pair of files is combined in
  memory. Equal rows always land in the same partition, so the counts stay exact. A partition is not
  split again.

With spill off, the hash tables stay in memory.

### Long chains

A chain of one associative operator — `UNION ALL`, `UNION`, `INTERSECT` or `INTERSECT ALL` — is
rebalanced by the parser when it has more than 8 operands. Generated SQL with a thousand
`UNION ALL` operands therefore runs, and `UNION ALL` still returns the rows in the order of the
operands.

`EXCEPT` is not associative, and a mix of operators is not one chain. Each of those adds one level
of nesting. A statement may nest at most 64 set operations; a deeper one fails with
`StatementTooDeeplyNested` (see [statement-nesting-limit.md](./statement-nesting-limit.md)).

## EXPLAIN

The set operation is one plan node, named for its operator, with each operand's plan under an
`operand` row:

| Node | Detail |
|---|---|
| `union` | `all`, or `distinct: hash` |
| `intersect`, `except` | `hash`, or `all, hash` |
| `operand` | `left` or `right`; the rows after it are the plan of that operand |

`, spill` follows `hash` when spill is enabled; it means that the hash table can move to disk, not
that it did.

```text
EXPLAIN SELECT x FROM a WHERE x > 1 UNION SELECT y FROM b ORDER BY 1

node         detail
union        distinct: hash
operand      left
project
filter       x > 1
table-scan   table=a
operand      right
project
table-scan   table=b
plan-info    shape=…, schema-deps=[…]
```

`EXPLAIN (ANALYZE)` runs the operand plans that it shows, so the counters under each operand are
real. The `actual_rows` of an `operand` row is the number of rows that the operand returned, and
the `actual_rows` of the set-operation row is the number of rows that it returned.

The `estimated_rows` of the set-operation row is the default estimate of a derived table. The
planner does not estimate the result size of a set operation.

## Caches

- The plan cache keys each set operation by its operator, its `ALL` flag and the shapes of both
  operands, so two set operations over different operands never share a plan.
- The combined result of a set operation is never stored in the query-result cache: like a join, it
  reads more than one relation, and the cache fences one table per entry.
