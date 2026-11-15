# WITH queries (common table expressions)

A `WITH` clause names one or more queries, and the statement after it reads them by name, as it reads a
table. Each named query is a common table expression (CTE). A `WITH RECURSIVE` query can also read
itself, to walk a tree or a graph; see [recursive queries](#recursive-queries). CamusDB uses the
PostgreSQL forms:

```sql
WITH totals AS (
         SELECT account_id, SUM(amount) AS total FROM deltas GROUP BY account_id
     ),
     big (account, amount) AS (
         SELECT account_id, total FROM totals WHERE total > 1000
     )
SELECT a.name, b.amount
  FROM accounts a JOIN big b ON b.account = a.id;
```

## Where a WITH can go

- In front of a `SELECT`, also under `EXPLAIN`. A `SELECT … FOR UPDATE` that reads a CTE is refused
  (`CADB0400`), as for a derived table.
- In front of an `INSERT`, an `UPSERT`, an `UPDATE` or a `DELETE`:
  `WITH s AS (…) UPDATE t SET … WHERE t.id IN (SELECT id FROM s)`.
- In the source query of `INSERT INTO t WITH s AS (…) SELECT …` and of `CREATE TABLE … AS`.
- In a view body and a materialized-view body.
- Inside parentheses: a subquery (`IN`, `EXISTS`, a scalar subquery), a derived table, and an operand of
  `UNION`, `INTERSECT` or `EXCEPT`.

## The definition

`name [(c1, c2, …)] AS [[NOT] MATERIALIZED] (query)`

- The name is not qualified. Two definitions of one `WITH` cannot have the same name (`CADB0400`).
- The column list names the columns of the query in order. A shorter list renames the first columns,
  and the later columns keep their names. A longer list is refused (`CADB0400`). A list can give one
  name two times, as in PostgreSQL; a reference to that name is then ambiguous (`CADB0400`).
- `MATERIALIZED` and `NOT MATERIALIZED` choose how the statement runs the query. See below.
- The query can be any query: joins, `GROUP BY`, window functions, set operations, `ORDER BY` and
  `LIMIT`, and a `WITH` of its own.

A derived table takes a column list too: `SELECT a + b FROM (SELECT 1, 2) AS d (a, b)`.

## Which relation a name reads

- A CTE is visible in the statement after its `WITH`, subqueries included, and in the CTEs after it.
- A CTE is not visible in its own query or in an earlier CTE. There the name reads the table of that
  name. So `WITH items AS (SELECT * FROM items WHERE id < 10) SELECT * FROM items` reads the table in
  the CTE and the CTE in the statement.
- A CTE name hides a table or a view of the same name.
- A `WITH` inside parentheses hides a CTE of the same name from an enclosing `WITH`.
- The target of a write is never a CTE: `WITH t AS (…) UPDATE t …` updates the table `t`. A CTE can be
  read in `UPDATE … FROM`, `DELETE … USING`, and any subquery of the write.
- An index hint on a CTE reference is refused (`CADB0400`): a CTE has no indexes.

## How the statement runs a CTE

The parser writes each reference to a CTE out as a derived table over a copy of the CTE's query. Every
later step (privileges, planning, `EXPLAIN`, spill) is then the step of a derived table. See
[joins](joins.md) for derived tables.

A CTE is **inlined** or **computed once**:

- **Inlined.** Each reader runs its own copy of the query, as a derived table it wrote itself. The
  planner can then push a filter of the reader down. This is the rule for a CTE that the statement reads
  one time, for `NOT MATERIALIZED`, and for every CTE of a `WITH` inside parentheses.
- **Computed once.** The first reader runs the query and keeps its rows for the statement, and every
  other reader reads the kept rows. This is the rule for a CTE of a `WITH` at the start of the
  statement, or at the start of its source query, that the statement reads two or more times, and for
  `MATERIALIZED`. A volatile function in such a CTE, such as `random()`, gives every reader the same
  values.

Each reader applies its own filter to the kept rows, so a filter of one reader does not change the
rows of another. The kept rows are spilled to disk under the rules of
[spill to disk](spill-to-disk.md), and the statement deletes them when it ends.

A CTE that is computed once is computed in full before any reader gets a row, also under a `LIMIT`.
For a large CTE that one reader stops early, `NOT MATERIALIZED` can be faster.

`EXPLAIN` marks each reader of a CTE that is computed once with `computed_once=<name>`.

A `WITH` inside parentheses is always inlined because such a query can read the columns of the query
around it: one set of rows is then not right for every row of the outer query.

## Recursive queries

`WITH RECURSIVE` lets a CTE read itself. The usual use is a walk over a tree or a graph: an org chart,
a bill of materials, a category tree, a path search.

```sql
WITH RECURSIVE walk (id, depth, path) AS (
    SELECT id, 0, CAST(id AS STRING) FROM node WHERE parent_id IS NULL
  UNION ALL
    SELECT n.id, w.depth + 1, w.path || '/' || CAST(n.id AS STRING)
      FROM walk w JOIN node n ON n.parent_id = w.id
)
SELECT id, depth, path FROM walk ORDER BY path;
```

### The form

A CTE that reads itself must be `non-recursive term UNION [ALL] recursive term`:

- The non-recursive term does not read the CTE. It can be any query, also a chain of `UNION ALL`.
- The recursive term reads the CTE one time, in its `FROM` or in a derived table in its `FROM`.
- The `UNION` has no `ORDER BY`, `LIMIT` or `OFFSET` of its own (`CADB0533`). A recursive term in
  parentheses can have them.

These places of the reference are refused (`CADB0400`), as in PostgreSQL. In each of them, a row of one
step would depend on rows that a later step adds:

- a subquery in an expression: `IN (SELECT …)`, `EXISTS`, a scalar subquery;
- the side of an outer join that can be NULL: the right side of `LEFT JOIN`, the left side of
  `RIGHT JOIN`;
- `INTERSECT ALL`, the right side of `EXCEPT`, and the left side of `EXCEPT ALL`;
- a SELECT whose own `FROM` reads the CTE and that has an aggregate. A SELECT over a derived table that
  reads the CTE can aggregate.

Window functions, `DISTINCT`, `GROUP BY` and `HAVING` are allowed in the recursive term.

### Names

Under `WITH RECURSIVE`, every CTE of the list is visible in the query of every other CTE, in any
order, and in its own query. So a CTE can read a CTE that is written after it. A CTE name hides a table
of the same name, also in the CTE's own query. Two or more CTEs that read each other are refused
(`CADB0533`). A CTE of the list that does not read itself is an ordinary CTE.

### Types

The column types are the types of the non-recursive term. A NULL or a placeholder alone in that term
has the type STRING. When the recursive term gives a column another type, the whole query has the
wider type, and the statement fails (`CADB0400`). For example, `SELECT 1 UNION ALL SELECT n + 1.5 …`
fails, because the rows of the first step are INT64. Cast the non-recursive term to the type of the
whole query: `SELECT CAST(1 AS FLOAT64) UNION ALL …`.

### How it runs

The query runs in steps, with the work-table method of PostgreSQL:

1. Step 0 runs the non-recursive term.
2. Each next step runs the recursive term again. The reference to the CTE reads only the rows that
   the step before made (the work table).
3. The query ends after a step that makes no row.

`UNION ALL` keeps every row. `UNION` drops a row that an earlier step or the same step returned, so a
walk over a graph with a cycle ends when no new row appears. Without `UNION`, such a walk does not end.

Each row is returned when its step makes it. So a `LIMIT` over a recursive CTE that the statement reads
one time stops the recursion, also a recursion with no end:

```sql
WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT n FROM t LIMIT 5;
```

A recursive CTE that the statement reads two or more times is computed once, as any CTE is (see
above). That run makes every row before any reader gets one, so a `LIMIT` in a reader does not stop it.

### Memory

The rows of one step are kept for the next step, and they spill to disk under the rules of
[spill to disk](spill-to-disk.md). Only the rows of the last two steps are kept at one time. `UNION`
also keeps one copy of each row that it returned, in memory, to drop a row that comes again.

### Limits of a recursion

A statement has no time limit, so a recursion with no end would run until the disk is full. Two
cluster settings stop it with `CADB0560` (`RecursiveQueryLimitExceeded`). The message names the
setting.

| Setting | Default | What it bounds |
|---|---|---|
| `max_recursive_cte_iterations` | `10000` | The steps of the recursive term that make rows. A recursion of exactly this many steps ends normally. |
| `max_recursive_cte_rows` | `10000000` | The rows of one run of the CTE, the rows of the non-recursive term included. |

Both are runtime settings: `SET CLUSTER SETTING` changes them, and the next statement uses the new
value. A value of `0` turns a limit off. See [request resource limits](request-resource-limits.md).

### EXPLAIN

`EXPLAIN` shows a reader of a recursive CTE as `recursive-union`, with the CTE name and the reader's
alias, and then the plans of the two terms. The reference to the CTE in the recursive term is a
`work-table-scan`. `EXPLAIN (ANALYZE)` does not count the rows of the two terms.

### Not supported

- The `SEARCH` and `CYCLE` clauses of PostgreSQL (`CADB0406`). Use `UNION` to end a walk over a cycle.
- `CREATE RECURSIVE VIEW`. Write the view body with `WITH RECURSIVE`.
- Mutual recursion between CTEs (`CADB0533`), as in PostgreSQL.

## Views

A view stores its body with each CTE written out as a derived table, so `SHOW CREATE VIEW` shows the
derived tables and no `WITH`. The stored text parses again to the same view. Because the stored body
has no `WITH`, a CTE that the body reads two times runs two times when the view is read.

A recursive CTE cannot become a plain derived table. It is stored where the body reads it, as
`(WITH RECURSIVE name (columns) AS (…) SELECT * FROM name) AS alias`. This text also parses again to
the same view. When the recursive query also reads a table of the CTE's name, the CTE is shown under the
first free name, such as `name_1`. That happens after a table rename, or when the query reads the table
through an outer CTE. Its reference keeps the old name as its alias, so the text means the same.

## Limits

- A data-modifying CTE, such as `WITH d AS (DELETE … RETURNING …)`, is not supported.
- A CTE that the statement does not read is not checked. An unknown table in it gives no error.
- The references of one `WITH` can copy at most 250,000 parse nodes (`CADB0413`). A chain of CTEs in
  which each reads the one before two times doubles at each step, and reaches the limit after about
  twenty steps.

See [differences from PostgreSQL](postgresql-differences.md#with-queries) for the answers that differ
from PostgreSQL.
