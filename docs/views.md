# Views

CamusDB supports **views** and **materialized views**. A view is a named, stored query that is
expanded at each reference. A materialized view stores the query's result as real rows and returns
them until you refresh it. The SQL surface follows PostgreSQL. The places where CamusDB deliberately
differs are listed in [Divergences from PostgreSQL](#divergences-from-postgresql) at the end, and
each one says why.

Most of this document is about views that are not materialized. Materialized views are covered in
[Materialized views](#materialized-views).

---

## Creating a view

The following example creates a view and reads it like a table.

```sql
CREATE VIEW open_orders AS
  SELECT id, customer, total FROM orders WHERE status = 'open';

SELECT customer, SUM(total) AS spent FROM open_orders GROUP BY customer;
```

A view can be given column names that differ from the names in its body.

```sql
CREATE VIEW order_summary (order_id, who, amount) AS
  SELECT id, customer, total FROM orders;
```

### The body is checked when you create it

The tables, columns and functions that the body references are resolved at `CREATE VIEW` time. A
mistake is reported to the author at once, before anyone reads the view.

```sql
CREATE VIEW v AS SELECT id FROM no_such_table;
-- ERROR: table 'no_such_table' does not exist
```

### Every output column needs a name

A projection that is a bare expression does not have a name to publish. CamusDB returns an error for
it.

```sql
CREATE VIEW v AS SELECT total + 1 FROM orders;
-- ERROR (CADB0400): Column 1 of the view body is an expression with no name; add an alias (AS <name>)

CREATE VIEW v AS SELECT total + 1 AS total_plus_one FROM orders;   -- OK
```

PostgreSQL names such a column `?column?`. CamusDB does not allow it, because that name cannot be
referenced. It would appear in `SHOW COLUMNS`, in dependent views and in client column maps as a
column that no query can select. It would also silently renumber if the projection list were
reordered.

### `SELECT *` is expanded when the view is created

In the following example, the view keeps the four columns that `orders` had when the view was
created.

```sql
CREATE VIEW all_orders AS SELECT * FROM orders;   -- orders has 4 columns
ALTER TABLE orders ADD COLUMN note string(64);

SELECT * FROM all_orders;                          -- still 4 columns
```

The view's shape is frozen at creation, matching PostgreSQL. If it were not, adding a column to a
base table would silently widen each `SELECT *` view over it. This would change what dependent views
and clients see, although no statement was issued against them.

A view body may not mix `*` with other projections. The expansion order is not stable when a base
column is added, and the frozen shape could not be honored.

---

## Replacing a view

`CREATE OR REPLACE VIEW` may only **append** columns. Existing column names, types, and order must
be preserved. The last two statements in the following example return an error.

```sql
CREATE VIEW v AS SELECT id, customer FROM orders;

CREATE OR REPLACE VIEW v AS SELECT id, customer, total FROM orders;   -- OK: appended
CREATE OR REPLACE VIEW v AS SELECT id FROM orders;                    -- ERROR (CADB0529)
CREATE OR REPLACE VIEW v AS SELECT id, status FROM orders;            -- ERROR (CADB0529)
```

A dependent view binds to the column names it saw when it was created. A cached plan binds to
positions, and a client binds to both. Silently changing any of them would change what those
already-created objects mean. The change would surface later and elsewhere as wrong data, and no
error would be raised. To change the shape, drop and recreate the view. That forces the dependents
into the open.

The body itself may change freely. The following replacement takes effect on the next read.

```sql
CREATE OR REPLACE VIEW v AS SELECT id, customer FROM orders WHERE status = 'closed';
```

---

## Dependencies

Views record what they read by **object id**. This means that dependencies survive renames.

### Dropping

A drop that would orphan a dependent returns an error. `CASCADE` drops the dependents too.

```sql
CREATE VIEW inner_v AS SELECT id, total FROM orders;
CREATE VIEW outer_v AS SELECT id FROM inner_v WHERE total > 10;

DROP VIEW inner_v;
-- ERROR (CADB0530): Cannot drop view 'inner_v' because other objects depend on it: outer_v.
--                   Use DROP VIEW ... CASCADE to drop them too.

DROP VIEW inner_v CASCADE;   -- drops outer_v as well
```

The same rule applies to tables.

```sql
DROP TABLE orders;
-- ERROR (CADB0530): Cannot drop table 'orders' because other objects depend on it: open_orders.
--                   Drop them first.
```

> **`DROP TABLE` has no `CASCADE` form.** Drop the dependent views first. This makes `DROP TABLE`
> stricter than it was before views existed. The alternative is a table drop that silently turns
> each dependent view into a delayed error for whoever reads it next.

The rule applies to all ways into a drop, for example `DROP TABLE … FORCE` and a statement that a
follower forwards to the schema leader. The ticket API is covered too. The rule is checked before
the drop changes anything. It is checked again in log order when the drop's schema entry is
applied. This means that a view that another node created in between also stops the drop. A drop
that returns this error leaves the table intact, whenever the error arrives. Its rows, its indexes
and its unique enforcement are not changed.

### Columns a view reads

A body binds the columns it reads by immutable id, just as it binds its relations. This means that
**renaming a column is transparent**. Dropping a column that a view reads returns an error. The
view therefore cannot fail at its next read.

```sql
CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open';

ALTER TABLE orders RENAME COLUMN total TO amount;
SELECT id, total FROM open_orders;    -- still works, still called "total"

ALTER TABLE orders DROP COLUMN amount;
-- ERROR (CADB0530): Cannot drop column 'amount' of table 'orders' because other objects
--                   depend on it: open_orders. Drop or replace them first.
```

The view continues to publish `total`. Its output names are frozen at creation and do not follow
the base column. `SHOW CREATE VIEW` renders the base column's *current* name. After the rename, the
body reads `... amount AS total ...`.

A column that the body reads in its `WHERE` clause and nowhere else also counts. Columns read in a
subquery count as well. A materialized view's body protects the columns it reads at each refresh.
`DROP COLUMN` does not have a `CASCADE` form. Drop or replace the dependent view first.

> **A rename returns an error in two cases**, because the body would be stranded. The first case is
> a definition written before columns were bound by id. The second is a reference in `ORDER BY`,
> `GROUP BY` or `HAVING` that matches one of the view's output names. The second is deliberate.
> Those clauses may legally name an output column in place of a relation's column. Such a reference
> is therefore left as written.

> **This check is a lower bound.** A reference that the analysis cannot attribute to one relation
> with certainty is not recorded. An unusually shaped body may therefore let a column be dropped.
> The check errs that way on purpose. A missed error leaves the behavior that existed before this
> check. A wrong error would block a column change that no view depends on.

### Renaming is transparent

A view's stored body refers to the relations it reads by their **immutable ids**. A rename does not
change the view, and there is no stored text to bring up to date.

```sql
CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open';
ALTER TABLE orders RENAME TO sales;

SELECT id FROM open_orders;   -- still works
SHOW CREATE VIEW open_orders;
-- CREATE VIEW `open_orders` AS SELECT id, customer FROM sales AS orders WHERE status = 'open'
```

This applies to renaming a **view** that a view reads, exactly as it does to a table.

Names come back when the definition is shown. The original name appears as an alias on purpose.
Qualified column references in the body resolve through it, and it must not change for the life of
the definition. An alias that would repeat the relation's current name is not printed. This means
that an ordinary view renders as you wrote it.

> **Definitions created by earlier versions** name their relations directly. They continue to work
> and to render unchanged. The first rename that would strand one converts it to the id form. The
> conversion is part of the same replicated change as the rename. It is validated against a single
> schema snapshot and checkpointed together with the rename. This means that a relation created
> under the freed name cannot be picked up by a body that has not caught up. After that conversion
> the view behaves like any other view, and renames no longer touch it.

### Reserved relation names

A table, view or materialized view may not be named with the prefix `__camus_rel_`.

```sql
CREATE TABLE __camus_rel_7 (id int64 PRIMARY KEY);
-- ERROR (CADB0400): Table name '__camus_rel_7' starts with '__camus_rel_', which is reserved
```

Stored view definitions use that prefix to refer to a relation by its immutable id. A relation with
such a name could shadow what a definition points at.

### Cycles

A view that would depend on itself returns an error when you create it.

```sql
CREATE VIEW a AS SELECT id FROM b;
CREATE OR REPLACE VIEW b AS SELECT id FROM a;
-- ERROR (CADB0528): Infinite recursion detected in the definition of view 'b'
```

Nesting is also capped at `max_view_expansion_depth` (default 32) as a backstop.

### Size limits

Depth does not bound how much a view expands to. When each of ten views reads the one below it
twice, a read of the top view expands 1024 copies of the bottom one. `max_view_expanded_relations`
and `max_view_expanded_nodes` bound the expansion of one statement. `CREATE VIEW` expands the views
its body reads under the same budget. A definition like that one returns an error when it is
created.

```sql
CREATE VIEW v10 AS SELECT a.id FROM v9 a JOIN v9 b ON a.id = b.id;
-- ERROR (CADB0563): Expanding the views of this statement substitutes more than 10000 view
--                   references (max_view_expanded_relations) ...
```

One definition is also bounded at `CREATE`. The limits cover its size (`max_view_definition_bytes`,
`max_view_definition_nodes`) and its output columns (`max_view_output_columns`). They also cover the
relations it reads directly (`max_view_direct_dependencies`). See [Configuration](#configuration).

---

## Reading through a view

A view is expanded into a derived table before the query is planned. This means that all features
that work on a subquery also work on a view, for example joins and the cost-based optimizer.
Serializable range/predicate locking on the underlying base tables works too.

```sql
SELECT v.id, c.region
FROM open_orders v INNER JOIN customers c ON v.customer = c.name;
```

There is no "view scan" node in `EXPLAIN`. The plan shows the expanded query, which is what is
executed.

A view is also a source for a statement that writes rows somewhere else. `CREATE TABLE … AS SELECT`
(also `WITH NO DATA`), `INSERT … SELECT` and a materialized view body read it in the same way. Each
column of the new table has the type that a `SELECT` of the view returns, also for a subquery
column. A write *to* a view is covered in [Writing through a view](#writing-through-a-view).

```sql
CREATE TABLE open_copy AS SELECT id, customer FROM open_orders;           -- OK
INSERT INTO archive (id, customer) SELECT id, customer FROM open_orders; -- OK
```

A subquery in the body's `WHERE` behaves as it would in a top-level statement. This includes
`IN (SELECT …)`, `NOT IN`, `EXISTS`, `NOT EXISTS` and scalar subqueries, correlated or not. The same is true of a subquery in
a derived table that the statement writes itself:
`SELECT COUNT(*) FROM (SELECT … WHERE EXISTS (…)) AS sub`. In a view body the subquery reads its
tables with the privileges of the view's owner, like the rest of the body (see
[Ownership and security](#ownership-and-security)). In a hand-written derived table it reads them as
the caller.

A view may be referenced by its name or given an alias.

```sql
SELECT open_orders.customer FROM open_orders;   -- the view's name is the default alias
SELECT o.customer FROM open_orders o;           -- an explicit alias wins
```

### Restrictions on the body

- **No `AS OF SYSTEM TIME`.** An absolute timestamp would freeze the view to one instant forever. A
  relative one (`'-2h'`) would mean something different at each reference. Two readers of the same
  view would then legitimately disagree about its contents. Use
  `CREATE TABLE … AS SELECT … AS OF SYSTEM TIME` for a point-in-time copy.
- **No index or cache hints.** A hint pins a plan choice into the stored definition, where it would
  outlive the statistics it was made against.
- **No `*` in a set operation in a derived table or a subquery.** A body that is
  `UNION`, `INTERSECT` or `EXCEPT` has the `*` of each operand expanded at creation. One nested
  deeper cannot be expanded, and a column added to one of its tables would give its operands
  different column counts. See [set-operations.md](./set-operations.md#views).

**A query that reads through a view is never served from the query-result cache.** The cache fences
one physical table's row keyspace per entry. A view expands to a derived table, which does not have
a keyspace. The same limitation makes joins uncacheable. A `{cache=name}` hint on a view reference
is allowed, and the response shows it as a bypass (`DerivedSource`). This means that the outcome of
the hint is visible. The rows are read from live storage each time.

An **index** hint on a view reference returns an error. A view does not have indexes. The hint could
apply only to a relation that the statement does not name.

A cache hint in a view *body* returns an error at `CREATE VIEW` time. None of this affects
materialized views. A materialized view is a physical relation, and the cache treats it exactly as it
treats a table.

---

## Introspection

The following statements list views and show their definitions and columns.

```sql
SHOW VIEWS;                     -- name and comment
SHOW VIEWS LIKE 'open%';
SHOW CREATE VIEW open_orders;
SHOW COLUMNS FROM open_orders;
```

A view can have a comment, which `SHOW VIEWS` shows in its `comment` column.

```sql
COMMENT ON VIEW open_orders IS 'orders not yet shipped';
COMMENT ON VIEW open_orders IS NULL;   -- removes it; IS '' stores an empty one
```

The comment belongs to the view itself. It is not removed by `CREATE OR REPLACE VIEW` or by
`ALTER VIEW … RENAME TO`. Setting it needs the `ALTER` privilege on the view. `SHOW CREATE VIEW`
does not print it. This means that its output is one statement.

`SHOW TABLES` lists **tables only**. It does not list views. PostgreSQL's `\dt` behaves the same
way, and changing `SHOW TABLES` output would break existing clients.

`SHOW CREATE VIEW` prints the **normalized** definition. This can differ from the text you typed.

```sql
CREATE VIEW v AS SELECT id,total FROM orders WHERE a OR b AND c;
SHOW CREATE VIEW v;
-- CREATE VIEW `v` AS SELECT id, total FROM orders WHERE a OR (b AND c)
```

Views are stored re-rendered. This lets renames rewrite them as targeted AST edits, and gives
`CREATE OR REPLACE` a canonical form to compare against. PostgreSQL's `pg_get_viewdef` also does not
return the original text. The printed DDL is guaranteed to re-parse to the same query. You can send
it straight back to the server.

---

## Writing through a view

`INSERT`, `UPDATE` and `DELETE` can target a view whose body reads one table, as in PostgreSQL. The
statement is rewritten into the same statement on the table. It is executed in your transaction,
under the same locks, limits and constraints as a write to the table.

```sql
CREATE VIEW acme_orders AS
  SELECT id, customer, total FROM orders WHERE customer = 'acme';

INSERT INTO acme_orders (id, customer, total) VALUES (10, 'acme', 5);
UPDATE acme_orders SET total = total + 1 WHERE TRUE;   -- only acme's rows
DELETE FROM acme_orders WHERE total < 20;              -- only acme's rows
```

An `UPDATE` or `DELETE` reaches only the rows that the view shows. The body's `WHERE` is added to the
statement's `WHERE`. A column that the view does not expose takes its default on `INSERT`. It also
cannot be named at all. For example, `DELETE FROM acme_orders WHERE status = 'closed'` returns an
error (`CADB0404`). If a hidden column could be named, a write's row count could disclose what the
column contains. `RETURNING` works, and names the view's columns.

### Which views can be written through

A view can be written through when its body reads **exactly one** table, or one view that can itself
be written through. The body must also not contain any of the following.

- An aggregate or a window function.
- `DISTINCT`, `GROUP BY` or `HAVING`.
- `LIMIT` or `OFFSET`.
- A set operation (`UNION`, `INTERSECT`, `EXCEPT`).
- A subquery in its `WHERE`.

A write to any other view returns `CADB0525`, and the message says which rule the view breaks.
CamusDB does not support rules or `INSTEAD OF` triggers. Such a view is therefore read-only.

An output column that is a column of the table, with no expression around it, can be written. All
other columns are computed. A computed column can be read, also in a `WHERE`, where it means the
expression it stands for. Writing it returns `CADB0526`.

```sql
CREATE VIEW doubled AS SELECT id, total, total * 2 AS twice FROM orders;
UPDATE doubled SET total = 1 WHERE twice = 60;   -- OK
UPDATE doubled SET twice = 1 WHERE id = 1;       -- ERROR (CADB0526)
```

A materialized view cannot be written to, whatever its shape. A view over a materialized view
cannot be written to either. `INSERT … ON CONFLICT`, `UPSERT`, `UPDATE … FROM` and
`DELETE … USING` cannot target a view (`CADB0533`). Write to the table.

### `WITH CHECK OPTION`

With a check option, a row written through the view must be one that the view shows. If it is not,
the statement returns `CADB0527` before anything is written.

```sql
CREATE VIEW acme_only AS
  SELECT id, customer, total FROM orders WHERE customer = 'acme'
  WITH CHECK OPTION;

INSERT INTO acme_only (id, customer, total) VALUES (11, 'globex', 1);
-- ERROR (CADB0527): new row violates check option for view "acme_only"
```

The view does not show a row whose condition is NULL. Such a row fails the check too. In a `CHECK`
constraint, a NULL condition passes. The condition is computed with the types of the base table's
columns, as a `SELECT` through the view computes it. With an `int64` column `i` and a `float64` column
`f`, `COALESCE(i, f) / 4` is `0.5` for the row `i = 2, f = NULL`, in the check and in the `SELECT`.
`WITH CASCADED CHECK OPTION` (the meaning of a bare
`WITH CHECK OPTION`) checks the condition of this view and of each view beneath it.
`WITH LOCAL CHECK OPTION` checks this view's condition. Beneath this view, it checks only the views
that have their own check option.

`CREATE VIEW` returns an error for a check option on a view that cannot be written through. As in
PostgreSQL, it checks the body of the view being created and does not look further. When the body
reads another view, that view's body is checked at the first write. For example, a check-option
view over an aggregate view can be created, but each write through it then returns `CADB0525`. A
later `CREATE OR REPLACE VIEW` of the inner view can have the same effect.

### Privileges

You need `INSERT`, `UPDATE` or `DELETE` on the **view**, as the statement requires. The view's
**owner** needs that privilege on what the view reads (and `SELECT` too, for `UPDATE` and `DELETE`),
as for a read through a view. You don't need a grant on the table. A subquery that you write in the
statement reads its tables as you.

---

## Renaming a view

Renaming a view changes metadata only.

```sql
ALTER VIEW open_orders RENAME TO active_orders;
```

The view's id is unchanged. This means that dependents continue to resolve.

---

## Ownership and security

A view executes its body with the privileges of **its owner**, whoever queries it. This makes a view
a security boundary, as the following example shows.

```sql
-- as alice, who can read orders
CREATE VIEW cheap_orders AS SELECT id, total FROM orders WHERE total < 25;
GRANT SELECT ON shop.cheap_orders TO bob;

-- as bob, who cannot read orders at all
SELECT * FROM cheap_orders;   -- works: returns exactly the rows the view exposes
SELECT * FROM orders;         -- refused
```

The caller is checked on the **view**. The view's owner is checked on all objects the body reads.
This means that a view can widen access to a slice of a table, and to nothing more.

The following rules give the details.

- **The owner is recorded by immutable id**. Dropping a user and re-creating the same name does not
  transfer ownership. The view fails closed. Reads return an error until the view is recreated or
  transferred.
- **`CREATE OR REPLACE VIEW` does not change the owner.** Replacing rewrites the body and does not
  re-own the object. Otherwise, replacing would be a way to seize a view and execute it as yourself.
- **`ALTER VIEW v OWNER TO u`** transfers it. Only a superuser or the current owner may do so. An
  `ALTER` grant on the view is not enough, because ownership decides whose privileges the body is
  executed with. The new owner must already exist.
- **The swap is scoped to the view.** A query can name the same table both through a view and
  directly. It gets the owner's rights for the reference that came through the view only. The direct
  reference is checked against the caller.
- **Each view in a chain is executed as its owner.** When a view's body reads another view, the
  outer view's owner needs `SELECT` on the inner view. This is checked on each read, as in
  PostgreSQL. If you revoke that grant, reads of the outer view fail from then on. If you grant it
  again, they work, and you don't need to replace the view. The reader does not need a grant on the
  inner view.
- **Views are grantable objects**: `GRANT SELECT ON db.my_view TO someone`. To drop, rename, replace
  or describe a view, you need a grant on it. `SHOW VIEWS` lists only the views the caller can reach.
- **Materialized views are not affected by any of this.** Their rows were computed at refresh time.
  Reading one is an ordinary read of a relation, checked against the caller.

`security_invoker` (executing a view as the caller in place of the owner) is not supported.

---

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_view_expansion_depth` | 32 | Backstop on view-over-view nesting depth. |
| `max_view_expanded_relations` | 10000 | View references that expanding one statement may substitute, counting a view as often as it is expanded. |
| `max_view_expanded_nodes` | 2000000 | Syntax-tree nodes that expanding the views of one statement may produce. |
| `max_view_definition_bytes` | 1048576 | Bytes of one normalized view or materialized-view body, checked at `CREATE`. |
| `max_view_definition_nodes` | 200000 | Syntax-tree nodes of one view body, checked at `CREATE`. |
| `max_view_output_columns` | 1600 | Output columns of one view or materialized view, checked at `CREATE`. |
| `max_view_direct_dependencies` | 1000 | Relations one view body reads directly, checked at `CREATE`. |
| `materialized_view_refresh_chunk_rows` | 10000 | Rows written per transaction while rebuilding a materialized view. |
| `materialized_view_refresh_enabled` | true | Set false to disallow refreshes on a node that should not do bulk work. `WITH NO DATA` works in either case. |
| `materialized_view_refresh_takeover_attempts` | 3 | How many times a refresh interrupted by a crash or a leadership change is restarted for you. 0 restarts none of them and only reclaims the abandoned storage. |
| `materialized_view_refresh_hold_lease_ms` | 300000 | Lease on the hold that protects the history a refresh reads from garbage collection. The hold is renewed every lease/3 while the rebuild is in progress. If renewal stops, the refresh fails and the view retains its previous contents. Must be at least 5000. |

All settings appear in `SHOW VARIABLES`.

---

## Materialized views

A materialized view executes its query **once** and stores the rows. Reads are answered from that
stored copy until you refresh it. A view that is not materialized trades storage for freshness. A
materialized view trades freshness for speed.

```sql
CREATE MATERIALIZED VIEW customer_totals AS
  SELECT customer, SUM(total) AS total_spent FROM orders GROUP BY customer;

SELECT * FROM customer_totals WHERE customer = 'acme';   -- reads stored rows, does not touch orders

REFRESH MATERIALIZED VIEW customer_totals;               -- re-runs the query, replaces the contents
```

Inserting into `orders` afterwards does not change what `customer_totals` returns. **A
materialized view is a snapshot, and only `REFRESH` moves it forward.** `SHOW MATERIALIZED VIEWS`
shows how stale each one is.

### It is a real relation

A materialized view is stored as an ordinary relation. This means that almost all statements that
work on a table also work on it.

```sql
CREATE INDEX customer_totals_customer ON customer_totals (customer);
ANALYZE TABLE customer_totals;
COMMENT ON MATERIALIZED VIEW customer_totals IS 'nightly rollup';
SHOW COLUMNS FROM customer_totals;
```

Indexes are preserved across refreshes. Backup and point-in-time recovery treat it as the relation
it is. So do database branching, TTL and the query planner's statistics. A refresh carries the row and index counts it measured onto the view. The
planner then costs it correctly straight away. The histograms that an `ANALYZE` built are dropped at
the same moment. They describe the contents that the refresh replaced.
[`SHOW STATISTICS FOR <view>`](show-statistics.md) shows the statistics it currently has.

You cannot write to a materialized view.

```sql
INSERT INTO customer_totals (customer, total_spent) VALUES ('acme', 1);
-- ERROR: 'customer_totals' is a materialized view and cannot be written to directly
```

The next refresh would silently discard a hand-written row. For this reason, the write returns an
error.

### Its shape is fixed at creation

The column list is derived from the query when the materialized view is created, and a refresh
reuses it. Adding a column to a base table therefore does not widen an existing materialized view.
Views that are not materialized follow the same rule, for the same reason. Anything bound to the
shape would otherwise change meaning, although no statement was issued against it.

As with `CREATE TABLE … AS SELECT`, each output column needs a name. The relation also gets an
extra generated `id` primary key, because a projection does not guarantee uniqueness. An explicit
column list renames the stored columns.

```sql
CREATE MATERIALIZED VIEW open_orders (order_id, amount) AS
  SELECT id, total FROM orders WHERE status = 'open';
```

### `WITH NO DATA`

`WITH NO DATA` creates the materialized view and does not execute the query. **Reading one that
has not been populated is an error.** An empty result would make a forgotten `REFRESH` look the
same as a correct answer.

```sql
CREATE MATERIALIZED VIEW customer_totals AS SELECT ... WITH NO DATA;
SELECT * FROM customer_totals;
-- ERROR: Materialized view 'customer_totals' has not been populated.

REFRESH MATERIALIZED VIEW customer_totals;   -- now it reads
```

`REFRESH MATERIALIZED VIEW … WITH NO DATA` empties one again and returns it to that state.

If you leave out `WITH NO DATA`, the view does not exist for anyone until its rows are in place.
The rows are loaded into a relation that no one can see. The view is then created on it, already
populated, in one atomic schema change. No other session or node can use the view while it is
half-created. For example, it cannot be listed, read or dropped. If the load fails, no data is left behind. If the
process dies during the load, the background sweep removes what it loaded, and running the statement
again starts clean. The name is not reserved during the load. If another statement creates a
relation with that name in the meantime, that statement succeeds, and the `CREATE` then fails
because the name is taken.

### What a refresh does to concurrent readers

A refresh does not affect concurrent readers. It builds an entirely new relation, populates it, and
then moves the materialized view's name onto it in a single atomic schema change. Readers that are
already in progress continue to read the previous contents at their snapshot. Readers that start
after the switch see all of the new contents. **No reader is blocked, and no reader ever observes a
half-built materialized view**, even briefly, on any node of a cluster.

For this reason, `REFRESH MATERIALIZED VIEW CONCURRENTLY` **returns an error**. It is not a synonym
of the ordinary form. In PostgreSQL, `CONCURRENTLY` exists because the ordinary form takes an
exclusive lock and blocks readers. CamusDB's ordinary form does not block readers. The other benefit
of `CONCURRENTLY` is that it writes only the rows that changed. That optimization is not
implemented, and the statement returns an error.

The rebuild reads its source at **one pinned snapshot** for its complete duration. This means that
the result is always a state that the database was in, however long the rebuild takes and however
many rows it copies. It writes in chunked transactions (`materialized_view_refresh_chunk_rows`).
This lets it exceed the per-transaction mutation limit, which would otherwise cap the size of a
materialized view.

If a refresh fails, the materialized view is left exactly as it was. A failed rebuild is discarded,
and no part of it is published.

Only one refresh of a given materialized view can be in progress at a time, anywhere in the
cluster. A second one returns an error while the first is in flight, whichever node it was issued
on.

A refresh can be **interrupted** without failing, for example when the process dies or the node
loses leadership part-way. A background sweep picks it up and **restarts it from the beginning**,
up to `materialized_view_refresh_takeover_attempts` times. The sweep takes the same cluster fence
that a `REFRESH` takes. This means that exactly one node finishes the work and that two rebuilds of
one view never execute at the same time. The refresh restarts because continuing a half-finished
scan would require the body to return rows in the same order on each execution. That is true of a
single-relation scan. It is not promised for a join or an aggregate. Resuming one of those would
give you a materialized view with duplicated and missing rows. Until a restart completes, the view
continues to serve its previous contents. When the attempts are used up, the view is left stale and
an error is logged that names it. You must then issue `REFRESH` again.

### Dependencies and lifecycle

The following statements rename and drop a materialized view.

```sql
ALTER MATERIALIZED VIEW customer_totals RENAME TO totals_by_customer;
DROP MATERIALIZED VIEW customer_totals;
DROP MATERIALIZED VIEW IF EXISTS customer_totals CASCADE;
```

A view may read a materialized view, and a materialized view may read a view. Dropping a
materialized view that another view reads returns an error unless you add `CASCADE`.

```sql
DROP MATERIALIZED VIEW customer_totals;
-- ERROR (CADB0530): Cannot drop materialized view 'customer_totals' because other objects depend
--                   on it: big_spenders. Use DROP MATERIALIZED VIEW ... CASCADE to drop them too.
```

`CASCADE` works the same way on `DROP MATERIALIZED VIEW` and on `DROP VIEW`. It drops all views and
materialized views that read the object, directly or through other views, before the object itself.
You need the privilege to drop each of them. All of them are checked before any is dropped. Renaming
a materialized view does not affect the views that read it.

**A drop does not wait for a refresh.** `DROP MATERIALIZED VIEW` returns `CADB0532`
(`RefreshAlreadyInProgress`) while a refresh of that view is in progress anywhere in the cluster.
Retry it once the refresh has finished. Index and constraint DDL behaves differently. It does not
wait either, but it commits, and the refresh that is in progress fails with `CADB0534`
(`ConcurrentSchemaChange`). Retry the refresh.

Tables, views and materialized views share **one namespace**. You cannot have a table and a
materialized view with the same name. Each kind is dropped and renamed by a separate statement.
`DROP TABLE` and `ALTER TABLE … RENAME TO` return an error for a materialized view, and the error
tells you which statement to use.

### Introspection

The following statements list materialized views and show their definitions, columns and indexes.

```sql
SHOW MATERIALIZED VIEWS;             -- name, whether it holds data, the snapshot it holds, and its comment
SHOW MATERIALIZED VIEWS LIKE 'cust%';
SHOW CREATE MATERIALIZED VIEW customer_totals;
SHOW COLUMNS FROM customer_totals;
SHOW INDEXES FROM customer_totals;
```

`SHOW TABLES` lists **tables only**. It lists neither views nor materialized views.
`SHOW CREATE MATERIALIZED VIEW` prints `WITH NO DATA` for an unpopulated one. Its output therefore
re-creates the same unpopulated object.

---

## Divergences from PostgreSQL

| Area | CamusDB | Why |
| --- | --- | --- |
| Unnamed expression columns | **Refused** | PostgreSQL names them `?column?`, which no query can reference. Requiring an alias costs one `AS` and produces a usable view. |
| Rules / `INSTEAD OF` triggers | **Not supported** | CamusDB supports neither. There is no escape hatch that makes a non-auto-updatable view writable. |
| Subquery in the `WHERE` of a view you write through | **Refused** (`CADB0525`) | The `WHERE` is added to the write, which is executed as you. A subquery there would read its tables as you, and the view's owner would not be used. |
| `ON CONFLICT`, `UPSERT`, `UPDATE … FROM`, `DELETE … USING` on a view | **Refused** (`CADB0533`) | Write to the table. |
| `REFRESH … CONCURRENTLY` | **Refused** | The ordinary refresh already leaves readers unblocked, which is what it buys in PostgreSQL; writing only the changed rows is not implemented. |
| Writing to a materialized view | **Refused** | PostgreSQL also returns an error; the row would be discarded by the next refresh either way. |
| `DROP TABLE … CASCADE` | **No `CASCADE` form** | Drop the dependent views explicitly. |
| `security_invoker` views | **Not supported** | Only the owner's-rights model is implemented. |
| `WITH` in a body | **Stored without the `WITH`** | Each CTE is stored as a derived table where the body reads it. `SHOW CREATE VIEW` therefore does not show a `WITH`, and a CTE that the body reads two times is executed two times. See [WITH queries](common-table-expressions.md#views). |
| `WITH RECURSIVE` in a body | **Stored where it is read** | A recursive CTE cannot become a simple derived table. It is stored as `(WITH RECURSIVE name AS (…) SELECT * FROM name) AS alias` where the body reads it. See [WITH queries](common-table-expressions.md#views). `CREATE RECURSIVE VIEW` is not supported. |
| Cross-database views | **Not supported** | Matches the existing single-database restriction on `INSERT … SELECT`. |
| Temporary views | **Not supported** | CamusDB does not support temporary relations. |
| Mixing `*` with other projections | **Refused** | The expansion order is not stable when a base column is added, and the frozen shape could not be honored. |
| `AS OF SYSTEM TIME` in a body | **Refused** | Neither an absolute nor a relative timestamp can be stored coherently (see above). |

---

## What is not implemented

The following features parse but are **rejected at execution**. They return an error.

- **Incremental refresh** (`REFRESH MATERIALIZED VIEW … CONCURRENTLY`), which would write only the
  rows that changed in place of a rebuild. The ordinary form works and does not block readers.
- **Resuming an interrupted refresh mid-scan.** An interrupted refresh is restarted from the
  beginning (see above). The work that the dead run did is done again. The materialized view is not
  changed in the meantime, and no inconsistent state is left.
- **Absorbing a column rename into a definition written before columns were bound by id**, or into a
  reference in `ORDER BY` / `GROUP BY` / `HAVING` that matches one of the view's output names.
  Both return an error (see *Columns a view reads*).
