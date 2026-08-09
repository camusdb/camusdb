# Views

CamusDB supports **views** — named, stored queries expanded at every reference — and **materialized
views**, which store the query's result as real rows and hand them back until you refresh them. The
SQL surface follows PostgreSQL; the places where CamusDB deliberately differs are listed in
[Divergences from PostgreSQL](#divergences-from-postgresql) at the end, and each one says why.

Most of this document is about plain views. Materialized views have their own section:
[Materialized views](#materialized-views).

---

## Creating a view

```sql
CREATE VIEW open_orders AS
  SELECT id, customer, total FROM orders WHERE status = 'open';

SELECT customer, SUM(total) AS spent FROM open_orders GROUP BY customer;
```

A view can be given its own column names:

```sql
CREATE VIEW order_summary (order_id, who, amount) AS
  SELECT id, customer, total FROM orders;
```

### The body is checked when you create it

Every table, column, and function the body references is resolved at `CREATE VIEW` time, so a
mistake is reported to the author immediately rather than to whoever reads the view first:

```sql
CREATE VIEW v AS SELECT id FROM no_such_table;
-- ERROR: table 'no_such_table' does not exist
```

### Every output column needs a name

A projection that is a bare expression has no name to publish, and CamusDB refuses it rather than
inventing one:

```sql
CREATE VIEW v AS SELECT total + 1 FROM orders;
-- ERROR (CADB0400): Column 1 of the view body is an expression with no name; add an alias (AS <name>)

CREATE VIEW v AS SELECT total + 1 AS total_plus_one FROM orders;   -- OK
```

PostgreSQL names such a column `?column?`. CamusDB refuses because that name cannot be referenced —
it would appear in `SHOW COLUMNS`, in dependent views, and in client column maps as something
nothing can select, and it would silently renumber if the projection list were reordered.

### `SELECT *` is expanded when the view is created

```sql
CREATE VIEW all_orders AS SELECT * FROM orders;   -- orders has 4 columns
ALTER TABLE orders ADD COLUMN note string(64);

SELECT * FROM all_orders;                          -- still 4 columns
```

The view's shape is frozen at creation, matching PostgreSQL. Without this, adding a column to a base
table would silently widen every `SELECT *` view over it, changing what dependent views and clients
see with no statement having been issued against them.

A view body may not mix `*` with other projections — the expansion order is not stable across a base
column being added, so the frozen shape could not be honored.

---

## Replacing a view

`CREATE OR REPLACE VIEW` may only **append** columns. Existing column names, types, and order must
be preserved:

```sql
CREATE VIEW v AS SELECT id, customer FROM orders;

CREATE OR REPLACE VIEW v AS SELECT id, customer, total FROM orders;   -- OK: appended
CREATE OR REPLACE VIEW v AS SELECT id FROM orders;                    -- ERROR (CADB0529)
CREATE OR REPLACE VIEW v AS SELECT id, status FROM orders;            -- ERROR (CADB0529)
```

This is not pedantry. A dependent view binds to the column names it saw at its own creation, a
cached plan binds to positions, and a client binds to both. Silently changing any of them would
change what those already-created objects mean, and the change would surface later and elsewhere as
wrong data rather than as an error. Drop and recreate to change the shape — which forces the
dependents into the open, and is the point.

The body itself may change freely:

```sql
CREATE OR REPLACE VIEW v AS SELECT id, customer FROM orders WHERE status = 'closed';
```

takes effect on the next read.

---

## Dependencies

Views record what they read by **object id**, not by name, so dependencies survive renames.

### Dropping

A drop that would orphan a dependent is refused:

```sql
CREATE VIEW inner_v AS SELECT id, total FROM orders;
CREATE VIEW outer_v AS SELECT id FROM inner_v WHERE total > 10;

DROP VIEW inner_v;
-- ERROR (CADB0530): Cannot drop view 'inner_v' because other objects depend on it: outer_v.
--                   Use DROP VIEW ... CASCADE to drop them too.

DROP VIEW inner_v CASCADE;   -- drops outer_v as well
```

The same rule applies to tables:

```sql
DROP TABLE orders;
-- ERROR (CADB0530): Cannot drop table 'orders' because other objects depend on it: open_orders.
```

> **`DROP TABLE` has no `CASCADE` form.** Drop the dependent views first. This makes `DROP TABLE`
> stricter than it was before views existed; the alternative is a table drop that silently converts
> every dependent view into a delayed error for whoever reads it next.

### Renaming is transparent

Renaming a base table rewrites every dependent view's stored body, so the views keep working:

```sql
CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open';
ALTER TABLE orders RENAME TO sales;

SELECT id FROM open_orders;   -- still works
SHOW CREATE VIEW open_orders;
-- CREATE VIEW `open_orders` AS SELECT id, customer FROM sales AS orders WHERE status = 'open'
```

The old name is kept as an alias on purpose: qualified column references inside the body resolve
through it.

The rewrite is an **AST edit**, never a textual substitution — a matching word inside a string
literal or a column name is left alone.

> **Rename caveat.** The rename and each dependent view's rewrite are separate replicated changes.
> There is a brief window in which the rename has landed but a given view's rewrite has not, during
> which reading that view fails with "table does not exist". The window cannot produce a *wrong*
> answer — the stale body names a relation that is simply gone — but it is a window. Folding them
> into one change is a planned improvement.

### Cycles

A view that would depend on itself is rejected when you create it:

```sql
CREATE VIEW a AS SELECT id FROM b;
CREATE OR REPLACE VIEW b AS SELECT id FROM a;
-- ERROR (CADB0528): Infinite recursion detected in the definition of view 'b'
```

Nesting is additionally capped at `max_view_expansion_depth` (default 32) as a backstop.

---

## Reading through a view

A view is expanded into a derived table before the query is planned, so everything that works on a
subquery works on a view: joins, aggregation, `DISTINCT`, `ORDER BY`, subqueries, the cost-based
optimizer, spill, and serializable range/predicate locking on the underlying base tables.

```sql
SELECT v.id, c.region
FROM open_orders v INNER JOIN customers c ON v.customer = c.name;
```

There is no "view scan" node in `EXPLAIN` — the plan shows what actually runs, which is the expanded
query.

A view may be referenced by its own name or given an alias:

```sql
SELECT open_orders.customer FROM open_orders;   -- the view's name is the default alias
SELECT o.customer FROM open_orders o;           -- an explicit alias wins
```

### Restrictions on the body

- **No `AS OF SYSTEM TIME`.** An absolute timestamp would freeze the view to one instant forever; a
  relative one (`'-2h'`) would mean something different at every reference, so two readers of the
  same view would legitimately disagree about its contents. Use
  `CREATE TABLE … AS SELECT … AS OF SYSTEM TIME` for a point-in-time copy.
- **No index or cache hints.** A hint pins a plan choice into the stored definition, where it would
  outlive the statistics it was made against.

Applying a hint *to* a view is likewise refused: a view has no indexes of its own, and the hint would
silently target a relation the statement does not name.

A consequence worth knowing: **a query that reads through a view is never served from the
query-result cache.** The cache fences one physical table's row keyspace per entry, and a view
expands to a derived table, which has no keyspace of its own — the same limitation that makes joins
uncacheable. A `{cache=name}` hint on a view reference is accepted and the response reports it as a
bypass (`DerivedSource`) rather than failing, so the hint is visible rather than silent, but the rows
come from live storage every time.

An **index** hint on a view reference is still refused: a view has no indexes of its own, so the hint
could only be applied to a relation the statement does not name.

A cache hint inside a view *body* is refused at `CREATE VIEW` time, and materialized views are
unaffected by any of this — a materialized view is a physical relation, so the cache treats it
exactly as it treats a table.

---

## Introspection

```sql
SHOW VIEWS;                     -- names only
SHOW VIEWS LIKE 'open%';
SHOW CREATE VIEW open_orders;
SHOW COLUMNS FROM open_orders;
```

`SHOW TABLES` lists **tables only** — it does not list views. (PostgreSQL's `\dt` behaves the same
way, and changing `SHOW TABLES` output would break existing clients.)

`SHOW CREATE VIEW` prints the **normalized** definition, not the text you typed:

```sql
CREATE VIEW v AS SELECT id,total FROM orders WHERE a OR b AND c;
SHOW CREATE VIEW v;
-- CREATE VIEW `v` AS SELECT id, total FROM orders WHERE a OR (b AND c)
```

Views are stored re-rendered so that renames can rewrite them as targeted AST edits and so that
`CREATE OR REPLACE` has a canonical form to compare against. PostgreSQL's `pg_get_viewdef` also never
returns the original text. The printed DDL is guaranteed to re-parse to the same query, so it can be
fed straight back to the server.

---

## Renaming a view

```sql
ALTER VIEW open_orders RENAME TO active_orders;
```

Metadata-only; the view's id is unchanged, so dependents keep resolving.

---

## Ownership and security

A view runs its body with the privileges of **its owner**, not of whoever queries it. That is what
makes a view a security boundary rather than a shorthand:

```sql
-- as alice, who can read orders
CREATE VIEW cheap_orders AS SELECT id, total FROM orders WHERE total < 25;
GRANT SELECT ON shop.cheap_orders TO bob;

-- as bob, who cannot read orders at all
SELECT * FROM cheap_orders;   -- works: returns exactly the rows the view exposes
SELECT * FROM orders;         -- refused
```

The caller is checked on the **view**; the view's owner is checked on everything the body reads. So a
view can widen access to a slice of a table, and nothing more.

Some specifics worth knowing:

- **The owner is recorded by immutable id**, not by name. Dropping a user and re-creating the same
  name does not transfer ownership — the view fails closed instead, and reads refuse until it is
  recreated or transferred.
- **`CREATE OR REPLACE VIEW` does not change the owner.** Replacing rewrites the body; it does not
  re-own the object, or replacing would be a way to seize a view and run it as yourself.
- **`ALTER VIEW v OWNER TO u`** transfers it. Only a superuser or the current owner may do so — an
  `ALTER` grant on the view is not enough, because ownership decides whose privileges the body runs
  with. The new owner must already exist.
- **The swap is scoped to the view.** A query naming the same table both through a view and directly
  gets the owner's rights only for the reference that came through the view; the direct one is still
  checked against the caller.
- **Each view in a chain runs as its own owner.**
- **Views are grantable objects**: `GRANT SELECT ON db.my_view TO someone`. Dropping, renaming,
  replacing and describing one all require a grant on the view, and `SHOW VIEWS` lists only what the
  caller can reach.
- **Materialized views are not affected by any of this.** Their rows were computed at refresh time;
  reading one is an ordinary read of a relation, checked against the caller.

`security_invoker` (running a view as the caller instead of the owner) is not supported.

---

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_view_expansion_depth` | 32 | Backstop on view-over-view nesting depth. |
| `materialized_view_refresh_chunk_rows` | 10000 | Rows written per transaction while rebuilding a materialized view. |
| `materialized_view_refresh_enabled` | true | Set false to refuse refreshes on a node that should not run bulk work. `WITH NO DATA` still works. |

All settings appear in `SHOW VARIABLES`.

---

## Materialized views

A materialized view runs its query **once**, stores the rows, and answers every read from that
stored copy until you refresh it. Where a plain view trades storage for freshness, a materialized
view trades freshness for speed.

```sql
CREATE MATERIALIZED VIEW customer_totals AS
  SELECT customer, SUM(total) AS total_spent FROM orders GROUP BY customer;

SELECT * FROM customer_totals WHERE customer = 'acme';   -- reads stored rows, does not touch orders

REFRESH MATERIALIZED VIEW customer_totals;               -- re-runs the query, replaces the contents
```

Inserting into `orders` afterwards changes nothing that `customer_totals` returns. That is the
point, and it is the one behavior to internalize: **a materialized view is a snapshot, and only
`REFRESH` moves it forward.** `SHOW MATERIALIZED VIEWS` reports how stale each one is.

### It is a real relation

A materialized view is stored as an ordinary relation, so almost everything that works on a table
works on it:

```sql
CREATE INDEX customer_totals_customer ON customer_totals (customer);
ANALYZE TABLE customer_totals;
COMMENT ON TABLE customer_totals IS 'nightly rollup';
SHOW COLUMNS FROM customer_totals;
```

Indexes are kept across refreshes. Backup and point-in-time recovery, database branching, TTL and
the query planner's statistics all treat it as the relation it is.

The one thing you cannot do is write to it:

```sql
INSERT INTO customer_totals (customer, total_spent) VALUES ('acme', 1);
-- ERROR: 'customer_totals' is a materialized view and cannot be written to directly
```

A hand-written row would be silently discarded by the next refresh, so the write is refused instead
of accepted and later erased.

### Its shape is fixed at creation

The column list is derived from the query when the materialized view is created, and a refresh
reuses it rather than re-deriving it. Adding a column to a base table therefore does not widen an
existing materialized view — the same rule plain views follow, and for the same reason: anything
bound to its shape would otherwise change meaning with no statement having been issued against it.

As with `CREATE TABLE … AS SELECT`, every output column needs a name, and the relation gets an
extra generated `id` primary key because a projection carries no uniqueness guarantee of its own.
An explicit column list renames the stored columns:

```sql
CREATE MATERIALIZED VIEW open_orders (order_id, amount) AS
  SELECT id, total FROM orders WHERE status = 'open';
```

### `WITH NO DATA`

`WITH NO DATA` creates the materialized view without running the query. **Reading one that has never
been populated is an error, not an empty result** — an empty result would make a forgotten `REFRESH`
indistinguishable from a correct answer:

```sql
CREATE MATERIALIZED VIEW customer_totals AS SELECT ... WITH NO DATA;
SELECT * FROM customer_totals;
-- ERROR: Materialized view 'customer_totals' has not been populated.

REFRESH MATERIALIZED VIEW customer_totals;   -- now it reads
```

`REFRESH MATERIALIZED VIEW … WITH NO DATA` empties one again and returns it to that state.

### What a refresh does to concurrent readers

Nothing. A refresh builds an entirely new relation, populates it, and then moves the materialized
view's name onto it in a single atomic schema change. Readers already running keep reading the
previous contents at their own snapshot; readers that start after the switch see the new contents
whole. **Nobody blocks, and nobody ever observes a half-built materialized view** — not even briefly,
and not on any node of a cluster.

This is why `REFRESH MATERIALIZED VIEW CONCURRENTLY` is **refused** rather than accepted as a
synonym. In PostgreSQL, `CONCURRENTLY` exists because the ordinary form takes an exclusive lock and
blocks readers; CamusDB's ordinary form already does not. What `CONCURRENTLY` additionally buys —
writing only the rows that changed — is a genuine optimization that is not implemented, so the
statement says so instead of quietly doing something else.

The rebuild reads its source at **one pinned snapshot** for its whole duration, so the result is
always a state the database actually was in, however long the rebuild takes and however many rows it
copies. It writes in chunked transactions (`materialized_view_refresh_chunk_rows`), which is what
lets it exceed the per-transaction mutation limit that would otherwise cap the size of a
materialized view.

If a refresh fails, the materialized view is left exactly as it was — a failed rebuild is discarded,
never partially published.

### Dependencies and lifecycle

```sql
ALTER MATERIALIZED VIEW customer_totals RENAME TO totals_by_customer;
DROP MATERIALIZED VIEW customer_totals;
DROP MATERIALIZED VIEW IF EXISTS customer_totals CASCADE;
```

A plain view may read a materialized view. Dropping the materialized view out from under it is
refused unless you say `CASCADE`, exactly as for a table. Renaming one rewrites the views that read
it, so they keep working.

Tables, views and materialized views share **one namespace**: you cannot have a table and a
materialized view with the same name, and each kind is dropped by its own statement — `DROP TABLE`
refuses a materialized view and tells you which statement to use.

### Introspection

```sql
SHOW MATERIALIZED VIEWS;             -- name, whether it holds data, and the snapshot it holds
SHOW MATERIALIZED VIEWS LIKE 'cust%';
SHOW CREATE MATERIALIZED VIEW customer_totals;
SHOW COLUMNS FROM customer_totals;
SHOW INDEXES FROM customer_totals;
```

`SHOW TABLES` lists **tables only** — neither views nor materialized views. `SHOW CREATE MATERIALIZED
VIEW` prints `WITH NO DATA` for an unpopulated one, so its output re-creates the same object rather
than a populated lookalike.

---

## Divergences from PostgreSQL

| Area | CamusDB | Why |
| --- | --- | --- |
| Unnamed expression columns | **Refused** | PostgreSQL names them `?column?`, which nothing can reference. Requiring an alias costs one `AS` and produces a usable view. |
| Rules / `INSTEAD OF` triggers | **Not supported** | CamusDB has neither, so there is no escape hatch that makes a non-auto-updatable view writable. |
| Updatable views | **Not implemented yet** | See below. All views are currently read-only. |
| `REFRESH … CONCURRENTLY` | **Refused** | The ordinary refresh already leaves readers unblocked, which is what it buys in PostgreSQL; writing only the changed rows is not implemented, and pretending otherwise would be worse than refusing. |
| Writing to a materialized view | **Refused** | PostgreSQL refuses too; the row would be discarded by the next refresh either way. |
| `DROP TABLE … CASCADE` | **No `CASCADE` form** | Drop the dependent views explicitly. |
| `security_invoker` views | **Not supported** | Only the owner's-rights model is implemented. |
| `WITH RECURSIVE` views | **Not supported** | CamusDB has no `WITH RECURSIVE`. |
| Cross-database views | **Not supported** | Matches the existing single-database restriction on `INSERT … SELECT`. |
| Temporary views | **Not supported** | CamusDB has no temporary relations. |
| Mixing `*` with other projections | **Refused** | The expansion order is not stable across a base column being added, so the frozen shape could not be honored. |
| `AS OF SYSTEM TIME` in a body | **Refused** | Neither an absolute nor a relative timestamp is storable coherently (see above). |

---

## What is not implemented

These parse but are **rejected at execution**, so they fail loudly rather than doing something
unexpected:

- **Updatable views.** `INSERT`/`UPDATE`/`DELETE` through a view, the auto-updatability rules, and
  `WITH CHECK OPTION` enforcement. The `WITH [LOCAL|CASCADED] CHECK OPTION` clause parses and is
  stored, but nothing enforces it yet. **All views are read-only.**
- **Incremental refresh** (`REFRESH MATERIALIZED VIEW … CONCURRENTLY`), which would write only the
  rows that changed instead of rebuilding. The plain form works and does not block readers.
- **Resuming an interrupted refresh.** A refresh that dies with the process, or whose node loses
  leadership part-way, is not picked up where it stopped — run it again. The materialized view is
  untouched in the meantime, so nothing is left inconsistent; the abandoned partial build is cleaned
  up by the next refresh of the same materialized view.
- **Fencing concurrent refreshes across nodes.** A second `REFRESH` of the same materialized view on
  the *same* node is refused while one is running. Two nodes refreshing it simultaneously are not
  coordinated: one of the two fails, and the materialized view ends up holding one run's complete
  output — never a mixture of both.
- **Re-checking a nested view's grant at read time.** A view whose body reads another view has that
  inner reference checked when it is *created*, not on every read; a grant revoked afterwards does not
  break the outer view until it is replaced. The inner view still runs as its own owner, so this
  widens nothing beyond what its author could already reach.
- **Column-level drop dependencies.** Dropping a *column* a view reads is not currently refused
  (dropping the *table* is).
