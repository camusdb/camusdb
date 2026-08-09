# Views

CamusDB supports non-materialized **views**: named, stored queries that are expanded at every
reference. The SQL surface follows PostgreSQL; the places where CamusDB deliberately differs are
listed in [Divergences from PostgreSQL](#divergences-from-postgresql) at the end, and each one says
why.

> **Materialized views are not implemented yet.** `CREATE MATERIALIZED VIEW`, `REFRESH MATERIALIZED
> VIEW`, and their `DROP`/`ALTER`/`SHOW` forms parse but are rejected at execution. See
> [What is not implemented](#what-is-not-implemented).

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

A consequence worth knowing: the **query-result cache is reachable only through a `{cache=name}`
hint**, and hints are refused both inside a view body and on a view reference — so a query that goes
through a view is never served from that cache. It executes live every time.

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

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_view_expansion_depth` | 32 | Backstop on view-over-view nesting depth. |

All settings appear in `SHOW VARIABLES`.

---

## Divergences from PostgreSQL

| Area | CamusDB | Why |
| --- | --- | --- |
| Unnamed expression columns | **Refused** | PostgreSQL names them `?column?`, which nothing can reference. Requiring an alias costs one `AS` and produces a usable view. |
| Rules / `INSTEAD OF` triggers | **Not supported** | CamusDB has neither, so there is no escape hatch that makes a non-auto-updatable view writable. |
| Updatable views | **Not implemented yet** | See below. All views are currently read-only. |
| `DROP TABLE … CASCADE` | **No `CASCADE` form** | Drop the dependent views explicitly. |
| `security_invoker` views | **Not supported** | Only the owner's-rights model is planned. |
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
- **Materialized views.** `CREATE MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, and
  `DROP`/`ALTER MATERIALIZED VIEW`. (`SHOW MATERIALIZED VIEWS` is wired up and simply lists nothing,
  since none can be created.)
- **`ALTER VIEW … OWNER TO`**, and with it the owner's-rights security model. A view's owner is
  recorded at creation, but base-relation privileges are currently checked against the **caller**,
  not the owner — so a view cannot yet be used to grant access to a table the caller lacks
  privileges on.
- **Column-level drop dependencies.** Dropping a *column* a view reads is not currently refused
  (dropping the *table* is).
