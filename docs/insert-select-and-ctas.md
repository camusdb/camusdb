# `INSERT INTO … SELECT` and `CREATE TABLE … AS SELECT`

Two statements that take their rows from a query.

```sql
-- Copy rows between tables
INSERT INTO archived_orders (id, customer, total)
SELECT id, customer, total FROM orders WHERE created_at < '2026-01-01';

-- Copy every column, in schema order
INSERT INTO archived_orders SELECT * FROM orders WHERE status = 'closed';

-- Build a new table from a query's result
CREATE TABLE order_totals AS
SELECT customer, SUM(total) AS total_spent FROM orders GROUP BY customer;

-- Structure only, no rows
CREATE TABLE IF NOT EXISTS orders_empty AS SELECT * FROM orders WITH NO DATA;
```

The source is an ordinary `SELECT`: joins, `GROUP BY`, `DISTINCT`, subqueries, `ORDER BY`,
`LIMIT`/`OFFSET` and parameters all work exactly as they do in a standalone query.

## `INSERT INTO … SELECT`

**Columns are matched by position, and their names are ignored.** The source's *k*-th output column
feeds the *k*-th target column, whatever either is called.

```sql
-- b's values land in a, and a's in b
INSERT INTO pairs (a, b) SELECT b, a FROM pairs;
```

The two lists must have the same number of columns. A mismatch returns an error before the query is
executed. If the statement doesn't name the target columns, the target is all columns of the table in
schema order — the same rule that `INSERT … VALUES` uses.

An `OVERRIDING SYSTEM VALUE` or `OVERRIDING USER VALUE` clause goes between the column list and the
`SELECT`, as in `INSERT INTO t (id, n) OVERRIDING SYSTEM VALUE SELECT id, n FROM old_t`. It decides
what happens to the values supplied for identity columns; see
[sequences](sequences.md#overriding-system-value-and-overriding-user-value).

Values are coerced to the target column's declared type. Target columns that the statement does not
mention take their column default. A function default such as `gen_uuid_v7()` is evaluated **per
row**, which means that each row gets a separate value. The rows are written by the same insert path
as `INSERT … VALUES`. This means that constraints such as NOT NULL and CHECK are enforced exactly
as for `INSERT … VALUES`, and so are unique indexes and TTL.

The statement is all-or-nothing: a violation on any row aborts the statement. The result is the
number of rows inserted; an empty source is a success with `0`.

`ORDER BY` in the source is allowed, but it has no effect on storage. Rows are stored by a generated
row id, in an order unrelated to the insertion order.

### Copying a table into itself

`INSERT INTO t SELECT … FROM t` is supported and terminates. The engine reads the entire source
before writing the first row. This means that the scan cannot observe the rows the statement is
inserting.

### Limits

A transaction may not insert more than `max_mutations_per_transaction` rows (default 20 000), and
this statement is bounded by that. A larger copy returns `CADB0506`. Split it with `WHERE` or
`LIMIT` and execute it in several transactions.

### Concurrent writers on the source

The transaction's isolation level and locking mode decide what the source scan does about a
concurrent writer of the source table. The statement itself does not change this behavior.

- **Serializable.** The source scan decides what is written. For this reason it takes an
  **exclusive range lock** over the range it scanned, and the lock lasts until the end of the
  transaction, exactly as for an `UPDATE`'s locate scan. A large copy therefore blocks writes to that
  range until it commits. (Under key-range sharding this lock is taken at all isolation levels.)
- **Read Committed, pessimistic locking.** The scan doesn't take a lock and doesn't record anything. The copy is a
  non-repeatable read of the rows as they were when the scan passed them, which is what this level
  permits, and a writer that changes a source row afterwards does not affect the commit.
- **Optimistic locking, at either level.** Each key the scan *visits* becomes a commit dependency.
  The commit returns `CADB0502` if any of them was rewritten by a transaction that committed in the
  meantime. The dependency is on the row. A write to a column that the copy doesn't read counts too.
  The dependencies cover all rows the scan passed over, including rows the `WHERE` clause filtered
  out. This is because the filter is applied after the store returned the page. For example, a full scan of a
  20,000-row table that copies 5,000 of them depends on all 20,000. A long copy is also exposed while
  it reads, as well as at commit. This is because a page the store has to retry is re-checked against the same pins.
  A bulk copy out of a table under sustained writes therefore does not commit under optimistic
  locking: each retry reads the source again and fails again.

  To copy out of a busy table, use one of these methods.

  - give the source an `AS OF SYSTEM TIME` clause (see below) — a historical source takes no lock and
    no dependency, and the writes are made in your transaction;
  - run the statement pessimistically — `"locking": "Pessimistic"` on the request, or
    `SET TRANSACTION LOCKING PESSIMISTIC` before the first statement of an explicit transaction
    (see [transactions, locking and isolation](transactions-locking-and-isolation.md#64-pessimistic-vs-optimistic-an-advanced-opt-in-choice));
  - or copy in slices small enough that each one commits before the writers reach its rows.

For the same reason, the engine's bulk jobs (index backfills, `ADD COLUMN` fills) use a pessimistic
transaction for each batch, whatever the configured default.

## `CREATE TABLE … AS SELECT`

The new table's columns are the source query's output columns, with the types that query would
return to a client. The new table inherits only the shape of the result. **Indexes and constraints
such as CHECK and NOT NULL are not copied** from the source table. Defaults, comments and table
settings are not copied either.

A statement parameter in the select list has the type of its value. This is because the statement
stores the rows it reads with that value: with `@px` a `float64`, `CREATE TABLE t AS SELECT @px * 1.0 AS c FROM v`
creates a `float64` column. A parameter whose value is NULL gives a `string` column, as a value of
unknown type does in PostgreSQL; write `CAST(@px AS float64)` to declare another type.

Add `WITH NO DATA` to create an empty table. `WITH DATA`, which loads the rows, is the default. With
`IF NOT EXISTS`, an existing table makes the statement a no-op, and the source query is not executed.

### The primary key

A CamusDB table needs a primary key, and a query result doesn't have one. For this reason, a key is
**synthesized**: a leading `id oid NOT NULL DEFAULT gen_id()` column that the source does not fill. If the query
already outputs a column called `id`, the synthesized one becomes `id2` (then `id3`, …).

A projected column is never reused as the key. A projection need not be unique: a join or a
non-distinct projection repeats values. A repeated value would make the copy fail partway with a
duplicate key.

### Projections that are rejected

Each output column has to become a named, typed column. Three shapes return an error that explains
the problem.

| Rejected | Why | Fix |
| --- | --- | --- |
| `SELECT year + 1 FROM t` | an unaliased expression doesn't have a name (it would be called `0`) | `SELECT year + 1 AS next_year` |
| `SELECT NULL AS x` | no type to declare | `SELECT CAST(NULL AS INT64) AS x` |
| `SELECT * FROM a JOIN b …`, where `a` and `b` both have `id` | `*` over a join gives two columns named `id` | list the columns with aliases |

### Schema and data are not committed together

Creating the table commits separately — in cluster mode it is replicated through Raft — **before** any
row is written. The two are not one atomic unit, and this has the following effects.

- The rows load in *your* transaction. They become durable when you commit, and the table is durable
  already.
- If the load fails, the table is dropped again as a compensating action. If that drop also fails, or
  the process dies in between, an **empty table is left behind** — the error says so when it can.
- Another session can see the empty table between the two commits.

## Time travel: recovering historical data

Both statements allow `AS OF SYSTEM TIME` on the source, which reads the source at a past instant
while writing to the present. This is the supported way to rebuild data as it was.

```sql
-- Rescue a table as it looked before a bad UPDATE
CREATE TABLE orders_before_incident AS
SELECT customer, total FROM orders AS OF SYSTEM TIME '2026-08-07 14:00:00+00:00';

-- Put historical rows back into the live table
INSERT INTO orders (id, customer, total)
SELECT gen_id(), customer, total FROM orders AS OF SYSTEM TIME '-2h' WHERE customer = 'acme';
```

The snapshot can be a negative offset (`'-2h'`, `'-500ms'`) or an absolute timestamp. Unix epoch
milliseconds and parameters are also allowed. The snapshot applies to each table that the source query reads.
This means that an aggregate over it returns the historical value.

A time-travel source is **cheaper and safer** than a live one. History cannot change. This means
that the scan takes no range locks and registers no commit dependencies, even under optimistic
locking. It does not block concurrent writers, and it can't observe the statement's writes. A
standalone historical `SELECT` doesn't work in an explicit transaction. However, these statements
do. Only the source reads at the snapshot, and the writes are made in your transaction.

### What time travel can and cannot recover

It reads **the rows that exist now, as they were then.** Before you rely on it in an incident, you
should know what it can recover.

- **Modified rows recover well.** A bad `UPDATE` is undone by copying the pre-update values.
- **Deleted rows recover too.** A `DELETE` writes its tombstone as a separate revision. The last live
  value's history record survives, and a snapshot taken before the delete reads the row back. Like
  any other historical read, this is bounded by revision retention (see below).
- **A dropped column cannot be recovered.** Historical values are read through the *current* schema.
  A column that no longer exists cannot be projected.
- **A dropped and recreated table is unreachable.** It has a different internal id. Its old rows are
  not part of the new table's history.
- **Retention bounds how far back you can go.** Time travel can only reach revisions that Kahuna has
  not reclaimed. A snapshot older than that reads as *empty* and does not fail. For this reason, a
  recovery that returns no rows is logged with a warning saying the history may already have been
  reclaimed.

During a copy, the engine pins the revision floor at the requested snapshot so reclamation cannot
advance past it mid-copy. That protects the copy from the moment it starts; it cannot bring back
revisions already gone before it started.

## What the response tells you

Both statements return the number of rows written. Both also flag a time-travel copy that read no
rows. The warning is sent to the client as well as to the server log. This means that an empty
recovery cannot be mistaken for a successful one.

CTAS responses come from `POST /execute-sql-ddl`, and `INSERT … SELECT` responses come from
`POST /execute-sql-non-query`.

```jsonc
// CREATE TABLE totals AS SELECT customer, total FROM orders
{ "status": "ok", "rows": 2, "warning": null }

// CREATE TABLE recovered AS SELECT customer FROM orders AS OF SYSTEM TIME '-1h'
{ "status": "ok", "rows": 0,
  "warning": "AS OF SYSTEM TIME copy into 'recovered' inserted no rows. The source may have been
              empty at that snapshot; the history may be older than the configured revision retention
              and already reclaimed; or the rows were deleted after the snapshot …" }
```

`rows` is 0 for a DDL statement that writes no rows, and `warning` is null unless there is
something to say. Over gRPC the same values arrive as `DdlReply.affected_rows` / `warning` and
`NonQueryReply.affected_rows` / `warning` (an absent warning is the empty string, per proto3).

## Privileges

`INSERT … SELECT` requires `Insert` on the target and `Select` on each source, including tables
reached only through a join or subquery. `CREATE TABLE … AS SELECT` requires `CreateTable` on the
database plus `Select` on each source.

## RETURNING

An `INSERT … SELECT` can end with a `RETURNING` list that sends back values from the copied rows;
see [returning.md](returning.md).
