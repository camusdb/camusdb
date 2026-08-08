# `INSERT INTO … SELECT` and `CREATE TABLE … AS SELECT`

Two statements that let a query, rather than a client, supply rows.

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

**Columns are matched by position, never by name.** The source's *k*-th output column feeds the
*k*-th target column, whatever either is called:

```sql
-- b's values land in a, and a's in b
INSERT INTO pairs (a, b) SELECT b, a FROM pairs;
```

The two lists must have the same number of columns; a mismatch is rejected before the query runs.
With no explicit column list, the target is every column of the table in schema order — the same rule
`INSERT … VALUES` uses.

Values are coerced to the target column's declared type, and target columns the statement does not
mention take their column default (a function default such as `gen_uuid_v7()` is evaluated **per
row**, so each row gets its own value). NOT NULL, CHECK, unique indexes and TTL are enforced exactly
as for `INSERT … VALUES`, because the same insert path writes the rows.

The statement is all-or-nothing: a violation on any row aborts the whole statement. The result is the
number of rows inserted; an empty source is a success with `0`.

`ORDER BY` in the source is accepted but carries no meaning for storage — rows are stored by a
generated row id, not in insertion order.

### Copying a table into itself

`INSERT INTO t SELECT … FROM t` is supported and terminates: the engine reads the entire source
before writing the first row, so the scan cannot observe the rows the statement is inserting.

### Limits

A transaction may not insert more than `max_mutations_per_transaction` rows (default 20 000), and
this statement is bounded by that. A larger copy fails with `CADB0506`; split it with `WHERE` or
`LIMIT` and run it in several transactions.

While the copy runs, the rows it reads are locked against concurrent writers for the rest of the
transaction — the source scan decides what is written, so it holds exclusive range locks over the
range it scanned. A large copy therefore blocks writes to that range until it commits. (A time-travel
source is exempt; see below.)

## `CREATE TABLE … AS SELECT`

The new table's columns are the source query's output columns, with the types that query would report
to a client. Nothing else is inherited: **indexes, CHECK constraints, NOT NULL, defaults, comments and
table settings are not copied** from the source table — only the shape of the result.

Add `WITH NO DATA` to create the table without loading rows; `WITH DATA` is the default. With
`IF NOT EXISTS`, an existing table makes the statement a no-op and the source query is never executed.

### The primary key

A query result has no key of its own, and every CamusDB table needs one, so a key is **synthesized**:
a leading `id oid NOT NULL DEFAULT gen_id()` column that the source does not fill. If the query
already outputs a column called `id`, the synthesized one becomes `id2` (then `id3`, …).

A projected column is never reused as the key: a projection is under no obligation to be unique — a
join or a non-distinct projection repeats values — and the copy would fail partway with a duplicate
key.

### Projections that are rejected

Every output column has to become a named, typed column, so three shapes are refused with an
explanation:

| Rejected | Why | Fix |
| --- | --- | --- |
| `SELECT year + 1 FROM t` | an unaliased expression has no name (it would be called `0`) | `SELECT year + 1 AS next_year` |
| `SELECT NULL AS x` | no type to declare | `SELECT CAST(NULL AS INT64) AS x` |
| `SELECT * FROM a JOIN b …` | `*` over a join yields qualified names like `a.id` | list the columns with aliases |

### Schema and data are not committed together

Creating the table commits on its own — in cluster mode it is replicated through Raft — **before** any
row is written, so the two are not one atomic unit:

- The rows load in *your* transaction, so they become durable when you commit; the table already is.
- If the load fails, the table is dropped again as a compensating action. If that drop also fails, or
  the process dies in between, an **empty table is left behind** — the error says so when it can.
- Another session can see the empty table between the two commits.

## Time travel: recovering historical data

Both statements accept `AS OF SYSTEM TIME` on the source, which reads the source at a past instant
while writing to the present. This is the supported way to rebuild data as it was:

```sql
-- Rescue a table as it looked before a bad UPDATE
CREATE TABLE orders_before_incident AS
SELECT customer, total FROM orders AS OF SYSTEM TIME '2026-08-07 14:00:00+00:00';

-- Put historical rows back into the live table
INSERT INTO orders (id, customer, total)
SELECT gen_id(), customer, total FROM orders AS OF SYSTEM TIME '-2h' WHERE customer = 'acme';
```

The snapshot accepts a negative offset (`'-2h'`, `'-500ms'`), an absolute timestamp, Unix epoch
milliseconds, or a parameter. It applies to the whole source query, so an aggregate over it reports
the historical value.

A time-travel source is **cheaper and safer** than a live one: history cannot change, so the scan
takes no range locks and does not block concurrent writers, and it can never observe the statement's
own writes. Unlike a plain historical `SELECT`, it also works inside an explicit transaction, because
only the source reads at the snapshot — the writes stay in your transaction.

### What time travel can and cannot recover

It reads **the rows that exist now, as they were then.** Consequences worth knowing before you rely on
it in an incident:

- **Modified rows recover well.** A bad `UPDATE` is undone by copying the pre-update values.
- **Deleted rows recover too.** A `DELETE` writes its tombstone as a revision of its own, so the
  last live value's history record survives and a snapshot taken before the delete reads the row
  back — bounded by revision retention like every other historical read (see below).
- **A dropped column cannot be recovered.** Historical values are read through the *current* schema,
  so a column that no longer exists cannot be projected.
- **A dropped and recreated table is unreachable.** It has a different internal id, so its old rows
  are not part of the new table's history.
- **Retention bounds how far back you can go.** Time travel only reaches revisions Kahuna still
  retains. A snapshot older than that reads as *empty* rather than failing — so a recovery that
  returns no rows is logged with a warning saying the history may already have been reclaimed.

While a copy runs, the engine pins the revision floor at the requested snapshot so reclamation cannot
advance past it mid-copy. That protects the copy from the moment it starts; it cannot bring back
revisions already gone before it started.

## What the response tells you

Both statements report the number of rows written, and both flag a time-travel copy that read
nothing — the warning reaches the client, not just the server log, so an empty recovery cannot be
mistaken for a successful one.

`POST /execute-sql-ddl` (CTAS) and `POST /execute-sql-non-query` (`INSERT … SELECT`):

```jsonc
// CREATE TABLE totals AS SELECT customer, total FROM orders
{ "status": "ok", "rows": 2, "warning": null }

// CREATE TABLE recovered AS SELECT customer FROM orders AS OF SYSTEM TIME '-1h'
{ "status": "ok", "rows": 0,
  "warning": "AS OF SYSTEM TIME copy into 'recovered' inserted no rows. The source may have been
              empty at that snapshot; the history may be older than the configured revision retention
              and already reclaimed; or the rows were deleted after the snapshot …" }
```

`rows` is 0 for every DDL statement that writes no rows, and `warning` is null unless there is
something to say. Over gRPC the same values arrive as `DdlReply.affected_rows` / `warning` and
`NonQueryReply.affected_rows` / `warning` (an absent warning is the empty string, per proto3).

## Privileges

`INSERT … SELECT` requires `Insert` on the target and `Select` on every source, including tables
reached only through a join or subquery. `CREATE TABLE … AS SELECT` requires `CreateTable` on the
database plus `Select` on every source.
