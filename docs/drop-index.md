# `DROP INDEX`

`DROP INDEX` removes a secondary index from a table: its schema entry and all of its index entries.
The table and its rows are not changed. The index is gone when the statement returns, and its entries
are removed in the background after that; see [Cost and space reclamation](#cost-and-space-reclamation).

```sql
DROP INDEX year_idx ON robots;               -- MySQL / SQL Server form, mirrors CREATE INDEX … ON
DROP INDEX robots.year_idx;                  -- table-qualified form, as COMMENT ON INDEX uses
ALTER TABLE robots DROP INDEX year_idx;      -- the ALTER TABLE form

DROP INDEX IF EXISTS year_idx ON robots;     -- IF EXISTS works on all three forms
DROP INDEX IF EXISTS robots.year_idx;
ALTER TABLE robots DROP INDEX IF EXISTS year_idx;
```

The three forms are one statement. The parser builds the same request from each of them. This means
that the rules, the required privilege, and the cluster behavior are identical for all three.

---

## The table is required

Index names are unique **per table**. Two tables in one database can each have an index called
`year_idx`. A bare name cannot be resolved, as there is no database-wide index registry.
Both statements below return an error.

```sql
DROP INDEX year_idx;            -- refused: InvalidInput, names the two accepted forms
DROP INDEX db.robots.year_idx;  -- refused: more than one dot
```

In PostgreSQL the bare form is allowed because its indexes live in a schema-wide namespace. CamusDB does
not, for the same reason [`COMMENT ON INDEX`](comment-on.md) requires `<table>.<index>`.

The `<table>.<index>` form splits the name at its dot. A quoted table name that itself contains a dot
cannot use this form; use `DROP INDEX <index> ON <table>` for it.

## `IF EXISTS`

| Situation | `IF EXISTS` omitted | `IF EXISTS` given |
| --- | --- | --- |
| The index exists | Dropped, reported as applied | Dropped, reported as applied |
| The table exists, the index does not | `InvalidInput` error | No-op, reported as not applied |
| The table does not exist | `TableDoesntExist` error | No-op, reported as not applied |
| The index exists, but a foreign key needs it | `DependentObjectsExist` error | `DependentObjectsExist` error |

`IF EXISTS` hides a missing object. It does not hide any other error.

A no-op makes no schema change. The schema version does not move, and no change is replicated.

The index is checked as part of the drop, under the database's DDL gate. Two concurrent
`DROP INDEX IF EXISTS` statements for one index therefore give one drop and one no-op. Neither
statement returns an error.

The table is also checked again under the gate. If another statement drops the table while the drop
waits for the gate, `IF EXISTS` gives a no-op and a plain drop gives `TableDoesntExist`. If that
statement also creates a table with the same name, the drop is refused with `ConcurrentSchemaChange`,
and the index of the new table stays.

## What cannot be dropped

- **The primary key.** Its internal name starts with `~`, which SQL cannot write. Its
  `<table>_pkey` alias is not an index name either. This means that `DROP INDEX` cannot reach it.
  Use `ALTER TABLE <table> DROP PRIMARY KEY`.
- **An index a foreign key needs.** The referenced unique index of a parent table, and a child index
  that a constraint reuses, are refused while the constraint exists. An index that a constraint
  created for itself has a name that starts with `~`, which SQL cannot write; `DROP CONSTRAINT`
  removes it. See [foreign-keys.md](foreign-keys.md).

## Privileges

`DROP INDEX` needs the `INDEX` privilege on the table, the same as `CREATE INDEX`. The privilege is
checked when the table is opened. For a table that does not exist, there is no privilege to check.
`DROP INDEX IF EXISTS` on it is a no-op for any caller, as `DROP TABLE IF EXISTS` is.

## Cluster behavior

A node that is not the schema leader forwards the statement to the leader, with its `IF EXISTS`
flag. The leader decides whether the table and the index exist. A follower with a stale catalog
therefore cannot decide the outcome.

The leader then takes the index down in three steps. Each step is one schema change, and every live
node applies a step before the leader proposes the next one. The same steps run on a standalone node.

| State of the index | Queries read it | Writes keep its entries |
| --- | --- | --- |
| `Public` (before the drop) | yes | yes |
| `WriteOnly` | no | yes |
| `DeleteOnly` | no | no |
| `Absent` | the index is gone | the index is gone |

Two nodes can be one step apart while a step rolls out. For each pair of adjacent states, no node
reads an index that another node stopped maintaining: a node that still reads the index finds an
entry for every row, because the nodes one step ahead still write them. The leader proposes the step
to `Absent` only after every live node applied `DeleteOnly` and finished the commits that it planned
before that step. After that, no write can add an entry to the index.

While the steps run, DDL on the same table waits for the drop. DDL on other tables of the database
runs between the steps.

## Cost and space reclamation

The statement does not read or delete the index entries. It proposes three schema changes and waits
for their acknowledgements, so its time and its memory do not depend on the size of the index. It
returns when the index is gone from the schema of every live node.

The entries are removed after the statement returns, by a purge that runs on the schema leader. The
purge deletes the key space of the index in batches of `keyspace_purge_batch_size` keys (default
512), so it holds one batch in memory at a time. Each batch starts after the last key of the batch
before it, so the purge does not read the deleted keys again. At the end it reads the key space again
from the start, and it is complete only when the key space is empty.

The purge is durable. In one KV transaction, the leader writes a purge record for the key space of the
index and deletes the job of the drop. The purge deletes its record last.

- **A crash or a leader change during the purge.** The record stays. The next purge run on the schema
  leader finishes it: after a restart, or on the node that becomes the leader.
- **A failure during the steps.** The statement returns the error, and the job of the drop stays. The
  next resume of schema-change jobs on the schema leader takes the index the rest of the way down and
  then purges it. An index that reached `WriteOnly` is not readable again. A leader change during the
  steps is the same: the new leader finishes the drop, and the statement that started it waits for
  that result.
- **`CREATE INDEX` of the same name.** It can run as soon as the statement returns. It does not wait
  for the purge: the new index has a new id and a new key space, and the purge record names the old
  ones only. If an earlier drop of that name stopped before its purge record was written, the
  `CREATE INDEX` writes that record first. If the earlier drop still has steps to take, the
  `CREATE INDEX` is refused with `ConcurrentSchemaChange` until the drop finishes.
- **A foreign key that claims the index first.** A drop that stopped before its first step leaves the
  index `Public`, and a new foreign key can use it. The resume then gives the drop up: the index and the
  constraint both stay. A drop that took its first step is always finished, because no foreign key can
  claim an index that is not `Public`.
- **`DROP TABLE` of the table.** A drop of the table, retained or `FORCE`, keeps the purge of an
  unfinished `DROP INDEX`: in the same transaction it replaces the job of the drop with its purge record.
  A `RELINK` of a retained table brings back its `Public` indexes only.
- **A database branch.** The purge removes only the entries that belong to the branch; see
  [database-branching.md](database-branching.md).

`ALTER TABLE ... DROP PRIMARY KEY` takes the same steps. While the primary key is not `Public`, a
statement that finds its rows through the primary key, such as `UPSERT`, `UPDATE ... FROM` or
`SHOW RANGE FROM TABLE ... FOR ROW`, is refused with `InvalidInput`.

A `DROP INDEX` inside an explicit transaction with transactional DDL keeps the entries until the
transaction commits, and its commit writes the purge record; see
[distributed-schema-architecture.md](distributed-schema-architecture.md#92-schema-changes-inside-an-explicit-transaction).
