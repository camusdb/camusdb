# `DROP INDEX`

`DROP INDEX` removes a secondary index from a table: its schema entry and all of its index entries.
The table and its rows are not changed.

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
therefore cannot decide the outcome. The drop then replicates to all nodes as one schema change.

## Cost

The drop deletes all entries of the index in one DDL transaction. A DDL transaction is exempt from
the mutation limit (see [transaction-limits.md](transaction-limits.md)). Its work grows with the
number of entries in the index. On a database branch, only the entries that belong to the branch
are deleted; see
[database-branching.md](database-branching.md).
