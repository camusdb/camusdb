# Recovering Dropped Databases and Tables

`DROP DATABASE` and `DROP TABLE` in CamusDB are **deferred, recoverable** operations. Dropping a
database or table detaches it immediately — the name is freed, it disappears from the catalog, and
queries against it fail — but its data is **retained on disk as an orphan** for a configurable
retention window. During that window you can bring it back under a new name with `RELINK`, or let the
background garbage collector reclaim it once the window elapses.

This turns an accidental `DROP` from a data-loss event into a recoverable one, and avoids the cost of
a full backup/restore.

> **Scope.** Recovery applies to **root databases** and their **tables**. It does **not** apply to
> dropped *columns*, and it does **not** apply to *branch* (copy-on-write fork) databases — both take
> the old immediate-delete path. See [Limitations](#limitations).

---

## At a glance

```sql
-- Drop is deferred: data is retained as a recoverable orphan.
DROP DATABASE sales;
DROP TABLE orders;

-- See what can still be recovered (ids + former names).
SHOW ORPHAN DATABASES;
SHOW ORPHAN TABLES;          -- for the current database

-- Recover under a new name, reusing the orphan's id and data.
CREATE DATABASE sales_restored RELINK TO '7';
CREATE TABLE orders_restored  RELINK TO 'A0';

-- Skip deferral: delete immediately and permanently (the pre-deferred behavior).
DROP DATABASE sales FORCE;
DROP TABLE orders FORCE;
```

---

## Deferred drop

A non-`FORCE` drop of a root database or one of its tables has three effects.

- **It removes the object from the catalog.** The name is immediately free to reuse, and
  `SHOW DATABASES` / `SHOW TABLES` no longer list it. Reads and writes against it fail with the usual
  "does not exist" error.
- **It leaves all of the object's data on disk.** The rows and index entries are unchanged. For a
  database, the schema of each table is unchanged too.
- **It records an orphan.** This is a small metadata entry that makes the object discoverable via
  `SHOW ORPHAN …` and recoverable via `RELINK`.

CamusDB never reuses ids. This means that recovery is unambiguous: the orphan has its original
id, and a new object created with the *same name* gets a *new*, empty keyspace with a new id. The new
object cannot collide with the orphan.

```sql
DROP DATABASE sales;                 -- 'sales' is now free; its data is an orphan
CREATE DATABASE sales;               -- brand-new empty database, new id
-- the old 'sales' data is still recoverable under its orphan id via RELINK
```

## Immediate drop — `FORCE`

Append `FORCE` to bypass deferral and physically delete the data right away. This is the behavior
CamusDB had before deferred drop existed. Use it when you are certain the data is not needed, or to
reclaim space immediately before the retention window ends.

```sql
DROP DATABASE staging FORCE;         -- keyspace purged now; not recoverable
DROP TABLE scratch FORCE;            -- rows/indexes deleted now; not recoverable
DROP DATABASE IF EXISTS staging FORCE;
```

A `FORCE` drop writes **no** orphan record. The object does not appear in `SHOW ORPHAN …`.

---

## Inspecting orphans

`SHOW ORPHAN DATABASES` lists dropped-but-recoverable databases (server-level; needs no current
database). `SHOW ORPHAN TABLES` lists dropped-but-recoverable tables in the **current** database.

```
SHOW ORPHAN DATABASES;

 id | former_name | dropped_at               | expires_at
----+-------------+--------------------------+--------------------------
 7  | sales       | 2026-07-16T09:52:03.456Z | 2026-07-23T09:52:03.456Z
```

| Column | Meaning |
|--------|---------|
| `id` | The orphan's stable id — pass this to `RELINK TO`. |
| `former_name` | The name the object had when it was dropped (for identification only). |
| `dropped_at` | UTC ISO-8601 timestamp of the drop (`yyyy-MM-ddTHH:mm:ss.fffZ`). |
| `expires_at` | When the garbage collector becomes eligible to reclaim it, or `never` when automatic reclamation is disabled (see [Retention](#retention-and-the-garbage-collector)). Advisory — recovery works right up until the data is actually purged. |

`SHOW ORPHAN TABLES` carries one extra column, `kind`. It tells apart the two different things that
can be retained.

| `kind` | What it is | Is `former_name` a live table? |
|--------|------------|-------------------------------|
| `dropped table` | A table that was dropped. | No — the name was freed. |
| `retired contents` | One set of contents a **still-live** table stopped reading after a [`TRUNCATE`](truncate-table.md). | **Yes** — that table exists and is empty. |

Recovering `retired contents` publishes the retained rows as a **separate new table** with a fresh
relation id; the table named in `former_name` is not affected. For more information, see
[`TRUNCATE TABLE` → Recovering the previous contents](truncate-table.md#recovering-the-previous-contents).

---

## Recovering with `RELINK`

`RELINK` re-attaches a **new name** to an existing orphan's id and retained data.

```sql
CREATE DATABASE sales_restored RELINK TO '7';
CREATE TABLE   orders_restored RELINK TO 'A0';
```

- The **id is quoted**. Orphan ids are opaque tokens (they may be purely numeric or mixed-case).
  They are given as a string literal, exactly as shown by `SHOW ORPHAN …`.
- `CREATE TABLE … RELINK TO` works in the context of the current database and recovers one of *its*
  table orphans.
- The recovered object opens fully populated. Its rows and indexes are present, and so are its CHECK
  and NOT NULL constraints. The drop did not delete them.
- **A recovered table comes back with its foreign keys removed.** While the table was dropped, its
  rows were not checked against their parents, and a parent can be gone. The SQL `RELINK` returns a
  warning that names each foreign key it did not restore.

  ```text
  Table 'x' was restored without its foreign keys (…). Add each one again with
  ALTER TABLE ... ADD CONSTRAINT, which validates the restored rows.
  ```

  An index that a foreign key created and owned remains as an ordinary index. For more information,
  see [foreign-keys.md](foreign-keys.md).

Recovery succeeds as long as the orphan exists (i.e. the garbage collector has not reclaimed it
yet), even slightly past `expires_at`.

### Errors

| Situation | Error |
|-----------|-------|
| No orphan exists for the given id (not dropped, or already reclaimed) | `OrphanNotFound` (`CADB0510`, HTTP 404) |
| The new name is already taken | `DatabaseAlreadyExists` / `TableAlreadyExists` |
| A relink and a GC reclamation of the same id race | one wins; the other gets `OrphanNotFound` or a "concurrent operation in progress" error — retry |

---

## Retention and the garbage collector

Orphans are not kept forever. A background **garbage collector** physically reclaims any orphan whose
age exceeds the retention window. It works on a single elected node, and a cluster reclaims each
orphan exactly once. Reclamation deletes the orphan's data and metadata together with the orphan
record. After that, the id is `OrphanNotFound` and the data is gone.

Two settings in `CamusDB/Config/config.yml` control this.

| Config key | Default | Meaning |
|------------|---------|---------|
| `orphan_retention_ms` | `604800000` (7 days) | How long an orphan is recoverable before the GC may delete it. **With `0` or a negative value, orphans are kept indefinitely.** They are reclaimed only by an explicit `… FORCE` drop. |
| `orphan_reclaim_interval_ms` | `300000` (5 min) | How often the GC sweeps. `0` or negative disables the sweep entirely. |

```yaml
# Keep dropped data recoverable for 30 days
orphan_retention_ms: 2592000000

# Never auto-delete; require an explicit FORCE / manual reclamation
orphan_retention_ms: 0
```

The GC also does one sweep at startup. This means that orphans that expired while the server was down
are reclaimed before the next interval. Reclamation is crash-safe: an interrupted purge is
idempotent and is finished by a later sweep. Retention is independent of Kahuna's PITR/WAL window.
That window governs log compaction and does not apply to these physical row keys.

---

## Cluster behavior

- Deferred drop and `RELINK` are cluster-aware, and so is `SHOW ORPHAN …`. Table DDL (including
  `RELINK`) is forwarded to the schema leader as usual.
- The GC sweep is done by whichever node currently leads the database-registry partition; leadership
  changes hand the sweep to the new leader automatically.
- A `RELINK` and a GC reclamation of the same object take the same per-object fence. They can't
  interleave, and recovered data is never half-purged.

---

## Limitations

- **Dropped columns are not recoverable.** `ALTER TABLE … DROP COLUMN` physically rewrites all rows
  to remove the column's bytes; there is no column orphan and `SHOW ORPHAN …` does not cover columns.
- **Branch (COW-fork) databases drop immediately.** A dropped branch database is purged right away.
  It is not deferred and is not recoverable via `RELINK`. (Deferring a branch would require pinning
  its parent's snapshot history for the full retention window.) Root databases and their tables are
  fully covered.

A relinked table that was `ALTER`ed before the drop recovers rows written under **all** schema
versions. It is reattached at its real drop-time version with a lazy schema-history loader over the
retained history keys. This means that both current-layout and earlier pre-`ALTER` rows decode.

---

## Related

- `docs/databases.md` — database lifecycle and naming.
- `docs/database-branching.md` — branch (COW-fork) databases, which take the immediate-drop path.
- `CamusDB/Config/config.yml` — inline documentation for `orphan_retention_ms` /
  `orphan_reclaim_interval_ms`.
