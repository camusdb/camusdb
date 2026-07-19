# Recovering Dropped Databases and Tables

`DROP DATABASE` and `DROP TABLE` in CamusDB are **deferred, recoverable** operations. Dropping a
database or table detaches it immediately — the name is freed, it disappears from the catalog, and
queries against it fail — but its data is **retained on disk as an orphan** for a configurable
retention window. During that window you can bring it back under a new name with `RELINK`, or let the
background garbage collector reclaim it once the window elapses.

This turns an accidental `DROP` from a data-loss event into a recoverable one, without the cost of a
full backup/restore.

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

A plain (non-`FORCE`) drop of a root database or one of its tables:

- **removes it from the catalog** — the name is immediately free to reuse, `SHOW DATABASES` /
  `SHOW TABLES` no longer list it, and reads/writes against it fail with the usual
  "does not exist" error;
- **keeps all of its data on disk** — every row, index entry, and (for a database) every table's
  schema stays exactly as it was;
- **records an orphan** — a small metadata entry that makes the object discoverable via
  `SHOW ORPHAN …` and recoverable via `RELINK`.

Because CamusDB never reuses ids, recovery is unambiguous: the orphan keeps its original id, and a new
object created with the *same name* simply gets a *new* id and its own empty keyspace — it never
collides with the orphan.

```sql
DROP DATABASE sales;                 -- 'sales' is now free; its data is an orphan
CREATE DATABASE sales;               -- brand-new empty database, new id
-- the old 'sales' data is still recoverable under its orphan id via RELINK
```

## Immediate drop — `FORCE`

Append `FORCE` to bypass deferral and physically delete the data right away. This is the behavior
CamusDB had before deferred drop existed. Use it when you are certain the data is not needed (and to
reclaim space immediately without waiting for the retention window).

```sql
DROP DATABASE staging FORCE;         -- keyspace purged now; not recoverable
DROP TABLE scratch FORCE;            -- rows/indexes deleted now; not recoverable
DROP DATABASE IF EXISTS staging FORCE;
```

A `FORCE` drop writes **no** orphan record, so the object never appears in `SHOW ORPHAN …`.

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

---

## Recovering with `RELINK`

`RELINK` re-attaches a **new name** to an existing orphan's id and retained data:

```sql
CREATE DATABASE sales_restored RELINK TO '7';
CREATE TABLE   orders_restored RELINK TO 'A0';
```

- The **id is quoted** — orphan ids are opaque tokens (they may be purely numeric or mixed-case), so
  they are given as a string literal, exactly as shown by `SHOW ORPHAN …`.
- `CREATE TABLE … RELINK TO` runs in the context of the current database and recovers one of *its*
  table orphans.
- The recovered object opens fully populated: all rows, indexes, and constraints are present, because
  they were never deleted.

Recovery succeeds as long as the orphan still exists (i.e. the garbage collector has not reclaimed it
yet) — even slightly past `expires_at`.

### Errors

| Situation | Error |
|-----------|-------|
| No orphan exists for the given id (never dropped, or already reclaimed) | `OrphanNotFound` (`CADB0510`, HTTP 404) |
| The new name is already taken | `DatabaseAlreadyExists` / `TableAlreadyExists` |
| A relink and a GC reclamation of the same id race | one wins; the other gets `OrphanNotFound` or a "concurrent operation in progress" error — retry |

---

## Retention and the garbage collector

Orphans are not kept forever. A background **garbage collector** physically reclaims any orphan whose
age exceeds the retention window, on a single elected node (so a cluster reclaims each orphan exactly
once). Reclamation deletes the orphan's row/index data, its metadata, and the orphan record together;
after that the id is `OrphanNotFound` and the data is gone.

Two settings control this (see `CamusDB/Config/config.yml`):

| Config key | Default | Meaning |
|------------|---------|---------|
| `orphan_retention_ms` | `604800000` (7 days) | How long an orphan stays recoverable before the GC may delete it. **`0` or negative keeps orphans indefinitely** — they are reclaimed only by an explicit `… FORCE` drop. |
| `orphan_reclaim_interval_ms` | `300000` (5 min) | How often the GC sweeps. `0` or negative disables the sweep entirely. |

```yaml
# Keep dropped data recoverable for 30 days
orphan_retention_ms: 2592000000

# Never auto-delete; require an explicit FORCE / manual reclamation
orphan_retention_ms: 0
```

The GC also runs one sweep at startup, so orphans that expired while the server was down are reclaimed
promptly rather than only on the next interval. Reclamation is crash-safe: an interrupted purge is
idempotent and is finished by a later sweep. Retention is independent of Kahuna's PITR/WAL window,
which governs log compaction, not these physical row keys.

---

## Cluster behavior

- Deferred drop, `RELINK`, and `SHOW ORPHAN …` are all cluster-aware; table DDL (including
  `RELINK`) is forwarded to the schema leader as usual.
- The GC sweep runs on whichever node currently leads the database-registry partition; leadership
  changes hand the sweep to the new leader automatically.
- A `RELINK` and a GC reclamation of the same object never interleave — they take the same per-object
  fence — so recovered data is never half-purged.

---

## Limitations

- **Dropped columns are not recoverable.** `ALTER TABLE … DROP COLUMN` physically rewrites every row
  to remove the column's bytes; there is no column orphan and `SHOW ORPHAN …` does not cover columns.
- **Branch (COW-fork) databases drop immediately.** A dropped branch database is purged right away,
  not deferred, and is not recoverable via `RELINK`. (Deferring a branch would require pinning its
  parent's snapshot history for the whole retention window.) Root databases and their tables are fully
  covered.
A relinked table that was `ALTER`ed before the drop recovers rows written under **every** schema version:
it is reattached at its real drop-time version with a lazy schema-history loader over the retained
history keys, so both current-layout and earlier pre-`ALTER` rows decode.

---

## Related

- `docs/databases.md` — database lifecycle and naming.
- `docs/database-branching.md` — branch (COW-fork) databases, which take the immediate-drop path.
- `CamusDB/Config/config.yml` — inline documentation for `orphan_retention_ms` /
  `orphan_reclaim_interval_ms`.
