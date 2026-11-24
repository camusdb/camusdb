# Time-travel reads (`AS OF SYSTEM TIME`)

CamusDB can read a table as it existed at a point in the past. Append an `AS OF SYSTEM TIME`
clause to a `SELECT`, and the statement reads a single consistent historical snapshot in place of
the latest committed data. The snapshot covers each table the statement scans, including the tables
in joins and subqueries.

```sql
-- relative: the leaderboard as it was 10 seconds ago
SELECT * FROM leaderboard AS OF SYSTEM TIME '-10s';

-- absolute: the leaderboard at a specific instant (UTC)
SELECT * FROM leaderboard AS OF SYSTEM TIME '2026-07-19 20:00:00+00:00';
```

## Syntax

The clause goes **immediately after the `FROM` table, before `WHERE`**. This is the standard
time-travel placement.

```sql
SELECT * FROM accounts AS OF SYSTEM TIME '-10s' WHERE id = 9910;

SELECT score FROM leaderboard AS OF SYSTEM TIME '-1m'
WHERE score > 5 ORDER BY score DESC LIMIT 10;
```

String values may be single- or double-quoted (`'-10s'` or `"-10s"`).

The value can take four forms.

| Form | Example | Meaning |
|------|---------|---------|
| Relative offset (string) | `'-10s'`, `'-500ms'`, `'-2m'`, `'-1h'`, `'-1d'` | that far into the past, relative to now |
| Absolute timestamp (string) | `'2026-07-19 20:00:00+00:00'`, `'2026-07-19T20:00:00Z'` | a UTC instant |
| Epoch milliseconds (integer) | `1721420000000` | a UTC instant, Unix ms |
| Parameter | `@ts` | a bound string or integer value, resolved as above |

Relative offsets support the units `ms`, `s`, `m`, `h`, `d` and must be negative (into the past).
A timestamp string that omits the timezone is interpreted as UTC.

## Semantics

- The read sees the **highest committed revision of each key at or before the resolved timestamp**.
  A write committed *after* that instant is invisible to the historical read but visible to a normal
  `SELECT`.
- The snapshot is fixed for the entire statement. Joins and subqueries all observe the same point
  in time.
- Historical reads are lock-free and don't block writers.
- A compressed or out-of-line value is read at the same timestamp as its row. An
  `ALTER TABLE ... REWRITE STORAGE` writes new versions and leaves old ones unchanged. This means
  that a historical read before the rewrite returns the same values as a read after it. For more
  information, see [Large values](storage-layout.md).

## How it works

CamusDB's storage layer (Kahuna) is multi-version: each key retains its prior revisions, each tagged
with the Hybrid Logical Clock (HLC) timestamp at which it committed. A CamusDB read already
carries a *read timestamp*; a normal query uses "latest", and `AS OF SYSTEM TIME` supplies a
past timestamp. The value is resolved to an HLC, and the query reads from a cheap read-only
snapshot pinned to it. This is the same mechanism used for serializable read-only snapshots, aimed
at an earlier point.

## Restrictions

- **Autocommit read-only only.** `AS OF SYSTEM TIME` is rejected in an explicit multi-statement
  transaction. Such a transaction is already pinned to a read snapshot and cannot be moved to an
  arbitrary past point. The clause is also read-only — there is no
  historical `UPDATE`/`DELETE`.
- **Only on the outer query.** The clause is read on the outer query of a `SELECT`, and on the
  outer source query of `INSERT … SELECT` and `CREATE TABLE … AS SELECT`, which copy rows from the
  past. A subquery reads in the snapshot of its statement. For this reason, the clause is refused
  in the following positions.

  - A CTE body or a derived table.
  - A scalar, `IN`, `EXISTS` or `LATERAL` subquery.
  - Any subquery of `INSERT`, `UPDATE` or `DELETE` (`WHERE`, `SET`, `VALUES`, `FROM`, `USING`,
    `ON CONFLICT`, `RETURNING`).
  - An operand of `UNION`, `INTERSECT` or `EXCEPT`. For more information, see
    [Set operations](set-operations.md).
- **Only past instants.** A value resolving to a future time, or to a time at or before the Unix
  epoch, is rejected.
- **A truncated table cannot be read before its current contents began.** `TRUNCATE` replaces the
  key-space a table's rows live in, and a live schema can locate only the current one. A snapshot
  earlier than that cut is refused with `CADB0537`
  (`SnapshotPrecedesContentsGeneration`, HTTP 400). An empty result in that case
  would look the same as a correct empty answer. A snapshot exactly at the cut is allowed
  and observes the new, empty contents. To read the older rows, recover them under a new table with
  `CREATE TABLE … RELINK TO` — see [`TRUNCATE TABLE`](truncate-table.md#time-travel).
- **Retention bounds how far back you can look.** The snapshot can only resolve against revisions
  that are retained by the storage layer. A timestamp older than the retained history returns an
  empty result (there is no error for "too old"). By default, all persisted revisions are kept in
  Kahuna. This means that in practice the window is wide; tightening revision retention narrows it.

All rejections surface as a `CamusDBException` with code `CADB0409` (`InvalidAsOfSystemTime`,
HTTP 400).
