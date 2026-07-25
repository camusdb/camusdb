# Time-travel reads (`AS OF SYSTEM TIME`)

CamusDB can read a table as it existed at a point in the past. Append an `AS OF SYSTEM TIME`
clause to a `SELECT` and the whole statement — every scanned table, join, and subquery — reads a
single consistent historical snapshot instead of the latest committed data.

```sql
-- relative: the leaderboard as it was 10 seconds ago
SELECT * FROM leaderboard AS OF SYSTEM TIME '-10s';

-- absolute: the leaderboard at a specific instant (UTC)
SELECT * FROM leaderboard AS OF SYSTEM TIME '2026-07-19 20:00:00+00:00';
```

## Syntax

The clause goes **immediately after the `FROM` table, before `WHERE`** (the standard time-travel
placement):

```sql
SELECT * FROM accounts AS OF SYSTEM TIME '-10s' WHERE id = 9910;

SELECT score FROM leaderboard AS OF SYSTEM TIME '-1m'
WHERE score > 5 ORDER BY score DESC LIMIT 10;
```

String values may be single- or double-quoted (`'-10s'` or `"-10s"`).

The value is one of:

| Form | Example | Meaning |
|------|---------|---------|
| Relative offset (string) | `'-10s'`, `'-500ms'`, `'-2m'`, `'-1h'`, `'-1d'` | that far into the past, relative to now |
| Absolute timestamp (string) | `'2026-07-19 20:00:00+00:00'`, `'2026-07-19T20:00:00Z'` | a UTC instant |
| Epoch milliseconds (integer) | `1721420000000` | a UTC instant, Unix ms |
| Parameter | `@ts` | a bound string or integer value, resolved as above |

Relative offsets support the units `ms`, `s`, `m`, `h`, `d` and must be negative (into the past).
A timestamp string with no timezone is interpreted as UTC.

## Semantics

- The read sees the **highest committed revision of each key at or before the resolved timestamp**.
  A write committed *after* that instant is invisible to the historical read but visible to a plain
  `SELECT`.
- The snapshot is fixed for the entire statement, so joins and subqueries all observe the same point
  in time.
- Historical reads are lock-free and never block writers.

## How it works

CamusDB's storage layer (Kahuna) is multi-version: each key retains its prior revisions, each tagged
with the Hybrid Logical Clock (HLC) timestamp at which it committed. Every CamusDB read already
carries a *read timestamp*; a normal query uses "latest", and `AS OF SYSTEM TIME` simply supplies a
past timestamp instead. The value is resolved to an HLC and the query runs on a cheap read-only
snapshot pinned to it — the same mechanism used for serializable read-only snapshots, just aimed at
an earlier point.

## Restrictions

- **Autocommit read-only only.** `AS OF SYSTEM TIME` is rejected inside an explicit multi-statement
  transaction or a promoted (key-range-sharded) read, which are already pinned to their own read
  snapshot and cannot be moved to an arbitrary past point. It is also read-only — there is no
  historical `UPDATE`/`DELETE`.
- **Only past instants.** A value resolving to a future time, or to a time at or before the Unix
  epoch, is rejected.
- **Retention bounds how far back you can look.** The snapshot can only resolve against revisions the
  storage layer still retains. A timestamp older than the retained history simply returns an empty
  result (there is no error for "too old"). By default Kahuna keeps all persisted revisions, so in
  practice the window is wide; tightening revision retention narrows it.

All rejections surface as a `CamusDBException` with code `CADB0409` (`InvalidAsOfSystemTime`,
HTTP 400).
