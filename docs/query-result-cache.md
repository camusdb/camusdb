# Query Result Cache — Concepts & Developer Guide

> **Audience:** operators enabling and tuning the cache, developers who want to understand what
> `{cache=…}` does and when it is safe, and engineers maintaining or extending the caching layer.
> **Scope:** how a `SELECT` opts into caching, how a result is served and published, and how writes
> and DDL invalidate entries on the same node. The guide also covers how TTL and strict validation
> bound cross-node staleness, the configuration knobs, and the current limitations.

> **Overview.** The query result cache is an **opt-in, per-node, in-memory** cache of fully
> materialized `SELECT` results. A query joins it with an inline hint —
> `SELECT * FROM orders {cache=recent_orders}` — and an identical later query (same shape, same
> bound values, same schema) can be served from memory. Storage is not read for such a query. The
> cache is **correct before it is fast**. A committed write on the same node evicts all dependent
> entries before it becomes visible to a later probe. This means that a same-node reader never sees
> stale data. Writes on *other* nodes are not seen eagerly. Their staleness is bounded by a
> per-entry TTL, or eliminated per hit with the opt-in `strict` mode. The cache is enabled by
> default, but it has no effect until a query opts in with a hint. Set
> `query_result_cache_enabled: false` to turn it off entirely.

---

## 1. Mental model

The cache rests on four ideas.

- **Opt-in per query.** A `SELECT` result is cached only if the statement carries a `{cache=name}`
  hint. The `name` is a *family*. The actual entry key also folds in the database id, the query
  shape, the cache options and the schema versions used. It also contains each bound value with its
  type. Two queries share one entry only if all of those match.

- **Per-node and in-memory (an L1 cache).** Each process owns one cache. It is not shared or
  replicated across a cluster, and it does not survive a restart. A hit on node A does not tell you
  anything about node B.

- **Reads capture a dependency set; writes invalidate against it.** While a cached query executes,
  it records which keyspaces it scanned (for membership) and which rows it read (for content). When
  a transaction commits on this node, its modified keys are mapped back to those dependencies, and
  the overlapping entries are dropped. Missing a dependency would be a correctness bug. Recording a
  slightly too-coarse one costs a false eviction, which is allowed.

- **Same-node writes are exact; cross-node writes are TTL-bounded (or strict).** The publish/commit
  ordering (§6) guarantees that once a write commits on this node, no stale entry for the touched
  keyspace can be served afterward. A write on another node is invisible to this node's cache until
  the entry's TTL expires. The exception is a `strict` entry. Each hit on a strict entry is first
  validated against live storage (§7).

### 1.1 Vocabulary

**Cache family name** — the `name` in `{cache=name}`. A logical namespace for a set of
parameterized entries. `EVICT CACHE 'name'` targets one family. Names are lowercased at parse time.
This means that families are case-insensitive.

**Result fingerprint** — the entry key. A 128-bit hash over a canonical encoding of `(database id,
family name, query shape, typed bound values, schema versions, cache options)`. Two logically
different queries never share a fingerprint (§8).

**Dependency set** — what an entry depends on. A dependency is one of the following three kinds.
- **range deps** — keyspace buckets scanned for membership (a table's row bucket, or an index
  bucket). These catch inserts and phantom rows.
- **point deps** — the full KV keys of individual rows whose bytes were read. These catch content
  updates and deletes that a later range scan could miss.
- **schema deps** — `(tableId, schemaVersion)` pairs the plan and row decoder relied on.

**Keyspace bucket** — the coarse unit of invalidation. A row bucket is `{dbId}:{tableId}|r`; an
index bucket is `{dbId}:{tableId}|i:{indexId}`. A modified KV key is mapped back to its bucket to
find affected entries. Buckets are deliberately coarse to keep the invalidation index small.

**Publish gate** — the small concurrency primitive that makes commit-vs-publish safe: a
monotonically increasing generation counter per keyspace plus a set of in-flight write marks (§6).

**T_cache** — the HLC snapshot timestamp at which a strict entry was computed. Strict validation
compares each dependency's `LastModified` against it.

---

## 2. Using the cache

### The hint

Add `{cache=name}` immediately after a table reference. If the table has an alias, the hint goes
after the alias.

```sql
SELECT id, total FROM orders {cache=recent_orders} WHERE status = 1 ORDER BY total DESC LIMIT 20;
```

Options go in the braces, separated by commas. Their order does not matter.

```sql
SELECT * FROM orders {cache=hot_orders, ttl=30s};        -- per-entry TTL override
SELECT * FROM orders {cache=hot_orders, strict};         -- validate each hit against live storage
SELECT * FROM orders {cache=hot_orders, ttl=5m, strict};
```

- **`ttl=<n>[unit]`** overrides the default TTL for this entry. Units are `ms`, `s`, `m`, `h`; a
  bare integer is milliseconds. The value must be a positive integer within `int` range. Any other
  value is a parse error, for example `ttl=0` or an unknown unit. Overflow and non-integer values
  are also parse errors.
- **`strict`** turns on per-hit validation against live storage (§7). Use it when a reader must not
  see cross-node staleness.

The hint applies to the complete `SELECT` result, whichever table it is attached to. Only one
hint is allowed per statement, and a second one is a parse error. Unknown option names and unknown
hint keys are parse errors.

The `@`-prefixed form `@{cache=name}` (with the same options) is an accepted alias of `{cache=name}`.
This is convenient because `@{…}` is also the index-hint syntax (`@{FORCE_INDEX=idx}`). Both forms
produce an identical cache hint.

### Manual eviction

```sql
EVICT CACHE 'recent_orders';   -- drop every entry in that family, for the current database
EVICT CACHE ALL;               -- drop every result-cache entry for the current database
```

Both are scoped to the current database. `EVICT CACHE ALL` never touches another database's
entries. The family name is a quoted string and is matched case-insensitively. It is lowercased to
line up with how the hint stores names. `EVICT` is a reserved word. `CACHE` and `ALL` are not
reserved. This means that existing tables or columns named `cache`/`all` continue to work.

### What is (and isn't) cache-eligible

A query goes through the cache path only if it meets **all** of the following conditions.

- It carries a `{cache=…}` hint.
- It is an **autocommit** read, which is a read-only transaction that does not have a
  client-supplied transaction id. An explicit `BEGIN … READ ONLY` carries a separate pinned
  snapshot. It is deliberately excluded and always reads live storage.
- It is a **single-table** query. Joins are served by a separate executor and bypass the cache.
- The cache feature is enabled.

All other queries read live storage. A join with a hint, or a hint in a `WHERE`-clause subquery, is
inert. The hint is ignored.

### Observing what happened

Each response to a hinted query carries the cache metadata in the following table. A client does
not have to guess from timing.

| Field | Meaning |
|-------|---------|
| `cacheStatus` | `hit`, `miss`, `bypass`, `stale-revalidated`, or `evicted-before-publish` |
| `cacheBypassReason` | why a bypass or failed publish happened (see below); null otherwise |
| `cacheName` | the family name — present whenever the query was cache-eligible |
| `cachedAtHlc` | the HLC at which a served entry was computed (hits only) |
| `ageMs` | approximate wall-clock age of a served entry in milliseconds (hits only) |

These fields are omitted entirely for a query that does not have a hint. Existing clients are not
affected.

The same five values are returned over gRPC. There, they cannot be part of a response envelope,
because the verdict is known only once the cursor has been drained. They are sent as a trailing
`CacheMetadata` message. On the unary `ExecuteQuery` stream, the message is appended after the last
row. On a batched `BatchExecute` query, it is carried on the `QueryComplete` terminator. An absent
message means the statement carried no hint, exactly as an absent `cacheStatus` does on REST.

**Status meanings.**

- `hit` served stored rows.
- `miss` executed live and stored a fresh entry.
- `bypass` executed live and did not store an entry. The query was ineligible, or a write was in
  flight.
- `evicted-before-publish` executed live and returned correct rows, but the fresh entry could not be
  stored.
- `stale-revalidated` found a strict entry that was stale, and then re-executed live.

**Bypass reasons currently emitted.**

- `in-flight-write`: a write was committing into the scanned keyspace.
- `cache-disabled`: the feature is off, but the query was otherwise eligible.
- `oversized-result`: the result exceeded the per-entry row/byte cap, or the cache is full.
- `dependency-limit`: there were too many dependencies to record completely, or a strict entry's
  per-row dependencies were truncated.

Other reason strings exist in the envelope schema but are not produced by the current code.

---

## 3. The read path

For an eligible query, `QueryExecutor` executes the following steps (see `QueryWithCache`).

1. **Fingerprint.** Build the entry key from the plan's shape id, the bound parameters, the schema
   deps and the cache options.
2. **Probe.** Look the fingerprint up. A live (non-expired) entry is a candidate hit.
   - **Non-strict hit:** check that the entry's schema deps are current, then yield the stored rows.
   - **Strict hit:** validate against live storage (§7). If the entry is valid, yield the stored
     rows. If it is stale, evict it, remember that this was a revalidation, and fall through to live
     execution. The final status becomes `stale-revalidated`.
3. **Strict-without-snapshot guard.** A strict entry is meaningful only if `T_cache` is a real HLC
   snapshot. If the autocommit read does not have a pinned snapshot, serve live rows and do not
   publish.
4. **Snapshot the generation.** Record the current generation counter for the table's row bucket
   *before* executing. If a write is already in flight for that bucket, serve live rows and do not
   publish (`in-flight-write`). This avoids a race with a write that is about to commit.
5. **Execute and collect.** Execute the real plan through a dependency collector (§4), materializing
   rows as they stream to the client.
6. **Publish.** After the stream fully drains, attempt to store the entry through the publish gate.
   The gate re-checks the generation. If a write committed into the bucket during execution, the
   entry is not stored (§6). Row/byte/dependency caps also gate the store here.

A cancelled or faulted enumeration never publishes. The runner marks the drain complete only after
the last row, and publish is skipped otherwise. This is the *no partial publish* invariant.

**Single-flight.** Between steps 3 and 4, concurrent requests for the same fingerprint collapse onto
one execution. The first caller becomes the owner and executes the sequence above, and the rest
wait. The slot carries a *completion signal only*, and rows are not passed through it. When the
owner signals that it published, each waiter repeats step 2 itself. A waiter serves the entry only
if its probe finds and validates it, and it executes live otherwise. The re-probe is needed because
a write can commit and evict the entry between the owner's publish and the moment waiters wake. A
waiter is a query that began *after* that invalidation. Handing it the owner's materialized rows
would serve state older than a committed write. It would also skip the schema and strict checks
that other readers perform. A waiter that times out, or whose owner failed, executes independently.

Step 4 snapshots only the table's **row** bucket, even for an index scan. Each
`INSERT`/`UPDATE`/`DELETE` on a table writes the row key. This means that the row bucket's
generation bumps on *any* same-table write, and the row-bucket fence also covers an index scan.
For the same reason, joins must not use this path. They touch the buckets of more than one table,
and the single-bucket fence would not cover the others.

---

## 4. Dependency capture

The scan and join operators feed a per-request collector (`QueryDependencyCollector`) as they read.
The collector is attached on the cached path only. Each recording call is guarded. An uncached
query pays no cost for it.

The recorded dependencies depend on the scan shape.

- **Full table scan** records the row bucket as a range dep and the table's schema version as a
  schema dep. Each fetched row's key is recorded as a point dep.
- **Index scan / lookup / range / IN-list** records the index bucket as a range dep, along with the
  schema version. Each fetched row's key is recorded as a point dep. Both the index range *and* the
  row points matter. The index range catches phantom inserts and membership changes. The row point
  catches an update to a projected column that isn't part of the index.
- Rows that a residual filter later drops are recorded too. A row that doesn't match today could
  match after an update. This means that it is a real dependency.

**Caps and truncation.** Point dependencies are capped per entry. Beyond the cap they are dropped
and the range dep provides coverage. This is safe for non-strict entries, because same-node
invalidation matches on the range. If an entry exceeds the **total** dependency cap, the collector
is marked and the entry is not published at all. The query bypasses the cache, and an incomplete
set is not stored. Missing coverage is never acceptable. Over-coverage is acceptable.

---

## 5. Invalidation on the same node

Invalidation is driven from the transaction boundary. Batch writes and point writes are covered in
the same way.

**Row and index writes.** `KvTransactionsManager.CommitAsync` collects the transaction's modified
keys and derives their keyspace buckets. After a successful Kahuna commit, it asks the cache to drop
each entry whose dependency set overlaps a modified key or bucket. A modified row key matches both a
row-bucket range dep (via its bucket) and a row point dep (exactly). This all happens through the
publish gate. It is therefore atomic with respect to a concurrent publish (§6). A rolled-back
transaction does not invalidate any entry.

**Schema and catalog changes.** DDL doesn't touch row/index keys the same way. Schema invalidation
is therefore separate. After a successful DDL commit, the executor evicts the entries that have a
schema dependency on the affected table (`InvalidateByTableId`). Dropping a table or a database
evicts by table id or by database. The fingerprint already folds in schema versions. A query issued
after a schema change computes a *different* fingerprint and cannot collide with a pre-change entry
in any case.

The dependency index (`DependencyIndex`) maps buckets, point keys and `(database, table)` schema
keys to the entry ids that depend on them. Invalidation cost therefore scales with the touched
keyspaces and the overlapping entries. The total number of cached entries does not affect it.

---

## 6. The publish gate (commit-safe ordering)

The dangerous race is a query that misses and publishes an entry at the same moment a write commits
into the same keyspace. If the publish landed after the write's invalidation, the stale entry would
survive. `CachePublishGate` closes this window with a generation counter and an in-flight mark count
for each keyspace.

- The **generation counter** is bumped on each committed write and is not decremented.
- The **in-flight mark** count is raised before a commit and cleared after.

**Write path** (in `CommitAsync`): mark the modified keyspaces in-flight *before* the Kahuna commit,
and resolve that mark by the finalize outcome. The deciding condition is whether the write could be
visible. The following table shows the action for each outcome.

| Finalize outcome | What it means | Gate action |
| --- | --- | --- |
| `Committed` | terminal success | bump generations, invalidate *under the gate's lock*, clear marks |
| `MustRetry` past the retry bound, `Errored`, or a cancellation/exception after a commit request went out | **outcome unknown** — the coordinator can apply a commit and lose the response, which means the rows may already be readable here | same as `Committed`: bump, invalidate, clear |
| `Aborted`, a confirmed rollback, or any failure *before* the first commit request | definite non-commit | clear the marks only |

Fencing a write that turns out not to have landed costs one re-execution. Skipping the fence for a
write that did land serves rows older than a committed write, which §1 promises
cannot happen on this node. For this reason, the unknown case is resolved conservatively. If the
caller later retries the same handle and resolves it, the second fence is a harmless no-op.
Generations only move forward, and the entries are already gone.

**Read path**: snapshot the keyspace generations before executing. If a keyspace is already
in-flight, bypass. After execution, publish **only** through `TryPublishUnderGeneration`. Under the
same lock that the commit path uses, it re-checks the generation and inserts the entry only if the
generation hasn't moved. A publish either lands entirely before the write's invalidation (and is
then removed by it) or is discarded because the generation advanced. There is no interleaving in
between.

The lock covers in-memory bookkeeping only. It is not held across a Kahuna commit, which keeps it
cheap. False rejections are acceptable, for example when a query re-executes because an unrelated
row in the same bucket changed. A stale entry that survives a committed write is not acceptable.

---

## 7. TTL and strict validation (bounding cross-node staleness)

The same-node guarantees above do not cover a write on a *different* node. That node's commit does
not reach this node's in-memory index. Two mechanisms bound the resulting staleness.

**TTL.** Each entry has an expiry (the per-hint `ttl`, or the configured default). Expiry is
checked on probe, where an expired entry misses and is removed. A background sweep also drops
expired entries periodically. They do not remain in memory.

**Strict validation.** A `strict` entry is validated against live storage on each hit before its
rows are served (`StrictValidator`). The checks are made in the following order.

1. **Schema deps** — if any depended-on table is gone or now at a different schema version, the
   entry is stale.
2. **Point deps** — probe each row key for the latest committed value. Absent (deleted) or
   `LastModified` newer than `T_cache` (updated) means stale.
3. **Range deps** — scan each bucket; any key with `LastModified` newer than `T_cache` means a
   phantom insert or untracked change.

All reads are non-transactional (latest committed). Validation is bounded by a probe-key limit and
**fails closed**. If validation would exceed the limit, the entry is treated as stale and the query
is re-executed. A failed validation evicts the entry. The next probe then doesn't re-validate a
known-stale one.

Note the following two correctness details.

- **Physical deletes need point deps.** A deleted row's key is gone, and a range scan can't see it.
  Only the row's point dependency catches the delete. For this reason, a strict entry whose point
  deps were truncated by the cap is **not stored at all**. If its point deps are incomplete, it
  could not detect a delete. The query bypasses publish.
- **Strict needs a snapshot.** If the autocommit read does not have a pinned HLC snapshot, `T_cache`
  would be meaningless. The `LastModified` comparisons would then fail. Such strict entries are not
  published. The query serves live rows.

Strict validation can be as expensive as a range scan. It is an opt-in correctness mode for
multi-node reads. Do not use it as a general performance path.

---

## 8. Fingerprinting

The fingerprint must never let two logically different queries share an entry. It is a 128-bit
non-cryptographic hash (XxHash128) over an **injective** canonical string. It guards against
accidental collisions and is no security boundary. A fast hash over an unambiguous encoding is
therefore sufficient.

The canonical form contains the database id, the family name and the query shape id. It also
contains each bound parameter (sorted by name), the schema deps (sorted by table name, with
version) and the cache options. The encoding has two safeguards, typed values and length prefixes.

- **Typed values.** Each value is tagged by type. This means that integer `1` and string `"1"`
  never encode the same way. The same is true of `NULL` and an object id.
- **Length-prefixing.** Each variable-length value is length-prefixed. A value that contains the
  structural delimiters can't be confused with a different set of values. Float formatting is
  culture-invariant so a node's locale can't change the fingerprint.

The query **shape id** is a separate, smaller (64-bit) hash of the query's structure with literals
replaced by placeholders. It is shared with the plan cache and identifies "same query modulo
constants." The result fingerprint always folds the literal values back in. The plan cache
intentionally ignores them, and the result cache must not.

---

## 9. Configuration

The cache is **on by default**. It has no effect until a query opts in with a `{cache=…}` hint.
The knobs are set in the same `config.yml` the rest of the server reads. All knobs except
`query_result_cache_enabled` can also change on a running node with `SET CLUSTER SETTING`
(see `docs/runtime-cluster-settings.md`). `query_result_cache_enabled` needs a restart, because a
disabled node builds no cache at all. Each kind of knob picks up a live change in a different way.

- **`default_ttl_ms`** governs entries published after the change. An entry retains the expiry it
  was stamped with. Lowering the TTL does not shorten entries already resident.
- **`max_entries` / `max_bytes`** trim the resident population down to the new cap at once
  (least recently used first), and govern later publishes.
- **`sweep_interval_ms`** re-arms the background sweep timer at the new interval.
- The per-entry caps, dependency caps, single-flight wait and strict-validation budget are read
  per query. The next query uses the new value.

| Key | Default | Meaning |
|-----|---------|---------|
| `query_result_cache_enabled` | `true` | Master switch. When off, hints are reported as `bypass` / `cache-disabled` and no cache/gate work is done. Turn off to eliminate all cache memory and the small per-write gate bookkeeping on deployments that do not use `{cache=…}`. |
| `query_result_cache_default_ttl_ms` | `5000` | TTL for entries whose hint does not set `ttl`. |
| `query_result_cache_max_entries` | `1024` | Entry count cap (LRU eviction beyond it). |
| `query_result_cache_max_bytes` | `67108864` | Total byte budget across all entries (64 MiB). |
| `query_result_cache_max_entry_bytes` | `1048576` | Per-entry byte cap (1 MiB); larger results are not stored. |
| `query_result_cache_max_entry_rows` | `10000` | Per-entry row cap; larger results are not stored. |
| `query_result_cache_max_deps` | `4096` | Total dependency cap per entry; exceeding it bypasses publish. |
| `query_result_cache_max_point_deps` | `2048` | Point-dep cap per entry; exceeding it drops points (and bypasses strict entries). |
| `query_result_cache_max_ranges` | `256` | Range-dep cap per entry. |
| `query_result_cache_singleflight_wait_ms` | `250` | How long a concurrent miss of the same fingerprint waits for the in-flight owner before executing independently (§3, *Single-flight*). |
| `query_result_cache_strict_validation_max_keys` | `10000` | Probe-key budget for strict validation; exceeding it fails closed. |
| `query_result_cache_sweep_interval_ms` | `10000` | Background TTL-sweep interval. |

Any cap breach fails safe. The query returns correct live rows, and the cache either does not store
the entry or evicts other entries to make room. It never stores an incomplete or oversized entry.

**Operator guidance.** The cache is on by default but inert until a query opts in. Add a
`{cache=…}` hint to read-heavy, frequently-repeated, parameterized queries where the data changes
far less often than it is read. A query that does not have a hint is not cached. In a cluster, note
that each node caches independently. With the default non-strict entries, a reader on one node can
lag a write on another by up to the TTL. Lower `default_ttl_ms` to tighten that bound. You can also
use `{cache=…, strict}` on the specific queries that must not lag, at the cost of a validation scan
per hit. Size `max_bytes` / `max_entries` to the working set you want resident. Oversized results
won't be cached. If a deployment does not use hints and you want to shave the small per-write gate
bookkeeping, set `query_result_cache_enabled: false`.

---

## 10. Limitations and non-goals

- **Single-node L1 only.** No cross-node sharing, no eager cross-node invalidation, no persistence
  across restart. Cross-node freshness is TTL-bounded or strict-validated. It is not eager.
- **Single-table reads only.** Joins bypass the cache. A hint on a join, or in a subquery, is
  inert.
- **Autocommit reads only.** Explicit transactions (including `READ ONLY`) always read live storage.
- **Deterministic reads only in spirit.** The cache keys on values. Wall-clock time and session
  state are not part of the key. Queries whose results depend on non-deterministic inputs should
  not be hinted, since they would cache a single evaluation.
- **Write paths do not read the cache.** `UPDATE`/`DELETE … WHERE` evaluate against live storage.

Possible future directions include cluster-wide eager invalidation and an index-scan fence that
narrows the row-bucket snapshot. Caching for joins would require fencing each involved table. A
shared or persistent second tier is another option. None of these change the correctness contract
above.

---

## 11. Where the code lives

The core types are in `CamusDB.Core/Cache/`.

- `IQueryResultCache` / `QueryResultCache` / `NullQueryResultCache` — the cache contract, the
  in-memory implementation (LRU, byte accounting, TTL sweep, secondary indexes), and the
  disabled-state stand-in used by tests.
- `CachePublishGate` / `CacheGenerationToken` — the commit-safe generation fence and in-flight marks,
  including the committed-vs-unknown-vs-aborted resolution rules (§6).
- `SingleFlightSlot` — the per-fingerprint completion signal concurrent misses wait on (§3).
- `CachedQueryRunner` — wraps execution so publish happens only after a full, successful drain, with
  cap and dependency checks.
- `QueryDependencyCollector` / `QueryDependencySet` / `DependencyIndex` — dependency capture, the
  immutable captured set, and the bucket→entry index used for invalidation.
- `ResultFingerprintBuilder` — the injective canonical encoding and 128-bit hash.
- `StrictValidator` — per-hit validation of strict entries against live storage.
- `CacheHintOptions` / `QueryCacheStatus` (statuses + bypass reasons) / `CacheMetadataHolder` — the
  parsed hint, the result/reason enums, and the response side-channel.

The cache is wired into the following files.

- `SQLParser/` — the `{cache=…}` hint grammar and the `EVICT CACHE` statement.
- `Commands/Executor/Controllers/QueryExecutor.cs` — the read path (`QueryWithCache`) and scan-level
  dependency recording.
- `Commands/Executor/Controllers/Queries/QueryScanner.cs` / `QueryJoinExecutor.cs` — per-scan
  dependency recording (the join sites are recorded but currently unused, since joins bypass).
- `Transactions/KvTransactionsManager.cs` — the commit-time invalidation and gate protocol.
- `Commands/Executor/CommandExecutor.cs` — DDL invalidation hooks and the `EVICT CACHE` handlers.
- `CamusDB/Program.cs` — constructs the singleton cache when enabled (or passes none when disabled).
- `Config/` — the `query_result_cache_*` knobs.
