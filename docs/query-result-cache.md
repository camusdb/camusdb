# Query Result Cache — Concepts & Developer Guide

> **Audience:** operators enabling and tuning the cache, developers who want to understand what
> `{cache=…}` does and when it is safe, and engineers maintaining or extending the caching layer.
> **Scope:** how a `SELECT` opts into caching, how a result is served and published, how writes and
> DDL invalidate entries on the same node, how cross-node staleness is bounded by TTL and strict
> validation, the configuration knobs, and the current limitations.

> **Overview.** The query result cache is an **opt-in, per-node, in-memory** cache of fully
> materialized `SELECT` results. A query joins it with an inline hint —
> `SELECT * FROM orders {cache=recent_orders}` — and an identical later query (same shape, same
> bound values, same schema) can be served from memory without touching storage. The cache is
> **correct before it is fast**: a committed write on the same node evicts every dependent entry
> before it becomes visible to a later probe, so a same-node reader never sees stale data. Writes on
> *other* nodes are not seen eagerly; their staleness is bounded by a per-entry TTL, or eliminated
> per-hit with the opt-in `strict` mode. It is enabled by default, but does nothing until a
> query opts in with a hint; set `query_result_cache_enabled: false` to turn it off entirely.

---

## 1. Mental model

Four ideas are the whole story; the rest is detail.

- **Opt-in per query.** Nothing is cached unless a `SELECT` carries a `{cache=name}` hint. The
  `name` is a *family*; the actual entry key also folds in the database id, the query shape, every
  bound value with its type, the schema versions used, and the cache options. Two queries collide
  on one entry only when all of those match.

- **Per-node and in-memory (an L1 cache).** Each process owns one cache. It is not shared or
  replicated across a cluster, and it does not survive a restart. A hit on node A tells you nothing
  about node B.

- **Reads capture a dependency set; writes invalidate against it.** While a cached query executes,
  it records which keyspaces it scanned (for membership) and which rows it read (for content). When
  a transaction commits on this node, its modified keys are mapped back to those dependencies and
  every overlapping entry is dropped. Missing a dependency would be a correctness bug; recording a
  slightly too-coarse one only costs a false eviction, which is allowed.

- **Same-node writes are exact; cross-node writes are TTL-bounded (or strict).** The publish/commit
  ordering (§6) guarantees that once a write commits on this node, no stale entry for the touched
  keyspace can be served afterward. A write on another node is invisible to this node's cache until
  the entry's TTL expires — unless the entry is `strict`, in which case each hit is validated
  against live storage first (§7).

### 1.1 Vocabulary

**Cache family name** — the `name` in `{cache=name}`. A logical namespace for a set of
parameterized entries. `EVICT CACHE 'name'` targets one family. Names are lowercased at parse time,
so families are case-insensitive.

**Result fingerprint** — the entry key. A 128-bit hash over a canonical encoding of `(database id,
family name, query shape, typed bound values, schema versions, cache options)`. Two logically
different queries never share a fingerprint (§8).

**Dependency set** — what an entry depends on, in three kinds:
- **range deps** — keyspace buckets scanned for membership (a table's row bucket, or an index
  bucket). These catch inserts and phantom rows.
- **point deps** — the full KV keys of individual rows whose bytes were read. These catch content
  updates and deletes that a later range scan could miss.
- **schema deps** — `(tableId, schemaVersion)` pairs the plan and row decoder relied on.

**Keyspace bucket** — the coarse unit of invalidation. A row bucket is `{dbId}:{tableId}:r`; an
index bucket is `{dbId}:{tableId}:i:{indexId}`. A modified KV key is mapped back to its bucket to
find affected entries. Buckets are deliberately coarse so the invalidation index stays small.

**Publish gate** — the small concurrency primitive that makes commit-vs-publish safe: a
monotonically increasing generation counter per keyspace plus a set of in-flight write marks (§6).

**T_cache** — the HLC snapshot timestamp at which a strict entry was computed. Strict validation
compares each dependency's `LastModified` against it.

---

## 2. Using the cache

### The hint

Add `{cache=name}` immediately after a table reference (after the optional alias):

```sql
SELECT id, total FROM orders {cache=recent_orders} WHERE status = 1 ORDER BY total DESC LIMIT 20;
```

Options go inside the braces, comma-separated, in any order:

```sql
SELECT * FROM orders {cache=hot_orders, ttl=30s};        -- per-entry TTL override
SELECT * FROM orders {cache=hot_orders, strict};         -- validate each hit against live storage
SELECT * FROM orders {cache=hot_orders, ttl=5m, strict};
```

- **`ttl=<n>[unit]`** overrides the default TTL for this entry. Units are `ms`, `s`, `m`, `h`; a
  bare integer is milliseconds. The value must be a positive integer within `int` range; `ttl=0`,
  overflow, an unknown unit, or a non-integer value are parse errors.
- **`strict`** turns on per-hit validation against live storage (§7). Useful when a reader must not
  see cross-node staleness.

The hint applies to the **whole** `SELECT` result, not just the table it is attached to. Only one
hint is allowed per statement; a second one is a parse error. Unknown option names and unknown hint
keys are parse errors.

The `@`-prefixed form `@{cache=name}` (with the same options) is an accepted alias of `{cache=name}`
— handy since `@{…}` is also the index-hint syntax (`@{FORCE_INDEX=idx}`). Both forms produce an
identical cache hint.

### Manual eviction

```sql
EVICT CACHE 'recent_orders';   -- drop every entry in that family, for the current database
EVICT CACHE ALL;               -- drop every result-cache entry for the current database
```

Both are scoped to the current database — `EVICT CACHE ALL` never touches another database's
entries. The family name is a quoted string and is matched case-insensitively (it is lowercased to
line up with how the hint stores names). `EVICT` is a reserved word; `CACHE` and `ALL` are not, so
existing tables or columns named `cache`/`all` keep working.

### What is (and isn't) cache-eligible

A query goes through the cache path only when **all** of these hold:

- it carries a `{cache=…}` hint,
- it is an **autocommit** read — a read-only transaction with no client-supplied transaction id
  (an explicit `BEGIN … READ ONLY` carries its own pinned snapshot and is deliberately excluded, so
  it always reads live storage),
- it is a **single-table** query (joins are served by a separate executor and bypass the cache),
- the cache feature is enabled.

Anything else reads live storage. A join with a hint, or a hint inside a `WHERE`-clause subquery, is
inert — the hint is ignored, not honored partially.

### Observing what happened

Every response to a hinted query carries cache metadata so a client never has to guess from timing:

| Field | Meaning |
|-------|---------|
| `cacheStatus` | `hit`, `miss`, `bypass`, `stale-revalidated`, or `evicted-before-publish` |
| `cacheBypassReason` | why a bypass or failed publish happened (see below); null otherwise |
| `cacheName` | the family name — present whenever the query was cache-eligible |
| `cachedAtHlc` | the HLC at which a served entry was computed (hits only) |
| `ageMs` | approximate wall-clock age of a served entry in milliseconds (hits only) |

These fields are omitted entirely for a query with no hint, so existing clients are unaffected.

**Status meanings.** `hit` served stored rows. `miss` executed live and stored a fresh entry.
`bypass` executed live and stored nothing (the query was ineligible, or a write was in flight).
`evicted-before-publish` executed live and returned correct rows, but the fresh entry could not be
stored. `stale-revalidated` found a strict entry, discovered it was stale, and re-executed live.

**Bypass reasons currently emitted:** `in-flight-write` (a write was committing into the scanned
keyspace), `cache-disabled` (the feature is off but the query was otherwise eligible),
`oversized-result` (the result exceeded the per-entry row/byte cap or the cache is full),
`dependency-limit` (too many dependencies to record completely, or a strict entry whose per-row
dependencies were truncated). Other reason strings exist in the envelope schema but are not produced
by the current code.

---

## 3. The read path

For an eligible query, `QueryExecutor` runs this sequence (see `QueryWithCache`):

1. **Fingerprint.** Build the entry key from the plan's shape id, bound parameters, schema deps, and
   cache options.
2. **Probe.** Look the fingerprint up. A live (non-expired) entry is a candidate hit.
   - **Non-strict hit:** yield the stored rows; done.
   - **Strict hit:** validate against live storage (§7). If valid, yield stored rows. If stale,
     evict it, remember that this was a revalidation, and fall through to live execution — the final
     status becomes `stale-revalidated`.
3. **Strict-without-snapshot guard.** A strict entry is only meaningful if `T_cache` is a real HLC
   snapshot. If the autocommit read has no pinned snapshot, serve live rows without publishing.
4. **Snapshot the generation.** Record the current generation counter for the table's row bucket
   *before* executing. If a write is already in flight for that bucket, serve live rows without
   publishing (`in-flight-write`) to avoid racing an about-to-commit write.
5. **Execute and collect.** Run the real plan through a dependency collector (§4), materializing
   rows as they stream to the client.
6. **Publish.** After the stream fully drains, attempt to store the entry — but only through the
   publish gate, which re-checks the generation and rejects the store if a write committed into the
   bucket during execution (§6). Row/byte/dependency caps also gate the store here.

A cancelled or faulted enumeration never publishes: the runner only marks the drain complete after
the last row, and publish is skipped otherwise. This is the *no partial publish* invariant.

Why snapshot only the table's **row** bucket, even for an index scan? Because every
`INSERT`/`UPDATE`/`DELETE` on a table writes the row key, so the row bucket's generation bumps on
*any* same-table write — an index scan is covered by the row-bucket fence too. (This is exactly why
joins must not use this path: they touch more than one table's buckets and the single-bucket fence
would not cover the others.)

---

## 4. Dependency capture

The scan and join operators feed a per-request collector (`QueryDependencyCollector`) as they read.
The collector is attached only on the cached path; every recording call is guarded, so an uncached
query pays nothing.

Rules by scan shape:

- **Full table scan** records the row bucket as a range dep, the table's schema version as a schema
  dep, and each fetched row's key as a point dep.
- **Index scan / lookup / range / IN-list** records the index bucket as a range dep, the schema
  version, and each fetched row's key as a point dep. Both the index range *and* the row points
  matter: the index range catches phantom inserts and membership changes; the row point catches an
  update to a projected column that isn't part of the index.
- Rows that a residual filter later rejects are still recorded — a row that doesn't match today
  could match after an update, so it is a real dependency.

**Caps and truncation.** Point dependencies are capped per entry. Beyond the cap they are dropped
and the range dep provides coverage — safe for non-strict entries because same-node invalidation
matches on the range. A **total** dependency cap, if exceeded, marks the collector and the entry is
not published at all (bypassing rather than storing an incomplete set). Missing coverage is never
acceptable; over-coverage is.

---

## 5. Invalidation on the same node

Invalidation is driven from the transaction boundary, not scattered across write methods, so batch
and point writes are covered uniformly.

**Row and index writes.** `KvTransactionsManager.CommitAsync` collects the transaction's modified
keys, derives their keyspace buckets, and — after a successful Kahuna commit — asks the cache to
drop every entry whose dependency set overlaps a modified key or bucket. A modified row key matches
both a row-bucket range dep (via its bucket) and a row point dep (exactly). This all happens through
the publish gate so it is atomic with respect to a concurrent publish (§6). A rolled-back
transaction invalidates nothing.

**Schema and catalog changes.** DDL doesn't touch row/index keys the same way, so schema
invalidation is separate: after a successful DDL commit, the executor evicts every entry with a
schema dependency on the affected table (`InvalidateByTableId`). Dropping a table or a whole
database evicts by table id or by database. Because the fingerprint already folds in schema
versions, a query issued after a schema change computes a *different* fingerprint and cannot collide
with a pre-change entry regardless.

The dependency index (`DependencyIndex`) maps buckets, point keys, and `(database, table)` schema
keys to the entry ids that depend on them, so invalidation cost scales with the touched keyspaces
and overlapping entries — not with the total number of cached entries.

---

## 6. The publish gate (commit-safe ordering)

The dangerous race is: a query misses, executes, and publishes an entry at the same moment a write
commits into the same keyspace. If the publish landed after the write's invalidation ran, the stale
entry would survive. `CachePublishGate` closes this window with two structures per keyspace:

- a **generation counter**, bumped on every committed write, never decremented;
- an **in-flight mark** count, raised before a commit and cleared after.

The protocol:

**Write path** (in `CommitAsync`): mark the modified keyspaces in-flight *before* the Kahuna commit;
on success, bump their generations and run the cache invalidation *inside the gate's lock*, then
clear the marks; on rollback or failure, just clear the marks.

**Read path**: snapshot the keyspace generations before executing; if a keyspace is already
in-flight, bypass; after execution, publish **only** through `TryPublishUnderGeneration`, which — 
under the same lock the commit path uses — re-checks the generation and inserts the entry only if it
hasn't moved. So a publish either lands entirely before the write's invalidation (and is then
removed by it) or is rejected because the generation advanced. There is no interleaving in between.

The lock is held only across in-memory bookkeeping — never across a Kahuna commit — so it stays
cheap. False rejections (a query re-executing because an unrelated row in the same bucket changed)
are acceptable; a stale entry surviving a committed write is not.

---

## 7. TTL and strict validation (bounding cross-node staleness)

The same-node guarantees above say nothing about a write on a *different* node — that node's commit
never touches this node's in-memory index. Two mechanisms bound the resulting staleness.

**TTL.** Every entry has an expiry (the per-hint `ttl`, or the configured default). Expiry is
checked on probe (an expired entry misses and is removed) and a background sweep periodically drops
expired entries so they don't sit in memory.

**Strict validation.** A `strict` entry is validated against live storage on every hit before its
rows are served (`StrictValidator`), in order:

1. **Schema deps** — if any depended-on table is gone or now at a different schema version, the
   entry is stale.
2. **Point deps** — probe each row key for the latest committed value. Absent (deleted) or
   `LastModified` newer than `T_cache` (updated) means stale.
3. **Range deps** — scan each bucket; any key with `LastModified` newer than `T_cache` means a
   phantom insert or untracked change.

All reads are non-transactional (latest committed). Validation is bounded by a probe-key limit and
**fails closed** — if it would exceed the limit, the entry is treated as stale and re-executed
rather than trusted. A failed validation evicts the entry so the next probe doesn't re-validate a
known-stale one.

Two correctness details worth knowing:

- **Physical deletes need point deps.** A deleted row's key is gone, so a range scan can't see it —
  only the row's point dependency catches the delete. That is why a strict entry whose point deps
  were truncated by the cap is **not stored at all**: without complete point deps it could not
  detect a delete, so it bypasses publish.
- **Strict needs a snapshot.** If the autocommit read has no pinned HLC snapshot, `T_cache` would be
  meaningless and every `LastModified` comparison would fail; such strict entries are not published
  (they serve live rows instead).

Strict validation can be as expensive as a range scan. It is an opt-in correctness mode for
multi-node reads, not a general performance path.

---

## 8. Fingerprinting

The fingerprint must never let two logically different queries share an entry. It is a 128-bit
non-cryptographic hash (XxHash128) over an **injective** canonical string — not a security boundary,
just accidental-collision avoidance, so a fast hash over an unambiguous encoding is the right tool.

Into the canonical form go: the database id, the family name, the query shape id, every bound
parameter (sorted by name), the schema deps (sorted by table name, with version), and the cache
options. The encoding is careful in two ways that matter:

- **Typed values.** Each value is tagged by type, so integer `1`, string `"1"`, `NULL`, and an
  object id never encode the same way.
- **Length-prefixing.** Every variable-length value is length-prefixed, so a value that contains the
  structural delimiters can't be confused with a different set of values. Float formatting is
  culture-invariant so a node's locale can't change the fingerprint.

The query **shape id** is a separate, smaller (64-bit) hash of the query's structure with literals
replaced by placeholders; it is shared with the plan cache and identifies "same query modulo
constants." The result fingerprint always folds the literal values back in — the plan cache
intentionally ignores them, the result cache must not.

---

## 9. Configuration

The cache is **on by default** (it does nothing until a query opts in with a `{cache=…}` hint).
All knobs are YAML-only (operational tuning), under the same `config.yml` the rest of the server
reads.

| Key | Default | Meaning |
|-----|---------|---------|
| `query_result_cache_enabled` | `true` | Master switch. When off, hints are reported as `bypass` / `cache-disabled` and no cache/gate work is done. Turn off to eliminate all cache memory and the small per-write gate bookkeeping on deployments that never use `{cache=…}`. |
| `query_result_cache_default_ttl_ms` | `5000` | TTL for entries without a per-hint `ttl`. |
| `query_result_cache_max_entries` | `1024` | Entry count cap (LRU eviction beyond it). |
| `query_result_cache_max_bytes` | `67108864` | Total byte budget across all entries (64 MiB). |
| `query_result_cache_max_entry_bytes` | `1048576` | Per-entry byte cap (1 MiB); larger results are not stored. |
| `query_result_cache_max_entry_rows` | `10000` | Per-entry row cap; larger results are not stored. |
| `query_result_cache_max_deps` | `4096` | Total dependency cap per entry; exceeding it bypasses publish. |
| `query_result_cache_max_point_deps` | `2048` | Point-dep cap per entry; exceeding it drops points (and bypasses strict entries). |
| `query_result_cache_max_ranges` | `256` | Range-dep cap per entry. |
| `query_result_cache_singleflight_wait_ms` | `250` | Reserved for a future single-flight gate (de-duplicating concurrent misses of the same entry). Not yet active — concurrent misses currently each compute and race to publish, and the gate keeps that safe. |
| `query_result_cache_strict_validation_max_keys` | `10000` | Probe-key budget for strict validation; exceeding it fails closed. |
| `query_result_cache_sweep_interval_ms` | `10000` | Background TTL-sweep interval. |

Any cap breach fails safe: the query still returns correct live rows, and the cache either bypasses
storing the entry or evicts to make room — it never stores an incomplete or oversized entry.

**Operator guidance.** The cache is on by default but inert until a query opts in, so add a
`{cache=…}` hint to read-heavy, frequently-repeated, parameterized queries where the data changes
far less often than it is read. Nothing without a hint is ever cached. In a cluster, remember each
node caches independently: with the default non-strict entries, a reader on one node can lag a write
on another by up to the TTL. Lower `default_ttl_ms` to tighten that bound, or use `{cache=…, strict}`
on the specific queries that must never lag (at the cost of a validation scan per hit). Size
`max_bytes` / `max_entries` to the working set you actually want resident; oversized results simply
won't be cached. If a deployment never uses hints and you want to shave the small per-write gate
bookkeeping, set `query_result_cache_enabled: false`.

---

## 10. Limitations and non-goals

- **Single-node L1 only.** No cross-node sharing, no eager cross-node invalidation, no persistence
  across restart. Cross-node freshness is TTL-bounded or strict-validated, never eager.
- **Single-table reads only.** Joins bypass the cache. A hint on a join, or inside a subquery, is
  inert.
- **Autocommit reads only.** Explicit transactions (including `READ ONLY`) always read live storage.
- **Deterministic reads only in spirit.** The cache keys on values, not on wall-clock or session
  state; queries whose results depend on non-deterministic inputs should not be hinted (they would
  cache a single evaluation).
- **Write paths never read the cache.** `UPDATE`/`DELETE … WHERE` evaluate against live storage.

Natural future directions include cluster-wide eager invalidation, an index-scan fence that narrows
the row-bucket snapshot, caching for joins (which requires fencing every involved table), and a
shared or persistent second tier. None of these change the correctness contract above.

---

## 11. Where the code lives

Core (`CamusDB.Core/Cache/`):

- `IQueryResultCache` / `QueryResultCache` / `NullQueryResultCache` — the cache contract, the
  in-memory implementation (LRU, byte accounting, TTL sweep, secondary indexes), and the
  disabled-state stand-in used by tests.
- `CachePublishGate` / `CacheGenerationToken` — the commit-safe generation fence and in-flight marks.
- `CachedQueryRunner` — wraps execution so publish happens only after a full, successful drain, with
  cap and dependency checks.
- `QueryDependencyCollector` / `QueryDependencySet` / `DependencyIndex` — dependency capture, the
  immutable captured set, and the bucket→entry index used for invalidation.
- `ResultFingerprintBuilder` — the injective canonical encoding and 128-bit hash.
- `StrictValidator` — per-hit validation of strict entries against live storage.
- `CacheHintOptions` / `QueryCacheStatus` (statuses + bypass reasons) / `CacheMetadataHolder` — the
  parsed hint, the result/reason enums, and the response side-channel.

Wiring:

- `SQLParser/` — the `{cache=…}` hint grammar and the `EVICT CACHE` statement.
- `Commands/Executor/Controllers/QueryExecutor.cs` — the read path (`QueryWithCache`) and scan-level
  dependency recording.
- `Commands/Executor/Controllers/Queries/QueryScanner.cs` / `QueryJoinExecutor.cs` — per-scan
  dependency recording (the join sites are recorded but currently unused, since joins bypass).
- `Transactions/KvTransactionsManager.cs` — the commit-time invalidation and gate protocol.
- `Commands/Executor/CommandExecutor.cs` — DDL invalidation hooks and the `EVICT CACHE` handlers.
- `CamusDB/Program.cs` — constructs the singleton cache when enabled (or passes none when disabled).
- `Config/` — the `query_result_cache_*` knobs.
