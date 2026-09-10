# Database Branching (Copy-on-Write Forks) — Architecture & Developer Guide

> **Audience:** engineers maintaining or extending CamusDB's database lifecycle, storage,
> and catalog layers, and operators running clusters that use branching.
> **Scope:** how a database is forked, how a branch reads its ancestors and writes its own
> overlay, how the fork's frozen view is kept durable, and how the whole lifecycle recovers
> from crashes and races.

> **Overview.** `CREATE DATABASE feature_x BRANCH FROM prod` mints an instant, isolated
> point-in-time fork. The branch shares the parent's bytes until it diverges: reads see the
> parent as of the fork instant, writes are private to the branch, and the parent keeps
> evolving and never sees the branch. It is copy-on-write over the existing Kahuna KV store —
> no data is copied at fork time, only a small amount of schema metadata. The hard parts are
> not the high-level model; they are durability (keeping the parent's fork-time versions
> readable under GC), recoverability (crash-safe create/drop), and cross-node concurrency
> (fencing a drop against a concurrent branch-create). Known limitations are in §11.

---

## 1. Mental model

Every database is a node in a **branch tree**. There is no "normal database" versus "branch":
a root database is just a branch with an empty ancestry. One code path serves both.

A database carries an **immutable ancestry** — the chain of parents it was forked from, each
with the HLC timestamp of the fork:

```text
DatabaseRegistryEntry {
  Id                    -- stable opaque id; never changes, never reused
  Name                  -- user-visible; mutable via RENAME
  Ancestors: [ (DatabaseId, ForkTimestamp) ]   -- nearest parent first; empty for roots
  ImmediateParentHoldId -- the snapshot-floor hold this branch owns on Ancestors[0]
}
```

From that, a descriptor derives its **read lineage**, nearest level first:

```text
[ (self.Id, live) ] + entry.Ancestors
```

Three ideas are the whole story; the rest is detail.

- **The database id is the branch namespace.** All of a database's data lives under keys
  prefixed by its id (§2). A branch shares its parent's *table ids* (schema is copied
  preserving them), so it can address the same logical inherited rows under a different id
  prefix.
- **Reads walk the lineage; writes touch only level 0.** A read tries the branch's own
  overlay first, then each ancestor *as of that ancestor's fork timestamp*, stopping at the
  first hit or tombstone. A write or delete only ever writes to the branch's own overlay
  (level 0) — never to an ancestor (§4).
- **The fork instant is an HLC timestamp, and the parent's history at that instant must stay
  readable.** That is the entire durability problem, solved by an upstream Kahuna
  **snapshot-floor hold** taken at `forkT` (§6).

### 1.1 Vocabulary

**`forkT`** — the HLC timestamp at which a branch was forked from its immediate parent. Minted
by starting and immediately rolling back a Kahuna transaction on the source, so it is causally
after every write already committed there (a bare local-clock read could lag a partition
actor's clock in a multi-partition deployment).

**Overlay / level 0** — a database's own keyspace (`{dbId}:…`). A branch's writes, deletes
(as tombstones), and new schema live here.

**Tombstone** — a level-0 marker meaning "deleted or suppressed at this level," distinct from
"never written here." Needed because Kahuna's `DoesNotExist` cannot tell those apart (§4).

**Snapshot-floor hold** — a leased, refcounted, Raft-replicated retention pin in Kahuna that
keeps the revision current at a timestamp readable, defeating revision reclamation (§6).

**Ancestry / lineage** — persisted `Ancestors` (excludes self) versus the derived read lineage
(includes self at level 0). Ancestry is immutable; only `Name` and `CreatedAt` ever change.

---

## 2. Keyspace invariants

All logical data for a database is under its id. These prefixes are load-bearing — do not
casually add or remove slashes or move the `|` (the last `/` ends a Kahuna key space, and the first
`|` ends the placement group that hash routing places):

```text
row                  {dbId}:{tableId}|r/{rowIdHex24}
unique index         {dbId}:{tableId}|i:{indexId}/{encodedKey}
non-unique index     {dbId}:{tableId}|i:{indexId}/{encodedKey}{rowIdHex24}
statistics           {dbId}:stats:{tableId}

schema meta bucket   {dbId}/meta
schema version       {dbId}/meta/version
table schema         {dbId}/meta/table:{tableId}
table history        {dbId}/meta/history:{tableId}:{version}
keyspace catalog     {dbId}/meta/keyspace:{tableId}    -- grow-only list of every index id ever allocated
coordinator jobs     {dbId}/meta/coordinator:{tableId}~{elementName}
```

Registry and lifecycle bookkeeping live under the reserved `_system/` prefix:

```text
name → entry         _system/dbregistry/db:{name}
id sequence          _system/dbregistry/seq
pending-create       _system/dbregistry/pending:{branchId}     -- crash handle for an unpublished branch (owner-tagged)
drop-intent          _system/dbregistry/drop-intent:{dbId}     -- cross-node drop/create fence (owner-tagged)
drop-in-progress     _system/dbregistry/dropping:{dbId}        -- crash-resume handle for a purge (owner-tagged)
```

Two invariants worth internalizing:

- **Index keys use the immutable `TableIndexSchema.Id`**, never the mutable index name.
  Otherwise a branch that drops and re-adds an index with the same name could fall through to
  the parent's old entries.
- **The keyspace catalog is grow-only.** It records every index id ever allocated for a table
  (it is *not* trimmed on `DROP INDEX`/`DROP TABLE`), so `DROP DATABASE` can purge the overlay
  of objects that are no longer in the live schema.

---

## 3. Value envelopes and tombstones

Row and index payloads carry a one-byte `BranchKvCodec` envelope prefix distinguishing a
**Value** from a **Tombstone**. Read paths decode it in `GetRow`, `ScanRows`, `LookupUnique`,
`ScanIndex`, and `WriteRowsBatch`; a tombstone returns not-found / is skipped and suppresses
same-key records at deeper levels.

Delete semantics differ by database kind — a deliberate two-path rule, not an inconsistency:

- **On a branch** (`ancestorStores.Length > 0`), a logical delete writes a **tombstone** to
  level 0 rather than physically deleting. This is **mandatory for correctness**: a physical
  delete is indistinguishable from "never written at this level," so the ancestry merge would
  fall through and return the inherited ancestor value instead of not-found.
- **On a root** (no ancestors), a delete is a physical KV delete. A root has nothing beneath it
  to fall through to, so tombstone and physical delete are semantically equivalent — and physical
  delete is the cheaper choice (immediate reclamation, no decode overhead on later scans). This
  is an **intentional optimization**.

The subtle correctness trap here (fixed early): batch paths must honor the envelope too.
`DeleteRowsBatch` originally bypassed the tombstone path via a physical batch delete; on a
branch it must write tombstone items instead. And `RowUpdater` must **skip** the delete+insert
of an unchanged index key (`oldKey.CompareTo(newKey) == 0`) — on a branch the delete tombstone
would otherwise block the following `SetIfNotExists` and wrongly raise `DuplicateUniqueKeyValue`.

---

## 4. Reads

Lineage resolution is centralized in `KvTableStore`, constructed by `TableOpener` with an
`ancestorStores[]` array — one entry per ancestry level, nearest first, each carrying its
`forkTimestamp`. `ancestorStores.Length > 0` is what distinguishes a branch store from a root.

**Timestamp selection.** Level 0 reads at the live transaction timestamp (or `HLCTimestamp.Zero`
for autocommit); ancestor level *i* reads as of `ancestor[i].ForkTimestamp`. Ancestor reads use
`HLCTimestamp.Zero` as the *transaction* id (no live txn) and the fork timestamp as the *read*
timestamp — the same MVCC snapshot pattern used everywhere ancestor data is touched.

**Point reads / unique lookups** probe the lineage nearest-first: a `Value` returns the bytes, a
`Tombstone` stops the walk and returns not-found, a miss descends to the next level.

**Scans** run one ordered iterator per lineage level and stream a **k-way merge** (a
`PriorityQueue`) with nearest-wins resolution and a seen-set for de-duplication. This is bounded
in memory even for `LIMIT 1` against a huge parent — levels are merged lazily, never
materialized. Merge keys are the *logical suffix* (the dbId prefixes differ per level): `rowId`
for rows, `encodedKey` for unique indexes, `encodedKey + rowId` for non-unique.

**Uniqueness and primary keys** are checked over the **union** of level 0 and the reachable
ancestry, tombstone-aware (`ResolveBranchUniqueFlagsAsync`): a key present only in an ancestor
still conflicts on the branch; a key tombstoned in the branch can be re-inserted (tombstone
replace); ancestor uniqueness is unaffected by branch state.

Serializable read-write transactions acquire the shared point/range locks in the **level-0**
keyspace only; ancestor levels are frozen and need no locks.

---

## 5. DDL and lifecycle isolation

- **Parent DDL after a fork is invisible to the branch** — the branch owns an independent schema
  checkpoint copied at `forkT` (§7).
- **Branch DDL is invisible to the parent and siblings** — it writes only the branch dbId's
  metadata/log. Branch `CREATE TABLE`/`ADD INDEX` allocate new ids in the branch; renames stay
  metadata-only and branch-local.
- **Branch `DROP TABLE`/`DROP INDEX` do not scan or tombstone inherited data.** Once the branch
  schema no longer references an object, its inherited data is naturally unreachable; only the
  branch-local overlay for that object is physically purged (`PurgeLocalRowOverlayAsync` /
  `DropIndexEntries`). This keeps branch drops O(branch overlay), not O(parent data).

---

## 6. Frozen-view durability — the snapshot floor

Reading an ancestor at `forkT` is only correct while the parent's revision current at `forkT`
is still retained. Kahuna bounds its in-memory revision archive (default 16 per key) and can
prune on-disk history, so a long-lived branch would otherwise see its frozen view silently
reclaimed under parent churn.

CamusDB solves this with the upstream **Kahuna snapshot floor** (Kahuna ≥ 0.6.0). The client
API on `IKahuna` — all auto-routed to the system-partition leader, all Raft-replicated:

| Op | Purpose |
|----|---------|
| `LocateAndAcquireSnapshotHold(holderId, timestamp, leaseMs)` | Pin all revisions at/after `timestamp`; idempotent by `(holderId, timestamp)`. |
| `LocateAndRenewSnapshotHold(holdId, leaseMs)` | Extend the lease. |
| `LocateAndReleaseSnapshotHold(holdId)` | Release; the floor rises when the lowest hold is released. |
| `GetSnapshotFloor()` | Introspection: current floor + live-hold count. |

**What CamusDB does with it:**

- **One hold per branch, on its immediate parent.** At fork, the branch acquires a hold on the
  parent at `forkT`, with `holderId = branchId`. The hold id is persisted on the registry entry
  (`ImmediateParentHoldId`). Deeper ancestor levels are *not* held directly — they stay pinned
  because a database with live descendants cannot be dropped (§8), so each intermediate branch
  keeps its own parent hold alive.
- **Block if the hold is not granted.** If the acquire does not return `Set`, branch creation
  **fails** — CamusDB never registers a branch whose frozen view it cannot guarantee.
- **Release on leaf drop.** Dropping a branch releases its parent hold so that history can be
  reclaimed.
- **Leader-owned renewal, tied to existence, not open-ness.** A hold must live as long as the
  branch is *registered*, even if no client has it open. `SnapshotHoldRenewer` runs on exactly
  one node — the leader of the database-registry partition (`EmbeddedKahuna.AmILeaderForKeyAsync`)
  — and every `lease/3` renews the hold of every registered branch. It reads the branch set from
  a **persistent registry scan** (`ScanAllEntriesAsync`), not the local cache, so a branch
  created on another node after this leader started is not missed. The election is re-checked
  each tick, so failover hands the sweep to the new leader.

A key property that makes this cheap to reason about: a **live hold keeps the fork-boundary
revision in memory regardless of storage backend**, and the *same* hold protects both the
ancestor row/index reads and the metadata copy at `forkT`.

**Fail-closed on a lost hold, fail-recovered on a mere lapse.** Since Kahuna 1.7.3 a hold's
protection ends only when the hold is **removed from the replicated registry** — an explicit
release, or the reaper's purge of a hold whose lease lapsed without renewal. A bare lapse is
recoverable: a registered hold keeps constraining reclamation even while its lease is expired,
and the next renew revives it. In particular, full downtime longer than the lease heals itself —
holds loaded from the durable registry are exempt from the purge for a startup grace window
(`SnapshotHoldStartupGraceWindow`, default 5 min), and the renewer's first sweep runs immediately
at startup so revival lands well inside it. Once a hold *is* removed, Kahuna answers
`DoesNotExist` to every later renew, and re-acquiring at the old timestamp does not bring
reclaimed history back. Without a fence, an ancestor read past that point reports reclaimed rows
as confirmed absences — a successful, silently incomplete (possibly empty) result. Three layers
turn that into a hard error instead:

- **Read-side guard (`BranchSnapshotHoldGuard`).** Every branch descriptor carries one guard,
  shared by all its ancestor-level stores. Each ancestor probe/scan verifies, after the Kahuna
  read and before its result (value, tombstone, *or absence*) is used, that the branch's whole
  hold chain — its own hold plus each non-root ancestor's hold — is still protected. The proof:
  Kahuna only renews a *registered* hold (renewing revives a lapsed-but-registered one), so one
  confirmed renew proves the chain's protection never lapsed up to that instant; the guard caches
  the confirmation and re-verifies once it is older than half the lease, so the fast path is one
  tick comparison per ancestor access. A definitive refusal — the hold was removed —
  latches the branch permanently and throws `BranchSnapshotProtectionLost` (HTTP 410); a
  transient verification failure past 0.8 × lease throws a retryable error instead of guessing.
  This covers descendants (a grandchild verifies the middle branch's hold too), reopen, failover,
  and renewer starvation — the sweep is an optimization for unopened branches, not the safety.
- **Durable lost marker.** When the renewer sweep (or a guard) gets a definitive refusal, it
  writes `dbregistry/holdlost:{branchId}`; a later open pre-latches the guard from the marker so
  the very first read fails fast with the recorded reason. Correctness never depends on the
  marker — the refused renew is visible on every node — and dropping the branch clears it.
- **Creation keep-alive.** During branch creation nothing else renews the fresh hold (the sweep
  only sees *registered* branches), and the metadata copy has no duration bound. Creation runs a
  private keep-alive (`SnapshotHoldLease.AdoptForKeepAlive`) for the whole create and validates
  it before `RegisterAsync` and again before returning success, so "published" implies
  "protected". `branch_snapshot_hold_lease_ms` must be ≥ 5000 for the same reason: the renewer
  never ticks faster than once a second, and a lease inside that tick would lapse by design.

---

## 7. Branch creation, step by step

`CommandExecutor.CreateBranchDatabaseAsync` (under the **source's** `SchemaDdlSemaphore`):

1. **Existence check via the persistent registry** (`TryResolveEntryAsync`, not the local cache).
   `IF NOT EXISTS` opens and returns an already-existing target; a plain create rejects early —
   both *before* allocating an id, acquiring a hold, or copying metadata.
2. **Schema stability.** The source must be schema-stable: `HeadSchemaVersion == SchemaVersion`,
   all elements `Public`, no in-flight coordinator jobs. Otherwise the copy could capture a
   half-applied online schema change.
3. **Mint `forkT`** (begin+rollback a source transaction — the causal fence) and **allocate
   `branchId`** from the monotonic id sequence.
4. **Acquire the snapshot hold** on the source at `forkT`; block if not `Set`.
5. **Write the pending-create marker** (`TrackPendingBranchAsync`) — a *confirmed-durable* write
   (it retries `MustRetry`/`WaitingForReplication` and throws on any terminal non-`Set`), placed
   *before* the metadata copy inside the try block. If it fails, the hold is released and the copy
   never runs.
6. **Copy schema metadata as of `forkT`** (`CopyMetaForBranchAsync` — an as-of-`forkT` scan, so a
   remote DDL committed after `forkT` is excluded, keeping the branch schema consistent with the
   ancestor data it inherits). All copied keys are written in one transaction.
7. **Publish the registry entry** (`RegisterAsync`), then **check the drop-intent fence and
   re-read the source's liveness by its immutable id** (§8).
8. **Clear the pending marker** on success; on abort, **prove the registration is not live
   first**, then release the hold and inline-purge any copied metadata.

The invariant that falls out of this ordering: **every `{branchId}/meta/…` namespace is either
registered, or has a durable pending marker** the startup scrubber will find. There is no window
where metadata exists with no recovery handle.

Publication and abort obey two further rules, because a failed `RegisterAsync` does not prove the
registration is absent:

- **A committed registration cannot be reported as a failure.** `RegisterAsync` returns once its
  transaction commits; the post-commit work (cache update, cross-node coherence stamp) is
  best-effort and never throws. Before this held, a transport fault on the coherence stamp made
  the create's abort path purge the metadata, release the hold, and clear the marker of a branch
  whose registry entry had already durably committed — leaving a live name backed by nothing.
- **Destructive abort cleanup requires proof of unpublication.** The abort path calls
  `RetractRegistrationAsync`, which reads the durable entry under its exclusive lock and deletes
  it only when it still carries this create's id — so a replacement registered concurrently under
  the same name is never removed. An absent entry counts as proof only when the registration
  failure had a resolved outcome; when the commit outcome is unknown
  (`TransactionFinalizeUnresolved`), the entry may still become durable, so the create keeps the
  hold, the metadata, and the pending marker, and surfaces a retryable indeterminate error
  instead of destroying state it cannot prove unpublished.

---

## 8. Drop, descendants, and cross-node fences

`DROP DATABASE` is a multi-step saga, not a single transaction, so its correctness rests on
several layered fences:

- **Descendant block (`CADB0508`).** A database with any registered descendant cannot be
  dropped. `HasLiveDescendantsAsync` checks the in-memory cache *then* does a persistent registry
  scan (to see branches registered on other nodes). Drop leaf-first: leaf → parent → root.
- **Same-node fence (semaphore).** `DropDatabase` opens the target and holds its
  `SchemaDdlSemaphore` across the descendant re-check and `UnregisterAsync`; branch-create holds
  the *source's* semaphore across its own publish — the same id-keyed descriptor, so they mutually
  exclude on one node.
- **Cross-node fence (drop-intent key).** Before its descendant scan, `DropDatabase` writes a
  Raft-replicated `drop-intent:{dbId}` (`SetIfNotExists`) and holds it through the keyspace purge;
  branch-create checks it *after* `RegisterAsync`. Raft linearizability guarantees exactly one
  wins — either branch-create sees the intent and retracts, or drop's descendant scan sees the
  new child and aborts. The intent is released on every exit path.
- **Source liveness re-read (completed-drop fence).** The intent key only covers a drop still in
  flight: a create that stalls between its metadata copy and `RegisterAsync` can resume after the
  drop has fully finished and released the intent. So directly after the intent check,
  branch-create re-reads the source's liveness from the persistent registry by its **immutable
  id** (`TryResolveNameByIdAsync`, never the local cache) and aborts when the id is gone. The
  order of the two reads closes the window: a drop whose scan missed the child holds the intent
  from before the child's `RegisterAsync` until after its own unregister, so the create either
  sees the intent, or its later liveness read sees the unregistered id. Ids are never reused, so
  a recreate of the name under a new id still aborts, while a rename (same id) does not.
- **Meta-last, resumable, paged purge.** The keyspace purge reads the catalog first, deletes
  row/index/stats, and deletes **meta (catalog included) last** — so a crashed purge can be
  resumed from the still-present catalog. A `dropping:{dbId}` marker is written before
  `UnregisterAsync` and cleared only after the purge completes; startup resumes any interrupted
  purge. Deletes are **paged** in bounded batches (`CamusDBConfig.KeyspacePurgeBatchSize`) so a
  large database drops in bounded memory.

---

## 9. Crash recovery

Every crash-recovery handle in this feature is a **persistent, owner-tagged marker** plus a
**startup scrubber** (`CommandExecutor.ScrubOrphanBranchNamespacesAsync`, fire-and-forget at
startup). "Owner-tagged" means the marker's value is the writing node's stable Raft id, so a
restarting node reclaims only *its own* crash remnants and never disturbs a marker another live
node currently holds.

| Marker | Written when | Cleared when | On crash, startup does |
|--------|-------------|--------------|------------------------|
| `pending:{branchId}` | before copying branch metadata | after publish, or on clean abort | for own prior-run markers only: re-check registration against the *persistent* registry, take the id's fence, re-check again, then scrub `{branchId}/meta`; a registered id only loses its obsolete marker |
| `drop-intent:{dbId}` | before a drop's descendant scan | after purge / on abort | clear own stale intents (a drop never spans a restart) |
| `dropping:{dbId}` | before a drop's `UnregisterAsync` | after the purge completes | resume the keyspace purge for any own, no-longer-registered id |

Two ordering choices make this robust: the pending marker is written *before* the metadata copy
(so metadata never exists without a handle), and the drop marker is written *before*
`UnregisterAsync` (so an interrupted purge is always resumable, and the meta-last purge order
keeps the catalog available for the resume).

---

## 10. Operating branches

**Snapshot holds pin parent history.** A live branch keeps its parent's fork-time revisions
retained. Long-lived branches over a hot parent therefore hold back reclamation of that parent's
old revisions — expected and correct, but worth watching capacity for. Kahuna exposes these
gauges (under the `Kahuna` meter scope) that operators should dashboard:

- `kahuna.snapshot_floor.live_holds` — one per live branch (roughly). Rising unboundedly means
  branches are being created but not dropped.
- `kahuna.snapshot_floor.effective_floor_ms` — how far back the oldest hold pins history.
- `kahuna.snapshot_floor.missing_protected_version_total` — **must stay 0.** A non-zero value
  means reclamation touched a protected version — a durability fault; alert on it.

**Lease / renewal.** `CamusDBConfig.BranchSnapshotHoldLeaseMs` (default `300_000` = 5 min) sets
the hold lease; the leader-owned renewer refreshes every `lease/3`. Choose it coarse enough that
renewals are not a hot Raft path. If the renewer's node is unhealthy, failover moves the sweep to
the new registry-partition leader; a branch only loses its hold if renewal stops for a full lease.

**Drop order matters.** You cannot drop a database that still has live descendant branches
(`CADB0508`) — drop descendants first, leaf to root. A dropped id is never reused.

**Deep chains cost reads, and are observable.** Read cost is proportional to lineage depth (one
ancestor probe per level on a point-read miss, one extra scan iterator per level). There is no
depth cap, but the read path is instrumented via always-on process-wide counters in
`BranchMetrics` — read them directly (there is no metrics exporter yet; this is the hook point for
one):

- `AncestorProbesTotal` — ancestor-level probes fired by `GetRow`/`LookupUnique` misses. A high
  rate relative to query volume means deep chains are amplifying point reads.
- `ScanIteratorsTotal` — ancestor scan iterators opened by `ScanRows`/`ScanIndex` (increments by
  the lineage depth per branch scan).
- `DeepLineageWarnings` — how many table stores were opened at a lineage depth ≥
  `BranchMetrics.LineageWarningThreshold`. Each such open also logs a warning naming the table and
  depth. A rising count is the signal to consider compacting or rebasing the chain.

Roots (no ancestry) never touch these paths, so root databases add no counter overhead.

**Config knobs.**

| Knob | Default | Effect |
|------|---------|--------|
| `BranchSnapshotHoldLeaseMs` | `300_000` | Snapshot-hold lease; renewer runs at `lease/3`. |
| `KeyspacePurgeBatchSize` | `512` | Max keys per batch in the `DROP DATABASE` purge (memory bound). |
| `BranchMetrics.LineageWarningThreshold` | `10` | Lineage depth at/above which opening a table store logs a warning and increments `DeepLineageWarnings`. Advisory, not a hard cap. |

---

## 11. Limitations and future work

- **No branch compaction or rebase.** Unbounded branch depth is intentional, and deep chains are
  now observable (the `BranchMetrics` counters and the depth-threshold warning in §10). But there is
  no mechanism yet to *shorten* a deep chain — no compaction of a branch into a standalone database,
  no rebase onto a different ancestor. The metrics tell an operator when a chain is getting expensive;
  the remedy is still manual (recreate the branch as a root).
- **Large branch `DROP TABLE`/`DROP INDEX`.** These delete inside one DDL transaction and are
  therefore capped by the per-transaction mutation limit; arbitrarily large single-object drops
  on a branch would need decoupling from the DDL transaction. (`DROP DATABASE` is already paged.)
- **Cross-node cluster tests.** Several fences are covered by single-node simulations of the
  cross-node scenario (two registry instances, injected markers) rather than true multi-node
  tests; the mechanisms are topology-independent, but network/replication fidelity is not
  separately exercised.

## Non-goals

Merge-back into the parent, cross-branch transactions (a transaction touches exactly one
database), branch compaction/rebase to shorten deep chains, reparenting descendants on drop, and
physical data-distribution changes — branching rides the existing Kahuna routing model.

---

## Inspecting the branch tree from SQL

Two read-only server-level statements let you explore the branch hierarchy without a database
context. Both resolve names through the persistent registry, so a database created on another
node is visible.

### `SHOW BRANCHES FROM <database>`

Returns every transitive descendant of `<database>` — direct children at depth 1, their children
at depth 2, and so on. Rows are ordered depth-ascending, then database-name ascending.

```sql
SHOW BRANCHES FROM prod;
```

| column | description |
|--------|-------------|
| `database` | descendant name |
| `id` | descendant's stable opaque id |
| `depth` | 1 = direct child, 2 = grandchild, … |
| `parent` | immediate-parent name |
| `fork_timestamp` | HLC string when this branch was forked from its parent |

A leaf database (no descendants) returns zero rows.

### `SHOW ANCESTORS FROM <database>`

Returns the full ancestry chain of `<database>` from nearest parent to root, one row per
generation.

```sql
SHOW ANCESTORS FROM feature_x;
```

| column | description |
|--------|-------------|
| `database` | ancestor name |
| `id` | ancestor's stable opaque id |
| `depth` | 1 = immediate parent, 2 = grandparent, … |
| `fork_timestamp` | HLC string of the fork that created the branch immediately below this ancestor |

A root database (no ancestors) returns zero rows.

Unknown database name → database-not-found error.

---

## Where the code lives

| Concern | Primary types |
|---------|---------------|
| Ancestry model | `DatabaseRegistryEntry`, `DatabaseBranchAncestor`, `DatabaseRegistry` |
| Branch-aware reads/writes | `KvTableStore` (`ancestorStores[]`), `BranchKvCodec`, `TableOpener` |
| Union uniqueness | `KvBranchReader.ResolveBranchUniqueFlagsAsync`, `RowUpdater`, `RowInserter` |
| Create / drop / rename | `CommandExecutor` (`CreateBranchDatabaseAsync`, `DropDatabase`), `DatabaseDropper` |
| Metadata copy | `CatalogsManager.CopyMetaForBranchAsync` |
| Durability | `SnapshotHoldRenewer`, `EmbeddedKahuna.AmILeaderForKeyAsync`, `IKahuna` snapshot-floor API |
| Recovery | `CommandExecutor.ScrubOrphanBranchNamespacesAsync`, the marker methods on `DatabaseRegistry` |
| Branch tree queries | `SchemaQuerier.ShowBranches`, `SchemaQuerier.ShowAncestors` |
