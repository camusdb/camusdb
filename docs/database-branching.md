# Database Branching (Copy-on-Write Forks) — Architecture & Developer Guide

> **Audience:** engineers maintaining or extending CamusDB's database lifecycle, storage,
> and catalog layers, and operators running clusters that use branching.
> **Scope:** how a database is forked and how a branch reads its ancestors and writes its
> overlay. It also covers how the fork's frozen view is kept durable and how the lifecycle
> recovers from crashes and races.

> **Overview.** `CREATE DATABASE feature_x BRANCH FROM prod` mints an instant, isolated
> point-in-time fork. The branch shares the parent's bytes until it diverges. Reads see the
> parent as of the fork instant, and writes are private to the branch. The parent continues to
> change and does not see the branch. Branching is copy-on-write over the existing Kahuna KV
> store: at fork time, CamusDB copies a small amount of schema metadata and no row data. The hard parts
> are durability (keeping the parent's fork-time versions
> readable under GC), recoverability (crash-safe create/drop), and cross-node concurrency
> (fencing a drop against a concurrent branch-create). Known limitations are in §11.

---

## 1. Mental model

Each database is a node in a **branch tree**. A root database is a branch with an empty
ancestry. This means that one code path serves roots and branches.

A database carries an **immutable ancestry**: the chain of parents it was forked from, each
with the HLC timestamp of the fork. The registry entry stores the ancestry next to the
database's id and name.

```text
DatabaseRegistryEntry {
  Id                    -- stable opaque id; never changes, never reused
  Name                  -- user-visible; mutable via RENAME
  Ancestors: [ (DatabaseId, ForkTimestamp) ]   -- nearest parent first; empty for roots
  ImmediateParentHoldId -- the snapshot-floor hold this branch owns on Ancestors[0]
}
```

From that, a descriptor derives its **read lineage**, nearest level first, by placing the
database itself at level 0 ahead of its ancestors.

```text
[ (self.Id, live) ] + entry.Ancestors
```

The model rests on three ideas.

- **The database id is the branch namespace.** All of a database's data is stored under keys
  prefixed by its id (§2). A branch shares its parent's *table ids* (schema is copied
  preserving them). It can therefore address the same logical inherited rows under a different
  id prefix.
- **Reads walk the lineage; writes touch only level 0.** A read tries the branch's overlay and
  then each ancestor *as of that ancestor's fork timestamp*. It stops at the first hit or
  tombstone. A write or delete writes only to the branch's overlay (level 0). Ancestors are not
  written (§4).
- **The fork instant is an HLC timestamp, and the parent's history at that instant must stay
  readable.** An upstream Kahuna **snapshot-floor hold** taken at `forkT` protects that history
  from reclamation (§6).

### 1.1 Vocabulary

**`forkT`** — the HLC timestamp at which a branch was forked from its immediate parent. It is
minted by starting and immediately rolling back a Kahuna transaction on the source. This places
it causally after all writes already committed there (a bare local-clock read could lag a
partition actor's clock in a multi-partition deployment).

**Overlay / level 0** — a database's own keyspace (`{dbId}:…`). A branch's writes, deletes
(as tombstones), and new schema live here.

**Tombstone** — a level-0 marker meaning "deleted or suppressed at this level," distinct from
a key that was not written at this level. Needed because Kahuna's `DoesNotExist` cannot tell
those apart (§4).

**Snapshot-floor hold** — a leased, refcounted, Raft-replicated retention pin in Kahuna. It
protects the revision current at a timestamp from revision reclamation (§6).

**Ancestry / lineage** — persisted `Ancestors` (excludes self) versus the derived read lineage
(includes self at level 0). Ancestry is immutable; only `Name` and `CreatedAt` can change.

---

## 2. Keyspace invariants

All logical data for a database is under its id. These prefixes are load-bearing. Do not
casually add or remove slashes or move the `|`: the last `/` ends a Kahuna key space, and the
first `|` ends the placement group that hash routing places.

```text
row                  {dbId}:{tableId}|r/{rowIdHex24}
unique index         {dbId}:{tableId}|i:{indexId}/{encodedKey}
non-unique index     {dbId}:{tableId}|i:{indexId}/{encodedKey}{rowIdHex24}
large value          {dbId}:{tableId}|v/{rowIdHex24}{ordinalHex4}  -- a compressed or raw cell stored out of its row
statistics           {dbId}:stats:{tableId}

schema meta bucket   {dbId}/meta
schema version       {dbId}/meta/version
table schema         {dbId}/meta/table:{tableId}
table history        {dbId}/meta/history:{tableId}:{version}
keyspace catalog     {dbId}/meta/keyspace:{tableId}    -- grow-only list of every index id ever allocated
coordinator jobs     {dbId}/meta/coordinator:{tableId}~{elementName}
rewrite cursor       {dbId}/meta/storagerewrite:{storageId}   -- progress of ALTER TABLE ... REWRITE STORAGE
refresh jobs         {dbId}/meta/mvrefresh:{viewTableId}       -- in-flight materialized-view rebuild; never copied to a branch
```

Registry and lifecycle bookkeeping are stored under the reserved `_system/` prefix.

```text
name → entry         _system/dbregistry/db:{name}
id sequence          _system/dbregistry/seq
pending-create       _system/dbregistry/pending:{branchId}     -- crash handle for an unpublished branch (owner-tagged)
drop-intent          _system/dbregistry/drop-intent:{dbId}     -- cross-node drop/create fence (owner-tagged)
drop-in-progress     _system/dbregistry/dropping:{dbId}        -- crash-resume handle for a purge (owner-tagged)
hold-lost            _system/dbregistry/holdlost:{dbId}        -- durable record that a snapshot hold was removed (§6)
```

Two invariants apply to these keys.

- **Index keys use the immutable `TableIndexSchema.Id`.** A key that used the mutable index
  name would let a branch that drops and re-adds an index with the same name fall through to
  the parent's old entries.
- **The keyspace catalog is grow-only.** It records all index ids ever allocated for a table
  and is *not* trimmed on `DROP INDEX`/`DROP TABLE`. This lets `DROP DATABASE` purge the
  overlay of objects that are no longer in the live schema.

---

## 3. Value envelopes and tombstones

Row and index payloads carry a one-byte `BranchKvCodec` envelope prefix distinguishing a
**Value** from a **Tombstone**. Read paths decode it in `GetRow`, `ScanRows`, `LookupUnique`,
`ScanIndex` and `WriteRowsBatch`. A tombstone returns not-found or is skipped, and it suppresses
same-key records at deeper levels.

Delete semantics differ by database kind, and both paths are deliberate.

- **On a branch** (`ancestorStores.Length > 0`), a logical delete writes a **tombstone** to
  level 0 and does not physically delete the key. This is **mandatory for correctness**. A
  physical delete cannot be distinguished from a key that was not written at this level. The
  ancestry merge would then fall through and return the inherited ancestor value, where it
  should return not-found.
- **On a root** (no ancestors), a delete is a physical KV delete. Below a root there is no level
  to fall through to. Tombstone and physical delete are therefore semantically equivalent, and
  physical delete is the cheaper choice (immediate reclamation, no decode overhead on later
  scans). This is an **intentional optimization**.

Batch paths must honor the envelope too (a correctness bug here was fixed early).
`DeleteRowsBatch` originally bypassed the tombstone path via a physical batch delete; on a
branch it must write tombstone items. `RowUpdater` must also **skip** the delete+insert of an
unchanged index key (`oldKey.CompareTo(newKey) == 0`). On a branch, the delete tombstone would
otherwise block the following `SetIfNotExists` and wrongly raise `DuplicateUniqueKeyValue`.

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
`PriorityQueue`) with nearest-wins resolution. Merge keys are the *logical suffix* (the dbId
prefixes differ per level): `rowId` for rows, `encodedKey` for unique indexes,
`encodedKey + rowId` for non-unique.

**De-duplication uses one last-suffix string and no seen-set.** The heap priority is the pair
`(suffix, levelIndex)`. All entries that share a suffix therefore dequeue consecutively, and the
nearest level dequeues first. An entry whose suffix equals the previous one is suppressed
immediately (`lastHex` in `KvRowAccessor`, `lastSuffix` in `KvIndexAccessor`). Scan state is
O(lineage depth) and does not grow with the number of distinct keys scanned. The merge is also
bounded for `LIMIT 1` against a huge parent: levels are merged lazily and are not materialized.

**Uniqueness and primary keys** are checked over the **union** of level 0 and the reachable
ancestry, tombstone-aware (`ResolveBranchUniqueFlagsAsync`): a key present only in an ancestor
also conflicts on the branch; a key tombstoned in the branch can be re-inserted (tombstone
replace); ancestor uniqueness is unaffected by branch state.

Serializable read-write transactions acquire the shared point/range locks in the **level-0**
keyspace only; ancestor levels are frozen and need no locks.

**Out-of-line values follow the row.** A row may point at a value stored under a separate
`|v` key (see [Large values](storage-layout.md)). The value is resolved by the same rule as the row:
a batched probe at level 0, then one batched probe per ancestry level at that level's fork timestamp
for the keys not yet found. A tombstone ends the walk. An inherited row therefore reads its
inherited values. On the branch, a write stores new values at level 0. An update of a small column
carries the inherited pointer and writes no value. A delete or a shrink writes a tombstone to the
value key, for the same reason a row delete does. `PurgeLocalRowOverlayAsync` removes the
branch's `|v` overlay together with its `|r` overlay. An `ALTER TABLE ... REWRITE STORAGE` on the
parent writes new versions only. A branch forked before it therefore reads the old versions at its
fork timestamp.

---

## 5. DDL and lifecycle isolation

- **Parent DDL after a fork is invisible to the branch** — the branch owns an independent schema
  checkpoint copied at `forkT` (§7).
- **A parent's in-flight materialized-view refresh is invisible to the branch.** The branch gets
  the view's published contents as of `forkT` and no refresh job; the parent's rebuild finishes
  or fails on the parent alone (§7, step 6).
- **Foreign keys are inherited by id.** The copy preserves table, column and index ids, and a
  constraint stores only ids. This means that a branch enforces all constraints of its source
  unchanged. The checks read the branch's keys merged with the frozen ancestry. A tombstone on
  the branch hides an inherited row or index entry, and a write in the source after the fork does
  not change a check on the branch. A child writer and a parent writer on a branch meet on the
  branch's copy of the parent's unique-index key. The lock rendezvous therefore needs no change.
  A reference can name a table in the same database only; a branch cannot reference its source. See
  [foreign-keys.md](foreign-keys.md) §8.
- **Branch DDL is invisible to the parent and siblings** — it writes only the branch dbId's
  metadata/log. Branch `CREATE TABLE`/`ADD INDEX` allocate new ids in the branch; renames stay
  metadata-only and branch-local.
- **Branch `DROP TABLE`/`DROP INDEX` do not scan or tombstone inherited data.** Once the branch
  schema no longer references an object, its inherited data is unreachable. Only the
  branch-local overlay for that object is physically purged (`PurgeLocalRowOverlayAsync` /
  `DropIndexEntries`). Branch drops therefore cost O(branch overlay) and do not depend on the
  size of the parent's data.

---

## 6. Frozen-view durability — the snapshot floor

Reading an ancestor at `forkT` is correct only while the parent's revision current at `forkT`
is retained. Kahuna bounds its in-memory revision archive (default 16 per key) and can prune
on-disk history. A long-lived branch would otherwise see its frozen view silently reclaimed
under parent churn.

CamusDB solves this with the upstream **Kahuna snapshot floor** (Kahuna ≥ 0.6.0). The client
API is on `IKahuna`, and all of its operations are auto-routed to the system-partition leader
and Raft-replicated.

| Op | Purpose |
|----|---------|
| `LocateAndAcquireSnapshotHold(holderId, timestamp, leaseMs)` | Pin all revisions at/after `timestamp`; idempotent by `(holderId, timestamp)`. |
| `LocateAndRenewSnapshotHold(holdId, leaseMs)` | Extend the lease. |
| `LocateAndReleaseSnapshotHold(holdId)` | Release; the floor rises when the lowest hold is released. |
| `GetSnapshotFloor()` | Introspection: current floor + live-hold count. |

CamusDB uses the snapshot floor in four ways.

- **One hold per branch, on its immediate parent.** At fork, the branch acquires a hold on the
  parent at `forkT`, with `holderId = branchId`. The hold id is persisted on the registry entry
  (`ImmediateParentHoldId`). Deeper ancestor levels are *not* held directly. They remain pinned
  because a database with live descendants cannot be dropped (§8). Each intermediate branch
  therefore maintains the hold on its parent.
- **Block if the hold is not granted.** If the acquire does not return `Set`, branch creation
  **fails**. CamusDB does not register a branch whose frozen view it cannot guarantee.
- **Release on leaf drop.** Dropping a branch releases its parent hold so that history can be
  reclaimed.
- **Leader-owned renewal, tied to registration.** A hold must live as long as the
  branch is *registered*, even if no client has it open. `SnapshotHoldRenewer` is active on
  exactly one node: the leader of the database-registry partition
  (`EmbeddedKahuna.AmILeaderForKeyAsync`). At each `lease/3` interval, it renews the holds of all
  registered branches. It reads the branch set from a **persistent registry scan**
  (`ScanAllEntriesAsync`) and does not use the local cache. This means that a branch created on
  another node after this leader started is not missed. The election is re-checked each tick,
  and failover hands the sweep to the new leader.

A **live hold pins the fork-boundary revision in memory regardless of storage backend**, and
the *same* hold protects both the ancestor row/index reads and the metadata copy at `forkT`.

**Fail-closed on a lost hold, fail-recovered on a mere lapse.** Since Kahuna 1.7.3, a hold's
protection ends when the hold is **removed from the replicated registry**, and in no other case.
A hold is removed by an explicit release, or by the reaper's purge of a hold whose lease lapsed
and was not renewed. A bare lapse is recoverable. A registered hold continues to constrain
reclamation while its lease is expired, and the next renew revives it. In particular, full
downtime longer than the lease heals itself. Holds loaded from the durable registry are exempt
from the purge for a startup grace window (`SnapshotHoldStartupGraceWindow`, default 5 min), and
the renewer's first sweep starts immediately at startup. The revival therefore happens long
before that window closes. Once a hold *is* removed, Kahuna answers `DoesNotExist` to all later
renews, and re-acquiring at the old timestamp does not bring reclaimed history back. If the read
were not fenced, an ancestor read past that point would return reclaimed rows as confirmed
absences: a successful, silently incomplete (possibly empty) result. Three layers turn that into
a hard error.

- **Read-side guard (`BranchSnapshotHoldGuard`).** Each branch descriptor carries one guard,
  shared by all its ancestor-level stores. After each ancestor probe/scan reads from Kahuna, and
  before its result (a value or tombstone, *or an absence*) is used, the guard verifies that all
  holds in the branch's chain are protected. The chain is the branch's hold plus the hold of each
  non-root ancestor. The proof: Kahuna only renews a *registered* hold (renewing revives a
  lapsed-but-registered one). One confirmed renew therefore proves that the chain's protection did
  not lapse before that instant. The guard caches the confirmation and re-verifies once it is older
  than half the lease. This means that the fast path is one tick comparison per ancestor access. A
  definitive refusal (the hold was removed) latches the branch permanently and throws
  `BranchSnapshotProtectionLost` (HTTP 410). A transient verification failure past 0.8 × lease
  throws a retryable error and does not guess. This covers descendants (a grandchild verifies the
  middle branch's hold too), as well as reopen, failover and renewer starvation. The guard is the
  safety mechanism; the sweep is an optimization for unopened branches.
- **Durable lost marker.** When the renewer sweep (or a guard) gets a definitive refusal, it
  writes `dbregistry/holdlost:{branchId}`. A later open pre-latches the guard from the marker, and
  the first read then fails fast with the recorded reason. Correctness does not depend on the
  marker: the refused renew is visible on all nodes. Dropping the branch clears the marker.
- **Creation keep-alive.** During branch creation, no other component renews the fresh hold: the
  sweep only sees *registered* branches. The metadata copy is also not bounded in duration.
  Creation starts a private keep-alive (`SnapshotHoldLease.AdoptForKeepAlive`) for the duration of
  the create. It validates the keep-alive before `RegisterAsync` and again before returning
  success. This means that "published" implies "protected". `branch_snapshot_hold_lease_ms` must
  be ≥ 5000 for the same reason. The renewer does not tick faster than once a second, and a lease
  shorter than that tick would lapse by design.

---

## 7. Branch creation, step by step

`DatabaseLifecycleService.CreateBranchDatabaseAsync` performs eight steps under the **source's**
`SchemaDdlSemaphore`.

1. **Existence check via the persistent registry** (`TryResolveEntryAsync`, which bypasses the
   local cache). `IF NOT EXISTS` opens and returns an already-existing target; otherwise the
   create fails early. Both happen *before* any id allocation, hold acquisition or metadata copy.
2. **Schema stability.** The source must be schema-stable: `HeadSchemaVersion == SchemaVersion`,
   all elements `Public`, no in-flight coordinator jobs. Otherwise the copy could capture a
   half-applied online schema change. The elements are columns, indexes and foreign keys. A fork
   is therefore not allowed while a foreign key is being added. Between `WriteOnly` and `Public`,
   the constraint is enforced, but its existing rows are not validated yet. The copy leaves out
   the coordinator job that validates it. A branch that inherited the constraint would then have a
   constraint that no job will validate. The state check applies even when no job is left. A fork
   that arrives while `ALTER TABLE ... ADD CONSTRAINT` is in progress on the same node waits for
   it. The fork enters the source's DDL gate exclusively. A staged change that locks its table
   while its rows are read (distributed-schema-architecture.md §4.1) therefore finishes first, and
   the fork then copies the constraint in `Public`.
3. **Mint `forkT`** (begin+rollback a source transaction — the causal fence) and **allocate
   `branchId`** from the monotonic id sequence.
4. **Acquire the snapshot hold** on the source at `forkT`; block if not `Set`.
5. **Write the pending-create marker** (`TrackPendingBranchAsync`) — a *confirmed-durable* write
   (it retries `MustRetry`/`WaitingForReplication` and throws on any terminal non-`Set`). It is
   placed *before* the metadata copy, in the try block. If it fails, the hold is released and the
   copy is skipped.
6. **Copy schema metadata as of `forkT`** (`CopyMetaForBranchAsync`). This is an as-of-`forkT`
   scan, which excludes a remote DDL committed after `forkT`. The branch schema is then
   consistent with the ancestor data it inherits. All copied keys are written in one transaction.
   Only the source's **published** schema is copied. Keys that record work the source has in
   flight are left out: coordinator jobs and the keyspace catalog, and a materialized-view
   refresh's job record (`mvrefresh:`) together with the staging relation it is building into
   (its `table:` and `history:` keys). A refresh in flight therefore does not block a fork. The
   branch's materialized view uses whichever storage the view's record named at `forkT`, and it
   inherits no rebuild. That storage is the pre-refresh contents, read through ancestry, or the
   rebuilt contents if the swap already committed. If the job were copied, the branch's
   abandoned-refresh sweep, fenced under the branch's id, could "take over" a run that the branch
   did not start and rewrite the view from post-fork branch writes. As a second line of defense,
   the job record carries the id of the database it was written in. A takeover that finds a
   record naming another database removes it and does not restart the rebuild.
7. **Publish the registry entry** (`RegisterAsync`), then **check the drop-intent fence and
   re-read the source's liveness by its immutable id** (§8).
8. **Clear the pending marker** on success. On abort, the create **proves that the registration
   is not live**, and then releases the hold and inline-purges any copied metadata. Only a purge
   that **verifiably** drained the namespace clears the marker: each delete had a terminal outcome,
   and a confirming scan found no keys. An incomplete purge leaves the marker in place. The startup
   scrubber then resumes the purge on the next restart.

The invariant that follows from this ordering: **each `{branchId}/meta/…` namespace is either
registered, or has a durable pending marker** that the startup scrubber will find. At all times,
metadata that exists has a recovery handle.

Publication and abort obey two further rules. A failed `RegisterAsync` does not prove that the
registration is absent.

- **A committed registration cannot be reported as a failure.** `RegisterAsync` returns once its
  transaction commits. The post-commit work (cache update, cross-node coherence stamp) is
  best-effort and does not throw. Before this rule held, a transport fault on the coherence stamp
  made the create's abort path destroy a branch whose registry entry had already durably
  committed. The path purged the metadata and released the hold, and it also cleared the marker.
  This left a live name whose metadata and hold were gone.
- **Destructive abort cleanup requires proof of unpublication.** The abort path calls
  `RetractRegistrationAsync`. It reads the durable entry under its exclusive lock. It deletes the
  entry if the entry carries this create's id, and leaves it in place otherwise. A replacement
  registered concurrently under the same name is therefore not removed. An absent entry is proof
  if the registration failure had a resolved outcome. When the commit outcome is unknown
  (`TransactionFinalizeUnresolved`), an absent entry is not proof: the entry may become durable
  later. In that case, the create does not release the hold, purge the metadata or clear the
  pending marker. It returns a retryable indeterminate error and does not destroy state it cannot
  prove unpublished.

---

## 8. Drop, descendants, and cross-node fences

`DROP DATABASE` is a multi-step saga. It does not run as a single transaction, and its
correctness rests on several layered fences.

- **Descendant block (`CADB0508`).** A database with any registered descendant cannot be
  dropped. `HasLiveDescendantsAsync` checks the in-memory cache *then* does a persistent registry
  scan (to see branches registered on other nodes). Drop leaf-first: leaf → parent → root.
  The scan reads with the read-only identity (transaction id zero) and does not use a read-write
  transaction. A read-write scan pins an OCC snapshot on each registry key it reads, and a
  concurrent `CREATE` or `DROP DATABASE` then aborts it. The latest committed state is enough for
  the fence, since Kahuna adds committed but unsettled writes to each latest scan page. A
  transient scan failure restarts the scan up to three times, and then fails the drop with the
  retryable `CADB0504`.
- **Same-node fence (semaphore).** `DropDatabase` opens the target and acquires its
  `SchemaDdlSemaphore` for the duration of the descendant re-check and `UnregisterAsync`. It
  enters the semaphore exclusively. A staged schema change in progress on any table of the
  database (an index build, a column fill) then finishes first. Branch-create acquires the
  *source's* semaphore for the duration of its publish. Both use the same id-keyed descriptor,
  which means that they mutually exclude on one node.
- **Cross-node fence (drop-intent lease).** Before its descendant scan, `DropDatabase` takes a
  Raft-replicated `drop-intent:{dbId}` (`SetIfNotExists`) and does not release it until the
  keyspace purge ends. Branch-create checks it *after* `RegisterAsync`. Raft linearizability
  guarantees that exactly one side wins: either branch-create sees the intent and retracts, or
  drop's descendant scan sees the new child and aborts. The intent is released on all exit paths.

  The intent is a **lease** (`KeyLeaseFence`). Its key carries a native expiry of
  `CamusDBOptions.FenceLeaseMs` (default 30 s). The holder re-stamps it at each
  `FenceLeaseRenewIntervalMs` interval for as long as it owns the fence. This means that a long
  purge is not interrupted, and a holder that dies stops blocking the id once the lease lapses.
  Each acquisition carries a fencing token. Release is a compare-and-set of that token onto a
  near-immediate expiry, and the key is not deleted. A stalled predecessor therefore cannot free a
  fence that now belongs to someone else.
- **Source liveness re-read (completed-drop fence).** The intent key only covers a drop that is in
  flight. A create that stalls between its metadata copy and `RegisterAsync` can resume after the
  drop has finished and released the intent. Directly after the intent check, branch-create
  therefore re-reads the source's liveness from the persistent registry by its **immutable id**
  (`TryResolveNameByIdAsync`, which bypasses the local cache). It aborts when the id is gone. The
  order of the two reads closes the window. A drop whose scan missed the child owns the intent from
  before the child's `RegisterAsync` until after it unregisters the id. The create therefore either
  sees the intent, or its later liveness read sees the unregistered id. Ids are not reused. A
  recreate of the name under a new id also aborts the create, while a rename (same id) does not.
- **Meta-last, resumable, paged purge.** This is the immediate-purge path, which all branch drops
  take. When a root is dropped and `FORCE` is not given, the root is retained as a recoverable
  orphan for `RELINK`, and the orphan GC purges its keyspace later. Branch recovery would have to
  pin the parent's snapshot floor for the duration of the retention window. Deferred drop is
  therefore out of scope for branches. The purge starts by reading the catalog. It deletes
  row/index/stats keys next and deletes **meta (catalog included) last**. This means that a crashed
  purge can be resumed from the catalog, which has not been deleted yet. A `dropping:{dbId}` marker is
  written before `UnregisterAsync` and is not cleared until the purge completes. Startup resumes any
  interrupted purge. Deletes are **paged** in bounded batches
  (`CamusDBOptions.KeyspacePurgeBatchSize`). A large database therefore drops in bounded memory.
  Each batch is deleted with **one delete-many call**, which costs one scan plus one Raft round,
  compared with one round per key.
- **Drop cost.** A branch with zero private rows and one branch-built index over 20,000 rows stores
  20,000 overlay keys and drops in well under a second on a standalone node (40 batches at the
  default batch size). Before the batched delete, the purge awaited one autocommit delete per key
  (one Raft round and one WAL fsync each, about 5 ms on a RocksDB node), and the same drop took
  about 100 s. The drop is inline: `DROP DATABASE` returns only after the purge completes.

---

## 9. Crash recovery

Each crash-recovery handle in this feature is a **persistent, owner-tagged marker** plus a
**startup scrubber** (`StartupRecoveryService.ScrubOrphanBranchNamespacesAsync`, fire-and-forget
at startup; `CommandExecutor` only delegates to it).

**Owner-tagged** means the marker's value is `{nodeId}:{epoch}`. The node id is the stable Raft id
of the node that wrote the marker. The epoch is a token minted once per process start. The lease
fence appends `:{acquisitionToken}` to the markers it owns. Together, the node id and the epoch
separate three cases that a bare marker cannot distinguish.

- **This node, this epoch** — a live operation in this process. Recovery does not touch it.
- **This node, another epoch** — a remnant of a run that died. Recovery may reclaim it.
- **Another node** — that node's marker, live or not. Recovery does not touch it.

One case is deliberately outside that rule. A pending-create marker written before owner stamping
contains a single `0x01` byte and names no owner. Recovery cannot attribute it and treats it as
reclaimable.

**A marker alone is not proof of abandonment.** The marker cleanup after a successful create is
best-effort. A live, published branch can therefore carry its pending marker. Before the scrubber
destroys anything, it takes three steps.

1. It re-reads registration from the **persistent** registry. The local cache can predate the
   branch, which means that an absent cache entry is not evidence.
2. It takes the id's drop-intent fence, which serializes the scrub against `RELINK` and the
   orphan GC.
3. It re-reads registration under that fence.

A registered id loses only its obsolete marker. An id whose data an orphan record retains for
`RELINK` also loses only its marker. An unresolved read skips the id. This means that
uncertainty does not lead to data being destroyed.

| Marker | Written when | Cleared when | On crash, startup does |
|--------|-------------|--------------|------------------------|
| `pending:{branchId}` | before copying branch metadata | after publish, or after a *verified* purge on abort | for own prior-run markers and unstamped legacy markers only: re-check registration against the *persistent* registry, take the id's fence, re-check again, then scrub `{branchId}/meta` with verified, paged deletes; the marker is cleared only once a confirming scan finds the namespace empty, otherwise it is left for the next startup; a registered id only loses its obsolete marker |
| `drop-intent:{dbId}` | before a drop's descendant scan | after purge / on abort | clear own stale intents (a drop does not span a restart). The lease also frees a dead owner's intent by itself. The sweep therefore only shortens the wait; there is no deadlock for it to resolve |
| `dropping:{dbId}` | before a drop's `UnregisterAsync` | after the purge completes | under the id's fence, and only for an own, prior-run marker: re-check registration against the *persistent* registry, then resume the keyspace purge; the marker is cleared only after the purge verifiably completes |

Two ordering choices make this robust. The pending marker is written *before* the metadata copy,
which means that metadata always has a handle. The drop marker is written *before*
`UnregisterAsync`. An interrupted purge is therefore always resumable, and the meta-last purge
order leaves the catalog available for the resume.

---

## 10. Operating branches

**A snapshot hold pins parent history.** A live branch prevents reclamation of its parent's
fork-time revisions. Long-lived branches over a hot parent therefore hold back reclamation of that
parent's old revisions. This is expected and correct, but you should watch capacity. Kahuna
exposes the gauges below under the `Kahuna` meter scope, and you should add them to your
dashboards.

- `kahuna.snapshot_floor.live_holds` — one per live branch (roughly). Rising unboundedly means
  branches are being created but not dropped.
- `kahuna.snapshot_floor.effective_floor_ms` — how far back the oldest hold pins history.
- `kahuna.snapshot_floor.missing_protected_version_total` — **must stay 0.** A non-zero value
  means reclamation touched a protected version — a durability fault; alert on it.

**Lease / renewal.** `CamusDBOptions.BranchSnapshotHoldLeaseMs` (default `300_000` = 5 min) sets
the hold lease; the leader-owned renewer refreshes holds at each `lease/3` interval. Choose it
coarse enough that renewals are not a hot Raft path. If the renewer's node is unhealthy, failover
moves the sweep to the new registry-partition leader. A branch does not lose its hold the moment a
lease lapses. A registered hold continues to constrain reclamation, and the next renew revives it.
The loss happens when Kahuna's reaper purges the lapsed hold. The lease therefore sets how long a
stalled renewer has to recover (§6).

**Drop order matters.** You cannot drop a database that has live descendant branches
(`CADB0508`). Drop descendants first, leaf to root. A dropped id is never reused.

**Deep chains cost reads, and are observable.** Read cost is proportional to lineage depth (one
ancestor probe per level on a point-read miss, one extra scan iterator per level). There is no
depth cap, but the read path is instrumented via always-on process-wide counters in
`BranchMetrics`. These counters are **not** published through the `ServerDiagnostics` meter, and
the Prometheus endpoint does not scrape them. Read them in process; attaching them to that meter is
the hook point.

- `AncestorProbesTotal` — ancestor-level probes fired by `GetRow`/`LookupUnique` misses. A high
  rate relative to query volume means deep chains are amplifying point reads.
- `ScanIteratorsTotal` — ancestor scan iterators opened by `ScanRows`/`ScanIndex` (increments by
  the lineage depth per branch scan).
- `DeepLineageWarnings` — how many table stores were opened at a lineage depth ≥
  `BranchMetrics.LineageWarningThreshold`. Each such open also logs a warning naming the table and
  depth. A rising count is the signal to consider compacting or rebasing the chain.

Roots (no ancestry) do not touch these paths. Root databases therefore add no counter overhead.

**Config knobs.**

| Knob | Default | Effect |
|------|---------|--------|
| `BranchSnapshotHoldLeaseMs` | `300_000` | Snapshot-hold lease; the renewer refreshes holds at `lease/3`. |
| `KeyspacePurgeBatchSize` | `512` | Max keys per scan batch in the `DROP DATABASE` purge. Each batch is one delete-many round trip. This bounds memory and sets how many keys share one Raft round. |
| `BranchMetrics.LineageWarningThreshold` | `10` | Lineage depth at/above which opening a table store logs a warning and increments `DeepLineageWarnings`. Advisory; it is not a hard cap. |

---

## 11. Limitations and future work

- **No branch compaction or rebase.** Unbounded branch depth is intentional, and deep chains are
  now observable (the `BranchMetrics` counters and the depth-threshold warning in §10). However,
  there is no mechanism yet to *shorten* a deep chain. A branch cannot be compacted into a
  standalone database or rebased onto a different ancestor. The metrics tell an operator when a
  chain is getting expensive. The remedy is currently manual: recreate the branch as a root.
- **Large branch `DROP TABLE`/`DROP INDEX`.** These delete in one DDL transaction and are
  therefore capped by the per-transaction mutation limit. Arbitrarily large single-object drops
  on a branch would need decoupling from the DDL transaction. (`DROP DATABASE` is paged and deletes
  each batch in one round trip. It is not subject to this cap.)
- **Cross-node cluster tests.** Several fences are covered by single-node simulations of the
  cross-node scenario (two registry instances, injected markers). They are not covered by true
  multi-node tests. The mechanisms are topology-independent, but network/replication fidelity is
  not separately exercised.

## Non-goals

Branching rides the existing Kahuna routing model and leaves several features out of scope.

- Merge-back into the parent.
- Cross-branch transactions. A transaction touches exactly one database.
- Branch compaction or rebase to shorten deep chains.
- Reparenting descendants on drop.
- Physical data-distribution changes.

---

## Inspecting the branch tree from SQL

Two read-only server-level statements let you explore the branch hierarchy, and they do not need
a database context. Both resolve names through the persistent registry. A database created on
another node is therefore visible.

### `SHOW BRANCHES FROM <database>`

Returns all transitive descendants of `<database>` — direct children at depth 1, their children
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
| Create / drop / rename | `DatabaseLifecycleService` (`CreateBranchDatabaseAsync`, `DropDatabase`, `RelinkDatabase`), `DatabaseDropper` (keyspace and meta purges). `CommandExecutor` is a facade that delegates to them. |
| Metadata copy | `BranchMetaCopier.CopyMetaForBranchAsync`, reached through `CatalogsManager.CopyMetaForBranchAsync` |
| Durability | `SnapshotHoldRenewer`, `EmbeddedKahuna.AmILeaderForKeyAsync`, `IKahuna` snapshot-floor API |
| Recovery | `StartupRecoveryService.ScrubOrphanBranchNamespacesAsync`, `RegistryPendingBranches` and `RegistryDropMarkers` (`DatabaseRegistry.PendingBranches` / `.DropMarkers`) |
| Branch tree queries | `SchemaQuerier.ShowBranches`, `SchemaQuerier.ShowAncestors` |
