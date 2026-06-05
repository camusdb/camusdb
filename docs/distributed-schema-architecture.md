# Distributed Schema — Architecture & Developer Guide

> **Audience:** engineers maintaining or extending CamusDB's catalog/DDL layer.
> **Scope:** how schema (tables, columns, indexes) is changed, replicated, persisted,
> versioned, and made visible across a cluster.

> **Overview.** The cluster DDL path is end-to-end: production follower→leader forwarding
> over HTTP with idempotent dedup, a resumable staged online-schema **coordinator** that
> drives `AddColumn`/`AddIndex` through `DeleteOnly → WriteOnly → Public` with an ack gate
> between steps and crash-resumable backfill, index DDL replicated through the schema log
> with indexes owned by `TableSchema`, and an ack-gate membership set sourced from live Raft
> membership. Known limitations and future-work items are collected in §13.

---

## 1. Mental model

CamusDB embeds **Kahuna** (a transactional KV store), and Kahuna embeds **Kommander**
(a Raft implementation). The schema of a database is treated as a **replicated state
machine**:

- **Source of truth = the committed Kommander log.** Each DDL operation is a
  `SchemaChangeLogEntry` (a *delta*) replicated through Raft on a dedicated partition.
  Every node applies the committed deltas **in order**, so every node converges on the
  same schema.
- **The persisted KV blobs are a checkpoint**, not the truth. They let a node rebuild
  its in-memory schema on open without replaying the entire log. They are written *as a
  side effect* of applying a delta.
- **Schema is versioned with a monotonic counter** (`Schema.SchemaVersion` per database,
  `TableSchema.Version` per table). Versions are how readers, writers, and the replication
  layer reason about "which schema" a piece of data or a transaction belongs to.

Two big consequences flow from this model and are worth internalizing before reading code:

1. **DDL is `FromVersion -> ToVersion`, never "set state X".** An entry is only valid if
   the node is currently at `FromVersion`. This makes apply idempotent and ordered.
2. **Renames and online column/index changes are *metadata-only*** because row bytes are
   keyed and serialized *positionally by immutable IDs*, not by names (see §7). Renaming a
   column never rewrites a single row.

### 1.1 Key concepts & vocabulary

If you are new to this subsystem, these five ideas are the whole story; the rest of the
document is detail.

**Consensus.** A cluster has many nodes that can fail, restart, or be slow, yet they must all
agree on *what the schema is and in what order it changed*. Kommander (Raft) provides this: a
change is **proposed** to a leader, replicated to followers, and **committed** only once a
**majority (quorum)** of nodes have durably stored it. A committed entry can never be lost or
reordered. We put every schema change for a database into one ordered Raft log (one partition,
§5.1), so "the committed log" *is* the authoritative, agreed-upon history of the schema. 

**Replicated state machine.** Each node starts from the same empty schema and applies the same
committed deltas in the same order, so each node deterministically arrives at the same schema.
The schema is never "copied" between nodes — it is *recomputed* on each node by replaying the
agreed log. (The persisted KV blob is only a cached snapshot so a restarting node doesn't have
to replay from the beginning.)

**Acknowledgement (ack).** Commit in Raft means "a majority has stored the delta", **not**
"every node has *applied* it to its in-memory schema yet". An **ack** is a node reporting *"I
have applied schema version N."* We track acks because two things depend on knowing every node
has caught up, not just a majority:

- *Safety of the next step.* We never want three different schema versions live at once (a
  reader could be on N-1 while a writer is already on N+1 — an unbounded spread is impossible to
  reason about). So before proposing the move to version N+1, we **wait until every live node
  has acked N** (the two-version invariant, §6.2). Acks are the gate that enforces this.
- *Honest completion.* When a `CREATE TABLE`/`ALTER` call returns to the client, we want it to
  mean "this change is in effect everywhere", not "a majority will get it eventually". Waiting
  for all live acks before returning gives that guarantee.

**Convergence.** The cluster has *converged* on version N when every live node has applied N and
acked it — i.e. all nodes show the same schema. Because of the ack gate, CamusDB's DDL is
*synchronously* convergent at each step: a step does not start until the previous one has
converged. (Tests assert this directly with `WaitForSchemaConvergenceAsync`.) A node that was
offline during some changes converges on rejoin by replaying the missed committed log entries.

**Coordinator.** A safe online schema change is not one delta — it is a *sequence* of small
deltas (e.g. add a column as invisible, then make it writable, backfill existing rows, then make
it readable), with a convergence gate between each. Something has to **own** that multi-step
sequence: emit the next delta only after the previous one has converged, run the backfill at the
right moment, and — crucially — **survive a leader change** so a half-finished change is carried
to completion rather than left stuck. That owner is the `SchemaChangeCoordinator` (§8.2). It runs
on the schema leader, records its progress durably, and resumes on whichever node becomes leader
next.

Putting it together: **consensus** agrees on each individual delta; **acks** tell us when a delta
has **converged** to every node; the **coordinator** chains convergent deltas into a complete,
crash-safe online schema change.

---

## 2. Component map

| Layer | Type | Responsibility |
|---|---|---|
| SQL / executor | `CommandExecutor` | Entry point for DDL & DML. Owns the DDL transaction, schema-version *pinning*, follower→leader *forwarding*, and the cluster add-column/add-index entry points that drive the coordinator. |
| Online-schema driver | `SchemaChangeCoordinator` | Drives a column or index element through the staged `SetElementState` sequence one adjacent transition at a time, gating each step on the cluster ack, running backfill before `Public`, and persisting its job for leader-change resume. |
| Catalog | `CatalogsManager` | Builds deltas, validates them, applies them (`ApplySchemaDelta`), persists per-object metadata, loads metadata on open. Exposes the `Replicate*` primitives the coordinator composes (`ReplicateAddColumnInStateAsync`, `ReplicateAddIndexInStateAsync`, `ReplicateElementStateAsync`, `ReplicateDropIndexAsync`). |
| Replication glue | `SchemaReplicator` | Bridges Kahuna's apply/restore callbacks to `CatalogsManager`. Applies committed deltas in-memory (never persists from the callback), records acks, evicts cached `TableDescriptor`s, and registers the coordinator-resume leader callback. |
| KV / consensus | `EmbeddedKahuna` | Routes schema deltas to a Raft partition, replicates+commits them, fans them out to local subscribers, tracks per-node acks, sources live membership from Raft, and fires `OnLeaderChanged` for coordinator resume. |
| Liveness | `SchemaAckTracker` | Per-database, per-node `{version, lastSeen}` map; powers the two-version invariant gate. The live set comes from Raft membership; an optional finite lease expires silent members. |
| Forwarding | `ISchemaDdlForwarder` / `HttpSchemaDdlForwarder`, `SchemaDdlForwardController`, `DdlOperationIdCache` | Ship a DDL *ticket* from a follower to the schema leader over HTTP, re-execute it as leader, and dedup retries by a stable operation id. |
| Models | `SchemaChangeLogEntry`, `SchemaOp`, `SchemaElementState`, `SchemaElementKind`, `TableSchema`, `TableColumnSchema`, `TableIndexSchema`, `PersistedCoordinatorJob`, `DatabaseIndexObject` | The serialized delta, the operation kinds, the online-state enum, the column/index discriminator, the in-memory/persisted shapes, and the durable coordinator job. |
| Storage | `RowEncoder`, `KvTableStore` | Positional row encode/decode with element-state visibility; row/index scans; idempotent index backfill writes (`PutIndexEntry(backfillMode:)`). |
| Transactions | `KvTransaction`, `KvTransactionsManager` | Carry schema-version *pins* and validate them at commit; lock/modified tracking is lock-guarded for concurrent use. |
| Test harness | `InProcessSchemaCluster`, `FaultInjectingCommunication` | N distinct in-process nodes with real Raft, ack-based convergence await, and pause/kill/force-leader fault injection. |

### Single-node vs cluster: the `OwnsKahuna` switch

Every catalog mutation has two code paths, selected by `database.OwnsKahuna`:

- **`OwnsKahuna == true`** (embedded/single-process owner of the Kahuna instance): the
  *local* path. `CatalogsManager.CreateTable/AlterTable/DropTableSchema` apply the delta
  directly under `Schema.Semaphore` and persist, all inside the caller's DDL transaction.
  No Raft round-trip.
- **`OwnsKahuna == false`** (cluster member): the *replicated* path
  (`*ReplicatedAsync`). The delta is validated locally, then proposed through Raft, and
  the in-memory mutation happens later, in the **apply callback**, on every node.

When reading the code, always note which path you are in — they look similar but the
replicated path deliberately does **not** mutate `database.Schema` inline; it only
validates and proposes.

---

## 3. The schema-change delta (`SchemaChangeLogEntry`)

```csharp
class SchemaChangeLogEntry {
    HLCTimestamp Ts;       // hybrid logical clock stamp (from the DDL transaction)
    string Database;       // which database this delta belongs to
    long FromVersion;      // schema version this delta expects to apply onto
    long ToVersion;        // == FromVersion + 1
    SchemaOp Op;           // CreateTable | DropTable | AddColumn | DropColumn |
                           // AddIndex | DropIndex | SetElementState
    byte[] Payload;        // op-specific, serialized (e.g. SchemaCreateTablePayload)
}
```

Key properties:

- **Deterministic IDs are baked into the entry at creation time** (`CreateTableEntry`,
  `AlterTableEntry`). The leader generates the table/column ID *once*, puts it in the
  payload, and every node uses that same ID when applying. IDs must never be regenerated
  during apply, or nodes would diverge.
- `FromVersion`/`ToVersion` are always adjacent (`+1`). The log is a strict chain.
- The entry is the unit of replication and the unit of idempotency.

`SchemaOp` includes `AddIndex`/`DropIndex` and `SetElementState` (the online-state advance
used for both columns and indexes). Index DDL **is** routed through this log; the replicated
source of truth for indexes is `TableSchema.Indexes`, persisted per-object alongside columns.
A cluster `ADD INDEX` is driven by the coordinator as a staged
`AddIndex(DeleteOnly) → SetElementState(WriteOnly) → [backfill] → SetElementState(Public)`
sequence — see §7.3 and §8.2.

`SetElementState` carries a `SchemaElementKind { Column, Index }` discriminator
(`SchemaElementStatePayload.ElementKind`, default `Column` so legacy entries deserialize
correctly). The same delta type therefore advances either kind; the apply path branches on
the discriminator (`ApplyElementState` vs `ApplyIndexElementState`).

---

## 4. The write path (proposing a DDL change)

This is the cluster path (`OwnsKahuna == false`). Entry point:
`CommandExecutor.AlterTable / CreateTable / DropTable / AlterIndex`.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ CommandExecutor.AlterTable(ticket)                                        │
│                                                                           │
│  1. TryForwardAlterTableAsync(database, ticket)                           │
│       └─ if this node is NOT the schema leader → forward to leader        │
│          over the production forwarder, await the forwarded apply, return │
│                                                                           │
│  2. ExecuteDdlInTransaction(database, tx => ...Alter..., onAbort: ...)    │
│       ├─ acquire database.SchemaDdlSemaphore (serialize DDL on this node) │
│       ├─ begin KvTransaction                                              │
│       ├─ run the action (→ CatalogsManager.AlterTableReplicatedAsync)     │
│       ├─ commit; on exception: rollback + run onAbort compensation        │
│       └─ release SchemaDdlSemaphore                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

Inside `CatalogsManager.AlterTableReplicatedAsync`:

```
 a. Under Schema.Semaphore:
      entry = AlterTableEntry(...)         // assigns FromVersion = current, ToVersion = +1
      ValidateSchemaDelta(schema, entry)   // dry-run apply on a CLONE; throws if invalid
 b. ReplicateAndWaitLocalApplyAsync(database, entry)
 c. return GetTableSchema(...)             // read back the now-applied schema
```

`ReplicateAndWaitLocalApplyAsync` is the heart of the protocol:

```
 1. WaitForPreviousVersionAcksAsync(entry.FromVersion)      ← TWO-VERSION GATE (§6)
        every live node must have applied FromVersion before we propose

 2. ReplicateSchemaChangeAsync(db, bytes)                   ← Raft round-trip (§5)
        propose → commit → fan out to local apply subscribers
        throws if outcome != Committed

 3. spin (≤5s) until local apply observed:
        database.Schema.SchemaVersion >= entry.ToVersion
        && WasSchemaDeltaApplied(schema, entry)

 4. WaitForSchemaAcksAsync(entry.ToVersion)                 ← wait for all live nodes
        to ack the NEW version before returning to the client
```

So when an `ALTER` returns successfully to the client, the change is (a) committed in Raft,
(b) applied locally, and (c) acknowledged-applied by every live node.

---

## 5. Replication transport (`EmbeddedKahuna`)

### 5.1 Partition routing

All schema deltas for one database go to a **single Raft partition**, computed once:

```csharp
SchemaLogPartition(db) => Raft.GetPrefixPartitionKey($"{db}/meta")
```

- `GetPrefixPartitionKey` hashes the **whole string** (unlike `GetPartitionKey`, which
  hashes the prefix before the last `/`). Using the prefix variant here is deliberate so
  that *all* `{db}/meta/...` schema traffic lands on the same partition and is therefore
  **totally ordered** for that database.
- **Partition 0 is reserved.** `SchemaLogPartition` throws if the hash resolves to 0, to
  avoid colliding with Kahuna's reserved partition.

> ⚠️ This is the one routing subtlety that has bitten us before. Schema *metadata keys*
> are per-object (`{db}/meta/table/{tableId}`) for storage scalability, but the *schema
> log* must be single-partition for ordering. Don't "optimize" the log to spread across
> partitions — ordering is the whole point.

### 5.2 Propose → commit → apply

`ReplicateSchemaChangeAsLeaderAsync` (leader only):

```
ReplicateLogs(partition, "SchemaChange", entry, autoCommit: false)   // propose
   → if !Success: return (NotLeader/Timeout/Failed)
CommitLogs(partition, ticketId)                                      // commit quorum
   → if committed & Success:
        InvokeLocalSchemaApplyAsync(partition, entry)                // local fan-out
        return Committed
   → else:
        RollbackLogs(partition, ticketId)                            // abort
        return failure
```

Followers receive the committed entry through Kommander's `OnReplicationReceived`
callback, which `RegisterSchemaApply` wires to `SchemaReplicator.ApplyAsync`. On restart /
log recovery, `OnLogRestored` is wired to `SchemaReplicator.RestoreAsync`.

`autoCommit: false` is intentional: the leader wants an explicit commit/rollback boundary
so it only triggers local apply *after* the quorum commit succeeds.

### 5.3 Follower forwarding

A non-leader that is asked to do DDL cannot propose to Raft, so it **forwards the DDL ticket
to the schema leader**, which re-runs the operation on the normal leader path and replies with
the applied version. Two mechanisms exist; the production one ships the ticket over HTTP.

**Production: HTTP ticket forwarding with idempotent dedup.**

```
Follower CommandExecutor.AlterTable(ticket)
  └─ TryForwardAlterTableAsync → TryForwardDdlAsync
       ├─ AmISchemaLeaderAsync? → no
       ├─ resolve leader endpoint (Raft schema-leader identity → HTTP base URL)
       ├─ operationId = Guid.NewGuid("N")        // stable across retries of THIS call
       └─ HttpSchemaDdlForwarder.ForwardAlterTableAsync(leader, ticket, operationId)
             POST {leaderUrl}/internal/schema-ddl  { op, ticket, operationId }

Leader SchemaDdlForwardController
  ├─ leader check: not the schema leader → 503/not-leader (client re-resolves & retries)
  ├─ DdlOperationIdCache.TryGetOrReserve(operationId)
  │     ├─ already completed → replay cached result (no re-execute)   ← sequential retry
  │     └─ in flight        → collapse onto the in-progress execution ← concurrent retry
  ├─ execute the DDL locally as leader (the normal replicated path)
  └─ cache + return the applied result
```

- **`ISchemaDdlForwarder` / `HttpSchemaDdlForwarder`** (`CamusDB.Core`) is the client:
  `ForwardCreateTable/AlterTable/DropTable/AlterIndexAsync`, each carrying the stable
  `operationId`. It is injected into `CommandExecutor` (nullable — single-node builds pass
  `null`). It returns `bool?`: `null` = "not forwarded, handle locally", non-null = the
  forwarded applied result.
- **`SchemaDdlForwardController`** (`CamusDB/App`) is the receiver: it **always** begins with
  an explicit leader check (returns not-leader rather than mis-applying on a stale follower),
  then consults the op-id cache before executing. A leadership change between the check and
  execution surfaces as a typed error and the client re-forwards to the new leader — a finite,
  retry-bounded chain.
- **`DdlOperationIdCache`** (`CamusDB/App/Services`) is the two-layer dedup: a stable op id
  makes a lost-response retry replay the prior result instead of bumping the schema version
  twice, and an in-flight reservation collapses concurrent duplicates. This is the
  "applied at most once" guarantee.

**Test-only:** `ISchemaReplicationForwarder` (internal) forwards the raw committed *entry*
(not the ticket) directly between in-process nodes. It is wired only in `InProcessSchemaCluster`
so multi-node tests can exercise apply/convergence without standing up HTTP endpoints.

---

## 6. Apply, idempotency, and the two-version invariant

### 6.1 `SchemaReplicator.ApplyAsync` (per committed entry, on every node)

```
under Schema.Semaphore:
  if entry.FromVersion != current SchemaVersion:
      if entry.ToVersion <= current && WasSchemaDeltaApplied(...):
          record ack(ToVersion); return true       // duplicate / already applied
      else:
          throw "out of order"                      // gap → cannot apply safely
  if entry.ToVersion <= current SchemaVersion:
      record ack(ToVersion); return true            // idempotent skip

  ApplySchemaDelta(database.Schema, entry)          // mutate in place — leader AND follower
  InvalidateAppliedTableDescriptor(...)             // drop cached descriptor on DropTable
  record ack(ToVersion)
```

**`ApplyAsync` never persists.** It runs inside the schema partition's commit pipeline
(it is invoked from `InvokeLocalSchemaApplyAsync`, which the leader's
`ReplicateSchemaChangeAsLeaderAsync` awaits right after `CommitLogs`, and from followers'
`OnReplicationReceived`). Issuing the checkpoint's KV writes from here re-enters the *same*
Raft partition and deadlocks (`ProposalTimeout`). So apply is identical on every node:
mutate in-memory, invalidate descriptor, ack.

**The proposer persists the checkpoint** in `ReplicateAndWaitLocalApplyAsync`, *after*
`ReplicateSchemaChangeAsync` returns and local apply is observed — i.e. outside the partition
pipeline. Persist uses its own KV transaction with bounded retry. This inverts the older
"persist-before-advance" ordering: in-memory now advances first (in the apply callback) and the
checkpoint is written just after. That is safe because the **committed schema log is the source
of truth** and the KV checkpoint is a load-time optimization — a node whose checkpoint write
fails is not divergent, it rebuilds from the committed log. If the retries are **exhausted** the
proposer does *not* fail the DDL (the change is already committed and live cluster-wide); instead
it marks the node's schema subsystem **degraded** and steps down its schema-partition leadership
so a healthy peer can take over — see the persist-failure policy in §10. (Making schema-log
replay authoritative on restart, so a degraded node provably recovers without operator action, is
the remaining piece — see §13.)

`RestoreAsync` (log recovery) is a separate, simpler path: it applies in version order and
logs+skips out-of-order entries; it likewise only mutates in-memory.

**Idempotency keys to remember:**
- `WasSchemaDeltaApplied(schema, entry)` checks the *effect* (table present? column present?)
  so a re-delivered entry is recognized as already-applied rather than re-run.
- Apply is gated on `FromVersion == current` and `ToVersion <= current` skips. Together
  these make redelivery and restart-replay safe.

### 6.2 The two-version invariant

Borrowed from CockroachDB/Yugabyte: at any instant the cluster tolerates at most **two
adjacent schema versions** in use. We enforce it as a **proposal barrier**:

> Before proposing `FromVersion -> ToVersion`, every *live* node must have already applied
> `FromVersion`.

This is `WaitForPreviousVersionAcksAsync`, called *first* in
`ReplicateAndWaitLocalApplyAsync`. It means a second DDL client cannot race ahead and
stack a third version onto a cluster where some node is still on version N-1.

Implemented by `SchemaAckTracker` (per-db `{node → NodeAck{Version, LastSeen}}`):

- `RecordApplied(db, node, version)` is called by the apply path on every node and takes the
  `Math.Max` of the existing and new version. **It also refreshes `LastSeen` on every call**,
  even an idempotent re-apply of the same version — so an apply is itself the interim liveness
  signal.
- `WaitForAllLiveAsync(db, version, timeout, getLiveMembers, liveNodeLease, ct)` polls until
  every endpoint returned by `getLiveMembers()` has acked `version`, or throws on timeout.

**Live membership comes from Raft, not a manual register set.**
`EmbeddedKahuna.GetLiveSchemaNodes()` supplies the live set, and the lease controls which Raft
signal it uses:

- **Standalone mode** (`isClusterMode == false`): only `Raft.GetLocalEndpoint()`, because
  `GetNodes()` returns phantom witness endpoints that would otherwise wedge the gate forever.
- **Cluster mode, infinite lease (default):** the local endpoint plus every peer from
  `Raft.GetNodes()` — the gate waits for **every configured** member. Safe (never false-evicts)
  but a crashed-but-configured node freezes DDL until `SchemaAckWaitTimeout`.
- **Cluster mode, finite lease:** the local endpoint plus the peers in
  `Raft.GetActiveNodes(lease)` — the leader's **real per-follower liveness** view (Kommander
  tracks each follower's last `AppendLogs` response). A peer the leader has not heard from within
  the lease is presumed dead and dropped from the gate, so DDL completes without it; a
  **slow-but-alive** peer (still answering Raft, even if it has applied no schema delta) stays in
  the active set and must still ack. The gate runs on the schema leader (the proposer), so
  `GetActiveNodes` reflects its follower reachability.

Acks are keyed on `Raft.GetLocalEndpoint()`, so each node reports under its real Raft identity.

**Liveness is sourced from Raft activity, not from acks.** Because membership already filters out
dead peers via `GetActiveNodes`, the `SchemaAckTracker` is given an **infinite** lease and simply
waits for every member of the (already liveness-filtered) set to ack — it does **not** expire a
member on its own apply-derived `LastSeen`. This is what prevents false eviction: a Raft-alive but
schema-idle node has no fresh ack, but it is still in the active set, so the gate keeps waiting for
it. (The tracker's `LastSeen` field is retained as a recorded version stamp but no longer drives
liveness.) `SchemaAckLiveNodeLease` defaults to `Timeout.InfiniteTimeSpan`; both it and
`SchemaAckWaitTimeout` are tunable on `EmbeddedKahuna`.

> **Remaining limitation.** The tracker is still process-`static` (shared across in-process test
> nodes); a per-`EmbeddedKahuna` instance would model multi-node state more honestly. See §13.

---

## 7. In-memory model, persistence layout, and positional rows

### 7.1 In-memory shapes

- `Schema` — per database. Holds `SchemaVersion` (the monotonic counter), `Semaphore`
  (serializes apply/validate), and `Tables : Dictionary<name, TableSchema>`.
- `TableSchema` — `Id` (immutable), `Version`, `Name` (mutable), `Columns`, `Indexes`,
  `SchemaHistory` (past column layouts), and an optional async `SchemaHistoryLoader`.
- `TableColumnSchema` / `TableIndexSchema` — carry `State : SchemaElementState`
  (legacy elements default to `Public`).
- `TableDescriptor` — the opened, ready-to-use handle a query/DML uses. Its `.Schema`
  **points at the same `TableSchema` instance** stored in `Schema.Tables`. This identity is
  load-bearing (see §7.4).

### 7.2 Per-object metadata keys

To scale to databases with many tables, metadata is stored per object, not as one blob:

```
{db}/meta/version                       → the database SchemaVersion counter
{db}/meta/table/{tableId}               → one TableSchema, incl. its Indexes (current version)
{db}/meta/history/{tableId}/{version}   → one past column layout (TableSchemaHistory)
{db}/meta/coordinator/...               → PersistedCoordinatorJob(s) for in-flight staged
                                          changes, so a new leader can resume them (§8.2)
{db}/meta/system                        → SystemSchema — LEGACY index storage, read-only;
                                          only migrated from on load (see §7.3)
{db}/meta/schema                        → LEGACY single-blob schema (migrated on load)
```

- Keyed by **immutable `tableId`**, so renaming a table never moves its key.
- `LoadMetaAsync` range-scans `{db}/meta/table/` to rebuild `Schema.Tables`, and lazily
  loads history on demand via `SchemaHistoryLoader` (a row written under an old version
  triggers a history fetch the first time it's decoded).
- Serialization uses UTF-8 + a source-generated `JsonSerializerContext`
  (`MetaJsonContext` / `MetaJsonSerializer`), with a UTF-16 fallback (`DeserializeCompat`)
  for old data.
- Legacy single-blob schema is detected on load and migrated to per-object keys.

### 7.3 Indexes: owned by `TableSchema`, replicated through the log, built via the staged coordinator

Indexes are **owned by the replicated `TableSchema`**, like columns:
`TableSchema.Indexes : List<TableIndexSchema>`, persisted inside the same
`{db}/meta/table/{tableId}` blob. `TableIndexSchema` serves two forms (one type, disambiguated
for JSON by a `[JsonConstructor]`):

- **Persisted** (inside `TableSchema.Indexes`): immutable `Id`, `ColumnIds` (immutable column
  ids — rename-safe), `Type`, `State`, `StartOffset` (online-backfill checkpoint). `Columns`
  is empty.
- **In-memory (query/DML)** (inside `TableDescriptor.Indexes`): resolved column `Names`, `Type`,
  `State`. `TableOpener` builds this from `TableSchema.Indexes`, resolving names from `ColumnIds`.
  `Id`/`ColumnIds`/`StartOffset` are intentionally dropped here — code that needs them reads
  `table.Schema.Indexes`.

**Legacy `SystemSchema` (`{db}/meta/system`, `DatabaseIndexObject`)** is read-only: it is
no longer written by index DDL. On open, `MigrateIndexesFromSystemSchema` copies any
not-yet-migrated index objects into `TableSchema.Indexes` in memory; the next index DDL
persists them. `TableOpener` falls back to `SystemSchema` only for descriptors opened before
`LoadMetaAsync` runs.

**Index DDL is replicated and staged.** On a cluster node, `CommandExecutor.AlterIndex`
routes an **add** through `ExecuteClusterAddIndexAsync`, which (under `SchemaDdlSemaphore`) hands
a `SchemaChangeJob{ kind: Index, target: Public }` to the `SchemaChangeCoordinator` with an
`IndexBackfillAsync` delegate wired. The coordinator drives the staged online sequence (§8.2):

```
AddIndex(DeleteOnly)  → ack gate → SetElementState(WriteOnly) → ack gate
   → [backfill existing rows into the index, committed]        → SetElementState(Public) → ack gate
```

- `ReplicateAddIndexInStateAsync` emits the initial `AddIndex` delta with `State = DeleteOnly`
  (not a single jump to `Public`). Subsequent steps are `ReplicateElementStateAsync(…, Index)`
  → `ApplyIndexElementState`, which updates `TableSchema.Indexes[i].State` in place and **does
  not** bump `TableSchema.Version` (indexes aren't part of row layout). `SchemaReplicator` evicts
  each node's cached `TableDescriptor` on `SetElementState(Index)` so DML rebuilds with the new
  state.
- **Backfill** (`CommandExecutor.BackfillIndexEntriesAsync`) fires exactly on the
  `WriteOnly → Public` transition, on both the initial run and a leader-change resume that starts
  from `WriteOnly`. It scans existing rows (`KvTableStore.ScanRows`), decodes them with
  *writable* visibility, and writes index entries with `PutIndexEntry(backfillMode: true)`.
  Backfilled entries replicate to followers through Kahuna's Raft KV (each node sees the index
  via `FORCE_INDEX` without rebuilding); the metadata converges via the schema delta. Because the
  backfill commits **before** the `Public` delta is proposed, the ack gate on the `Public`
  version implies backfill-done cluster-wide.
- **Idempotent backfill (no duplicates on resume).** `PutIndexEntry(backfillMode: true)` makes a
  re-run safe: on a unique index a `NotSet` response triggers a read-back — same `rowId` ⇒ this
  is an idempotent re-write of a row a previous partial run already indexed, so skip it; a
  different `rowId` ⇒ a genuine duplicate-key violation, so throw `DuplicateUniqueKeyValue`.
- **Failure compensation.** If the staged add throws before reaching `Public`,
  `CompensateClusterAddIndexAsync` emits a `DropIndex` delta (removing the partial `DeleteOnly`/
  `WriteOnly` index on every node) and deletes the persisted coordinator job, leaving the cluster
  clean — there is never a *phantom* index (schema says it exists, no data) or a partially-public
  one.

A **drop** stays a single `DropIndex` delta (`ApplyDropIndex`, idempotent remove-by-name); no
staging is needed to remove an index. `DROP INDEX` reclaims the data via
`KvTableStore.DropIndexEntries`.

**The backfill is checkpointed and resumable.** `BackfillIndexEntriesAsync` processes rows in
bounded batches (`BackfillBatchSize`, currently 500 rows per Kahuna transaction). After each
batch *commits*, it invokes a checkpoint callback supplied by the coordinator, which persists
the last processed rowId into `PersistedCoordinatorJob.StartOffset`. A leader-change resume reads
that offset and restarts the scan via `ScanRows(afterRowId:)`, skipping the rows already indexed
rather than rebuilding from row zero. Two ordering/accounting details make this safe:

- **Commit before checkpoint.** Each batch's index entries are committed *before* `StartOffset`
  advances, so a crash in that window re-runs at most the last batch — and the idempotent
  `backfillMode` re-write (above) makes that harmless (no duplicates, no skips).
- **Attempts preserved.** The checkpoint write rebuilds the persisted job with the *current*
  resume-attempt count, so persisting progress mid-resume does not reset the attempt budget that
  bounds poison-job retries (§8.2).

> **Note on the final batch.** Only full batches checkpoint; the trailing partial batch does not
> (completion is signalled by the element reaching `Public`, not by a stored offset). A table
> whose row count is an exact multiple of the batch size performs one extra empty scan/commit at
> the end — harmless.

### 7.4 Positional row encoding & why renames are free (`RowEncoder`)

Rows are stored as `byte[]` values keyed `{tableId}:r/{rowId}`. The wire format is
**positional**: a 4-byte schema-version header, the rowId, then one slot per column **in
schema order** — no column names in the bytes. Index entries are keyed
`{tableId}:i:{indexId}/...`.

Therefore:
- **Renaming a column/table/index** changes only metadata. No row or index bytes move.
- **Decoding must use the row's own schema version for byte *layout*** (which columns, in
  what order, of what type) but the **current** schema for *visibility* (which columns the
  caller may see).

`RowEncoder` implements exactly this split:

- `Encode(schema, row, rowId)` writes the version header and writes a value only for
  **writable** columns (`WriteOnly`/`Public`); `DeleteOnly`/`Absent` columns get a Null
  slot.
- `Decode* (...)` reads the version header, fetches the **history layout** for that version
  (`GetSchemaHistory[Async]`), and for each column maps it to the **current** column via
  `FindCurrentColumn` (by immutable `Id`; name-fallback **only** for legacy Id-less rows).
  Visibility is then:
  - `DecodeAsync` → `PublicOnly` (user-facing reads see only `Public`).
  - `DecodeWritableAsync` → `Writable` (update/delete internals also see `WriteOnly`,
    so those values are preserved on rewrite and writable indexes stay maintained).

The `Id`-first / legacy-only-name-fallback rule prevents a dropped-then-re-added same-name
column from resurrecting stale bytes (the re-added column has a different `Id`, so old rows
correctly read it as absent).

---

## 8. Online schema changes (element states)

`SchemaElementState { Absent, DeleteOnly, WriteOnly, Public }` models the CockroachDB/
Yugabyte staged rollout of an add/drop so concurrent DML on other nodes never loses writes
or sees half-built structures.

**The state diagram.** An element (a column or an index) moves through these four states. Each
**edge is one `SetElementState` delta** (the very first edge of an add is the `AddColumn`/
`AddIndex` delta that creates the element directly in `DeleteOnly`). The rule that lets you
*take* an edge is always the same: the delta must be **committed in Raft and acked by every
live node** — i.e. the cluster must converge on that step — before the next edge is taken. That
convergence gate (driven by the coordinator, §8.2) is what keeps the cluster within the
two-version window the whole time.

```
                     ADD  ───────────────────────────────────────────────►
                 (1)              (2)                  (3) backfill + (4)
        ┌────────┐      ┌────────────┐         ┌────────────┐         ┌────────┐
        │ Absent │─────►│ DeleteOnly │────────►│ WriteOnly  │────────►│ Public │
        │ r✗ w✗  │◄─────│  r✗ w✗(*)  │◄────────│  r✗ w✓     │◄────────│ r✓ w✓  │
        └────────┘      └────────────┘         └────────────┘         └────────┘
                     ◄───────────────────────────────────────────────  DROP
        r = readable by user queries   w = writable by DML   (*) DeleteOnly: delete-time only

  Edge        Delta emitted                 Extra work / why the step exists
  ─────────────────────────────────────────────────────────────────────────────────────────
  (1) →DelOnly  AddColumn / AddIndex         Element exists in the catalog but is invisible and
                (State=DeleteOnly)           inert; no node reads or writes it. Establishes the
                                             element on every node before anyone depends on it.
  (2) →WriteOnly SetElementState(WriteOnly)  DML now *maintains* the element (writes the column,
                                             updates the index) but no query *reads* it yet — so
                                             new writes are captured before backfill runs.
  (3) backfill   (no delta; a committed      With the element WriteOnly everywhere, existing rows
                 data write before edge 4)   are filled in: column defaults are materialized /
                                             index entries are built. Runs once, on the leader.
  (4) →Public    SetElementState(Public)     Backfill is done and every new write is already
                                             maintained, so the element is safe to read. It
                                             becomes visible to queries.
  DROP (reverse) SetElementState(…) per step Mirror image: Public→WriteOnly (stop reading)
                                             →DeleteOnly (stop writing, delete-time only)
                                             →Absent (DropColumn / DropIndex removes it).
```

So the canonical sequences are:

```
AddColumn:  Absent → DeleteOnly → WriteOnly → (backfill defaults) → Public
AddIndex:   Absent → DeleteOnly → WriteOnly → (backfill entries)  → Public
DropColumn: Public → WriteOnly → DeleteOnly → Absent
DropIndex:  Public → Absent          (single delta — removing an index needs no staging)
```

**Why each intermediate state is necessary.** The danger in a distributed add is the two-version
window: while the change rolls out, one node may be a version ahead of another. `WriteOnly` exists
so that a node already running the change *captures* writes the lagging node's clients still send,
**before** backfill scans existing rows — otherwise a row inserted mid-backfill could be missed.
`DeleteOnly` exists for the symmetric drop case (and the start of an add): the element must be
*maintained-on-delete* before it can be fully writable, and fully gone only after no node still
reads it. Skipping a state would reintroduce exactly the lost-write / half-built-read races the
ladder is designed to prevent.

State semantics (`SchemaElementStateRules`):

| State | Readable (user) | Writable (DML) |
|---|---|---|
| `Public` | ✅ | ✅ |
| `WriteOnly` | ❌ | ✅ |
| `DeleteOnly` | ❌ | ❌ (delete-time only) |
| `Absent` | ❌ | ❌ |

`SetElementState` is the `SchemaOp` that advances one element across **adjacent** states
(validated transitions; same-state is a no-op that does not bump version/history). It carries
a `SchemaElementKind` so the same delta drives a column or an index (§3). Transitions are only
ever adjacent — there is no "Absent → Public" shortcut, which is what forces every add/drop
through the full convergence-gated ladder above.

### 8.1 DML honors states

All read/write paths respect the visibility/writability table above:

- `RowEncoder` encodes only writable columns; decodes with current-state visibility (§7.4).
- Insert/update target validation rejects non-writable columns; update/delete reload a
  **writable** row view so `WriteOnly` data survives a rewrite.
- Query binding/planning and `SHOW COLUMNS/INDEXES/CREATE TABLE` expose only `Public`
  elements (`SchemaElementStateRules.IsReadableIndex/IsWritableIndex` centralize the composite
  "index + all its columns" check).
- DML/read transactions **pin** each touched table's `(version, identity)` and the commit path
  rejects the transaction if the schema moved underneath it (see §9).

This is what makes the staged rollout safe: while a column is `WriteOnly` on the node running
the change and `Public`/`DeleteOnly` on another (the two-version window), concurrent DML on
either node neither loses a write nor reads a half-built element.

### 8.2 The online-schema coordinator

`SchemaChangeCoordinator` is the component that **drives** the staged sequence — it turns a
high-level intent ("add column X", "build index Y") into the successive `SetElementState`
deltas, waiting for the cluster ack gate between each one. It runs **on the schema leader**
(followers forward DDL via §5.3) and only in cluster mode (`!OwnsKahuna`).

**Job model & drive loop.** `RunJobAsync(database, SchemaChangeJob{ table, element, kind,
targetState }, columnDefinition?, indexBuildInfo?)`:

1. Compute `current = GetCurrentElementState(...)` (reads `Columns` or `Indexes` per kind) and
   the adjacent-transition `path` from `current` to `targetState` (forward for adds, reverse
   for drops). Empty path ⇒ already at target ⇒ no-op.
2. Persist the job durably (`PersistCoordinatorJobAsync`, attempt 0) **before** driving, so a
   leader change can resume it.
3. `DriveToTargetAsync`: for each `nextState` in `path`, emit one delta and let
   `ReplicateAndWaitLocalApplyAsync` enforce the `FromVersion` ack gate (§6.2) — so every live
   node has applied step *k* before step *k+1* is proposed (the two-version invariant for
   multi-step sequences). The first step of an add (`Absent → DeleteOnly`) calls
   `ReplicateAddColumnInStateAsync` / `ReplicateAddIndexInStateAsync`; subsequent steps call
   `ReplicateElementStateAsync(…, kind)`.
4. On success the durable job is deleted; on failure it is left for resume (the `finally` only
   deletes it once `current == targetState`).

**Backfill hook.** Just before the `WriteOnly → Public` transition (while `current` is still
`WriteOnly`, before reassignment), the coordinator invokes the matching delegate:

- **Column:** `BackfillAsync(db, table, columnInfo)` →
  `CommandExecutor.BackfillColumnDefaultsAsync` re-encodes existing rows so the new column's
  default is physically materialized (not just read-time injected) before it becomes readable.
- **Index:** `IndexBackfillAsync(db, table, indexBuildInfo, startOffset)` →
  `CommandExecutor.BackfillIndexEntriesAsync` (see §7.3).

Firing the backfill at exactly this point means it runs **after** `WriteOnly` is committed (so
`RowEncoder.Encode` already includes the column / the index accepts writes) and **before**
`Public` is committed (so existing rows carry the value before the element is visible). It fires
on both the initial run and a `WriteOnly`-start resume, closing the crash window between the
`WriteOnly` ack and the backfill commit.

**Persistence & leader-change resume.** The durable job is `PersistedCoordinatorJob`
(`{db}/meta/coordinator/...`): table, element, `targetState`, `ElementKind`, the column fields
(`ColumnType/NotNull/Default`) or index fields (`IndexId/IndexColumnIds/IndexType/StartOffset`),
and an `Attempts` counter. `SchemaReplicator` registers a leader-change callback
(`RegisterSchemaLeaderCallback` → `EmbeddedKahuna.OnLeaderChanged`); when this node wins
schema leadership it runs `ResumeJobsAsync`:

- Loads all persisted jobs (with bounded retry/backoff — `OnLeaderChanged` can fire before the
  new leader's KV state machine has applied every committed entry, so the job written by the
  previous leader may not be visible on the first read).
- For each job: reconstructs `ColumnInfo`/`IndexBuildInfo` (index column **names** are resolved
  from the persisted immutable `ColumnIds` against the current schema), recomputes the remaining
  path from the *current* element state, bumps and persists `Attempts` **before** driving (so a
  crash mid-resume still counts against the budget), and re-drives to target.
- A job that keeps failing is abandoned after `MaxResumeAttempts` (5): it is deleted and logged
  loudly rather than retried on every future election — a poison job can't loop forever.

**Adds start in `DeleteOnly`.** Because the coordinator exists, `AddColumn`/`AddIndex` never
land directly in `Public`. The cluster entry points
(`ExecuteClusterAddColumnAsync` / `ExecuteClusterAddIndexAsync`) hand the coordinator a job with
`targetState = Public`, and the staged path carries the element through
`Absent → DeleteOnly → WriteOnly → [backfill] → Public`. An interrupted add therefore leaves the
element in a valid intermediate state that resume completes — never a stuck half-add.

---

## 9. Schema-version pinning (transaction-level safety)

A DML/read statement opens the table, captures `table.Schema.Version`, and **pins** it on
its `KvTransaction`:

```csharp
// CommandExecutor.PinSchemaVersion
string resource = $"{database.Name}/{table.Id}";
tx.PinSchemaVersion(
    resource,
    table.Schema.Version,
    currentVersion: () => table.Schema.Version,
    isStillValid:   () => database.Schema.Tables.TryGetValue(table.Name, out var cur)
                          && cur.Id == table.Id);
```

At commit, `KvTransactionsManager.CommitAsync` calls `tx.ValidateSchemaPins()`, which:

1. runs `isStillValid` first → catches **drop / drop+recreate / rename** (the table is gone
   or now has a different `Id`), then
2. compares `currentVersion()` against the pinned version → catches **add/drop column,
   element-state** changes.

If either check fails the commit is rejected. (Transparent retry against the new version is a
future-work item — see §13; today the transaction simply fails with a typed error.)

Why this works without extra plumbing: `ApplyAlterColumn`/`ApplyElementState` mutate the
**same `TableSchema` instance** the cached `TableDescriptor` holds (`Version++` in place),
so the pin closure observes the bump. **This in-place-mutation identity is an invariant** —
if a future change ever replaces `Schema.Tables[name]` with a *new* `TableSchema` object
instead of mutating in place, the pin closure (and the per-alias query visibility version)
would silently stop observing changes. Keep apply mutating in place, or update pinning.

Read queries capture the visibility version at **plan time** (`QueryPlan.TableSchemaVersion`,
and per-alias `TableSchemaVersionByAlias` for joins) and decode against it, so a long scan
sees a consistent schema snapshot even if an `ALTER` lands mid-stream. Read-only autocommit
SELECTs don't run the commit-time validation step (they have a consistent snapshot already).

---

## 10. Failure handling & compensation

- **Checkpoint persist-failure (degrade + step-down policy):** the schema delta is already
  committed in Raft and applied in-memory cluster-wide, so a failed checkpoint is never
  divergence — it is a stale load-time cache the committed log reconciles. The proposer's
  `PersistSchemaCheckpointWithRetryAsync` retries the KV write with bounded backoff; on
  **exhaustion** it applies a defined policy rather than failing the (already-live) DDL:
  - It marks the node **degraded** (`DatabaseDescriptor.MarkSchemaSubsystemDegraded`). The
    degraded flag gates *all* further DDL on this node — both the proposer path
    (`ReplicateAndWaitLocalApplyAsync` throws up front) and the forwarder
    (`TryForwardDdlAsync` throws *before* the leader check, so post-step-down DDL returns a
    typed "degraded" error rather than a confusing "not leader").
  - It requests a **deferred schema-partition step-down** (`RequestDeferredSchemaStepDown`),
    fired from a `finally` *after* the in-flight KV transaction commits/rolls back — the defer
    is essential because, in single-partition clusters, schema and KV share one Raft partition,
    so stepping down before the commit would invalidate the in-flight transaction. Every DDL
    exit path fires it: the four `CommandExecutor` entry points and the leader-change resume
    callback. A healthy peer then wins the next election and takes over.
  - The committed DDL still returns success to the client. The degraded node does not ack any
    *new* version it cannot persist, because it refuses to propose while degraded.
  - **Recovery is by restart** today: a fresh `DatabaseDescriptor` opens non-degraded, and the
    node rebuilds from the committed log. Making that restart provably reconcile a stale/failed
    checkpoint against the committed log (replay-to-head) is the remaining durability piece —
    see §13.
- **Out-of-order / gap on apply:** thrown (apply) or logged+skipped (restore). A gap means a
  node is missing a delta; restore replays in order.
- **DDL transaction abort (`ExecuteDdlInTransaction`):** rolls back the KV transaction, then
  runs an optional `onAbort` compensation. Compensation errors are swallowed+logged so they
  never mask the original exception. Hard crashes need no compensation — the node reloads from
  persisted (rolled-back) metadata.
- **Cluster `ADD INDEX` failure (coordinator path):** if the staged add throws before reaching
  `Public`, `CompensateClusterAddIndexAsync` emits a `DropIndex` delta (removing the partial
  `DeleteOnly`/`WriteOnly` index on every node) and deletes the persisted coordinator job. Since
  the element is published incrementally but only becomes *usable* at `Public`, an abort never
  leaves a usable phantom index, and the next leader has no stale job to resume (§7.3, §8.2).
- **Coordinator interruption (leader loss mid-sequence):** the durable job survives; the new
  schema leader's `ResumeJobsAsync` re-drives the element to `Public` (re-running backfill
  idempotently), or abandons the job after `MaxResumeAttempts` and logs loudly — never a column
  or index left stuck in an intermediate state (§8.2).
- **Two-version gate timeout:** if a live node never acks `FromVersion`/`ToVersion` within
  `SchemaAckWaitTimeout`, the DDL throws. With the infinite live-node lease this is also how
  a dead-but-un-evicted member surfaces.

---

## 11. End-to-end example: `ALTER TABLE robots ADD COLUMN age INT DEFAULT 0` on a 3-node cluster

This is the full staged path: a follower forwards to the leader, and the coordinator drives
three adjacent deltas (`DeleteOnly → WriteOnly → [backfill] → Public`), gating on the cluster
ack between each.

```
Client → Node B (a follower)
  CommandExecutor.AlterTable
    TryForwardAlterTableAsync: B is not the schema leader for `mydb`
      → operationId = G; HttpSchemaDdlForwarder POSTs the ticket to leader A's /internal/schema-ddl
      → SchemaDdlForwardController on A: leader-check ✔, DdlOperationIdCache.TryGetOrReserve(G) ✔
      → A executes the DDL as leader (below); B awaits the result and returns it to the client

Node A (schema leader): ExecuteClusterAddColumnAsync (holds SchemaDdlSemaphore)
  coordinator = new SchemaChangeCoordinator; coordinator.BackfillAsync = BackfillColumnDefaultsAsync
  RunJobAsync(job{ robots, age, kind=Column, target=Public }, columnDefinition=age INT DEFAULT 0)
    current = Absent;  path = [DeleteOnly, WriteOnly, Public]
    PersistCoordinatorJob(attempt 0)                         // durable, for leader-change resume

    ── step 1: Absent → DeleteOnly ──
      ReplicateAddColumnInStateAsync(age, DeleteOnly)
        entry {From: 7, To: 8, AddColumn age(id=X), DeleteOnly}; ValidateSchemaDelta(clone) ✔
        ReplicateAndWaitLocalApplyAsync: ack-gate v7 → Raft propose+commit (partition=hash("mydb/meta"))
          → InvokeLocalSchemaApply on A; B,C via OnReplicationReceived → ApplyAsync (in-memory, ack v8)
          → spin until A.SchemaVersion ≥ 8 → persist checkpoint (proposer ctx) → WaitForSchemaAcks(8) ✔

    ── step 2: DeleteOnly → WriteOnly ──
      ReplicateElementStateAsync(age, WriteOnly, Column)     // SetElementState; v8 → v9, ack-gated

    ── backfill (current==WriteOnly, next==Public) ──
      BackfillColumnDefaultsAsync: re-encode existing rows so `age = 0` is physically stored
        (committed in its own txn, before age becomes readable)

    ── step 3: WriteOnly → Public ──
      ReplicateElementStateAsync(age, Public, Column)        // v9 → v10, ack-gated

    DeleteCoordinatorJob                                     // reached target → durable job removed
  return success → forwarded back to B → back to client
```

After this returns, every node has `age` at `Public`, the column's immutable id `X` baked
identically everywhere, and the database schema version advanced by three (one per delta). The
two-version gate guarantees no node was ever more than one version behind during the sequence.
Rows that existed before the change were physically backfilled to `age = 0` during the
`WriteOnly` window (so the value is materialized, not only injected at read time); rows still
on an older layout decode with their own version and read `age` with current visibility.

> If A had crashed after, say, the `WriteOnly` ack but before `Public`, the durable job survives:
> whichever node wins schema leadership runs `ResumeJobsAsync`, recomputes the remaining path
> (`[Public]`), re-runs the (idempotent) backfill, and drives `age` to `Public` — the client's
> forwarded call either gets the completed result or a typed error that re-forwards to the new
> leader (dedup by `operationId`).

---

## 12. Invariants checklist (don't break these)

1. **Schema log is single-partition per database** (`GetPrefixPartitionKey($"{db}/meta")`,
   never partition 0). Ordering depends on it.
2. **Deltas are adjacent `FromVersion → FromVersion+1`** and IDs are assigned once by the
   proposer and reused verbatim on apply.
3. **Apply mutates the existing `TableSchema` instance in place** (`Version++`). Pinning and
   query visibility depend on this identity.
4. **Acks are recorded only after a delta is actually applied in-memory.** Checkpoint
   persistence happens separately, in the proposer context — never from the apply callback
   (it would re-enter the schema partition and deadlock).
5. **The two-version gate is checked *before proposing*** the next change.
6. **Row bytes are positional and ID-keyed.** Names are metadata; renames never rewrite data.
7. **Decode layout = row's version; decode visibility = current/pinned version.**
8. **An add must reach `Public` via the coordinator, or be compensated/resumed** — it begins in
   `DeleteOnly` and is driven through `WriteOnly → [backfill] → Public` with the ack gate
   between steps. A failed add is compensated (`DropIndex` / rollback); an interrupted one is
   resumed by the next leader. Still treat any pre-existing non-`Public` element as `Public` on
   load as a safety net for legacy data.
9. **Index state changes do NOT bump `TableSchema.Version`** (indexes aren't in the row layout),
   but they DO advance the database `SchemaVersion` chain. After a `SetElementState(Index)`,
   evict the cached `TableDescriptor` so DML rebuilds with the new index state.
10. **Index backfill writes must be idempotent** (`PutIndexEntry(backfillMode: true)`): a resume
    re-runs the backfill, and re-indexing the same `(key → rowId)` must be a no-op, not a
    duplicate-key error.
11. **A forwarded DDL is applied at most once**: forward with a stable `operationId` and dedup
    on the leader (`DdlOperationIdCache`) so a lost-response retry never double-bumps the
    schema version.

---

## 13. Where to look next

### Tests that double as executable documentation

- `CamusDB.Tests/Storage/TestRowEncoder.cs` — positional encode/decode, visibility,
  history layout, drop+re-add identity.
- `CamusDB.Tests/Storage/TestKvTableStore.cs` — scan ordering + `afterRowId` resume.
- `CamusDB.Tests/Storage/TestEmbeddedKahuna.cs` — ack gate, Raft-sourced live membership,
  leader/follower apply.
- `CamusDB.Tests/Catalogs/TestSchemaReplicator.cs` — apply ordering, idempotency, descriptor
  eviction.
- `CamusDB.Tests/CommandsExecutor/TestTableAlterer.cs` — online index add + backfill (local).
- `CamusDB.Tests/CommandsExecutor/TestClusterAddColumn.cs` — coordinator-driven staged
  `AddColumn` across nodes, including physical backfill materialization.
- `CamusDB.Tests/CommandsExecutor/TestPersistentIndexSchema.cs` — index persist → reopen →
  deserialize round-trip; the new path stands alone with `SystemSchema` cleared.
- `CamusDB.Tests/Cluster/InProcessSchemaCluster.cs` — N distinct-node fixture with real Raft,
  ack-based convergence await, and fault injection (pause/kill/force-leader);
  `TestInProcessSchemaCluster.cs` — cluster create-table, paused-node catch-up, force-leader-
  change convergence, and staged `ADD/DROP INDEX` + column-backfill convergence.
- `CamusDB.Tests/Cluster/TestMultiPartitionRouting.cs` — partition routing.

### Known limitations & future work

- **Renames.** `RenameTable`/`RenameIndex`/`RenameColumn` are not yet implemented. They should
  be metadata-only (mutate `Name`, leave the immutable `Id` so no rows/indexes move) and drain
  old names across the two-version window. The positional/ID-keyed encoding (§7.4) already makes
  the data side free.
- **Per-instance ack tracker.** The ack-gate live set is now sourced from real Raft per-follower
  liveness (`GetActiveNodes`, §6.2), so a finite lease evicts only genuinely-dead nodes and never
  false-evicts an alive-but-idle one. The one remaining membership cleanup is that
  `SchemaAckTracker` is still process-`static` (shared across in-process test nodes); a
  per-`EmbeddedKahuna` instance would model multi-node state more honestly.
- **Restart-replay durability.** The persist-failure *policy* is implemented (degrade +
  step-down, §10), but recovery still relies on restart. The remaining piece is making the
  restart path provably reconcile a stale or failed checkpoint against the committed schema log
  — read the persisted checkpoint version as a *floor*, replay committed entries to head, and
  re-persist — so a degraded node recovers without operator action and a node that missed DDLs
  while offline always converges on reopen. This is the durability backing that lets the §10
  policy lean on restart.
- **Auto-retry on schema-version conflict.** A DML/read transaction whose pin is invalidated by a
  concurrent `ALTER` currently fails (§9) rather than transparently retrying against the new
  version.
- **Staged `DROP INDEX`.** Drops are a single delta; an online-safe
  `Public → WriteOnly → DeleteOnly → Absent` drop is a possible future refinement.
- **Legacy index cleanup.** `SystemSchema` index storage is read-only legacy (§7.3); remove it
  once all databases are known to be migrated.
- **Broader failure-injection coverage.** The in-process harness supports concurrent DML during
  ALTER, coordinator failover mid-sequence, node rejoin replay, dead-member lease, and persist-
  failure recovery scenarios — worth expanding into a standing suite.
