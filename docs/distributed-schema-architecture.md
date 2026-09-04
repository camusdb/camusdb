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
| Catalog | `CatalogsManager` | The stable entry point. Delegates only — see the table below for what actually does the work. Still the type every caller holds, so the `Replicate*` primitives the coordinator composes (`ReplicateAddColumnInStateAsync`, `ReplicateAddIndexInStateAsync`, `ReplicateElementStateAsync`, `ReplicateDropIndexAsync`) are reached through it. |

### Inside the catalog package

`CatalogsManager` is a facade. The work is divided by responsibility:

| Directory | Type | Owns |
|---|---|---|
| `Catalogs/` | `RelationCatalog` | create / alter / drop / rename / relink / truncate a relation |
| `Catalogs/` | `ViewCatalog` | views and materialized-view state |
| `Catalogs/` | `TableCommentWriter` | COMMENT ON, single-node path |
| `Catalogs/Replication/` | `SchemaChangeEntryFactory` | builds every `SchemaChangeLogEntry`; nothing else constructs one |
| `Catalogs/Replication/` | `SchemaChangePublisher` | the Raft round-trip, the apply wait, and both ack gates |
| `Catalogs/Replication/` | `SchemaElementReplicator` | column, index, constraint, settings and comment deltas |
| `Catalogs/Apply/` | `SchemaDeltaApplier` and six siblings | applies a committed delta to in-memory schema |
| `Catalogs/Meta/` | `MetaKeys`, `MetaKeyWriter`, `SchemaMetaStore` | key construction and KV input/output |
| `Catalogs/Meta/` | `SchemaLoader`, `SchemaHistoryStore` | the open path and lazy schema history |
| `Catalogs/Meta/` | `SchemaCheckpointWriter` | the durable checkpoint after a commit |
| `Catalogs/Meta/` | `OrphanTableStore`, `CoordinatorJobStore`, `MaterializedViewRefreshJobStore`, `ContentsRetirementStore`, `BranchMetaCopier` | the per-object record families |

**No type under `Catalogs/Apply/` takes an `IKahuna` or a `KvTransaction`.** Apply runs inside the
schema partition's commit pipeline on every node, and a KV write from there re-enters the same
partition and deadlocks it. The separation used to be a comment; it is now something the compiler
refuses to let you break.

| Replication glue | `SchemaReplicator` | Bridges Kahuna's apply/restore callbacks to `CatalogsManager`. Applies committed deltas in-memory (never persists from the callback), records acks, evicts cached `TableDescriptor`s, and registers the coordinator-resume leader callback. |
| KV / consensus | `EmbeddedKahuna` | Routes schema deltas to a Raft partition, replicates+commits them, fans them out to local subscribers, tracks per-node acks, sources live membership from Raft, and fires `OnLeaderChanged` for coordinator resume. |
| Liveness | `SchemaAckTracker` | Per-database, per-node `{version, lastSeen}` map; powers the two-version invariant gate. The live set comes from Raft membership; an optional finite lease expires silent members. |
| Forwarding | `ISchemaDdlForwarder` / `HttpSchemaDdlForwarder`, `SchemaDdlForwardController`, `DdlOperationIdCache` | Ship a DDL *ticket* from a follower to the schema leader over HTTP, re-execute it as leader, and dedup retries by a stable operation id. |
| Models | `SchemaChangeLogEntry`, `SchemaOp`, `SchemaElementState`, `SchemaElementKind`, `TableSchema`, `TableColumnSchema`, `TableIndexSchema`, `PersistedCoordinatorJob`, `DatabaseIndexObject` | The serialized delta, the operation kinds, the online-state enum, the column/index discriminator, the in-memory/persisted shapes, and the durable coordinator job. |
| Storage | `RowEncoder`, `KvTableStore` | Positional row encode/decode with element-state visibility; row/index scans; idempotent index backfill writes (`PutIndexEntry(backfillMode:)`). |
| Transactions | `KvTransaction`, `KvTransactionsManager` | Carry schema-version *pins* and validate them at commit; lock/modified tracking is lock-guarded for concurrent use. |
| Test harness | `InProcessSchemaCluster`, `FaultInjectingCommunication` | N distinct in-process nodes with real Raft, ack-based convergence await, and pause/kill/force-leader fault injection. |

### Single-node vs cluster: the `isClusterMode` flag

Every catalog mutation uses the replicated path. `isClusterMode` on `CommandExecutor` (and
threaded into `DatabaseOpener`) controls whether schema replication and coordinator
registration are active:

- **`isClusterMode == false`** (standalone): `CatalogsManager.CreateTable/AlterTable/DropTableSchema`
  apply the delta directly under `Schema.Semaphore` and persist, all inside the caller's DDL
  transaction. No Raft round-trip. `schemaReplicator.Register` is skipped.
- **`isClusterMode == true`** (cluster member): the *replicated* path
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
    byte[] Payload;        // op-specific, UTF-8 JSON (e.g. SchemaCreateTablePayload); read
                           // through GetPayload<T>(), never parsed directly — see §3.1
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

### 3.1 How an entry is written on the wire

An entry is replicated as a fixed header followed by the entry itself as UTF-8 JSON. The one place
that writes and reads these bytes is `SchemaChangeLogEntryCodec`:

```text
[0]        0x01                    framed format, version 1
[1]        database-id length      1 byte, so at most 255 UTF-8 bytes
[2..]      database id             UTF-8
           from-version            int64, little-endian
           to-version              int64, little-endian
           body                    the whole SchemaChangeLogEntry as UTF-8 JSON
```

The header repeats three fields that are also in the body, and that repetition is the point. Every
open database subscribes to its own schema-log partition, several databases can hash to the same
partition, and the node that proposed a change is delivered its own entry **twice** — once through
the replication callback and once through the local apply that lets the proposer observe its change
before the statement returns (§5.2). A subscriber therefore drops far more entries than it applies.
Reading the header answers both reasons to drop one — *not my database*, and *already applied* —
without deserializing anything and without allocating.

"Already applied" needs one thing the versions alone cannot give. A re-delivery of the entry that
produced the current version and a *different* entry claiming that same target version carry
identical from/to versions; the first must be dropped and the second must fail loudly as an
out-of-order change (§6.1). So `ApplyAsync` also compares a 64-bit digest of the entry bytes against
the digest of the delta the node last applied. Same digest means the same delta, and nothing else is
ever dropped from the header. `RestoreAsync` needs no digest: replay delivers the committed tail in
order, and it already treats every entry at or below the current version as done.

**The pre-framing form.** Entries written before this format are the entry as UTF-16 JSON, with a
payload that is itself UTF-16 JSON nested inside as base64. They begin `0x7B 0x00` — `{` in UTF-16 LE
— so a first byte of `0x01` cannot be one of them and the decoder branches on that byte alone. Both
forms decode, and a decoded entry remembers which form it came from so its payload is read with the
matching reader. The old form disappears from a cluster when log compaction retires those entries;
nothing rewrites them.

**Upgrade constraint: every node in a cluster must run the same build.** A build without this codec
cannot read a framed entry — it fails with a JSON error and Kommander raises a replication error.
That is deliberate. The log type string is unchanged, so an old node fails loudly instead of
filtering the entry out by type and silently falling behind. There is no dual-write mode.

`SetElementState` carries a `SchemaElementKind { Column, Index }` discriminator
(`SchemaElementStatePayload.ElementKind`, default `Column` so legacy entries deserialize
correctly). The same delta type therefore advances either kind; the apply path branches on
the discriminator (`ApplyElementState` vs `ApplyIndexElementState`).

---

## 4. The write path (proposing a DDL change)

This is the cluster path (`isClusterMode == true`). Entry point:
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
read the entry's frame (no decode, no allocation — see §3.1):
  if the frame names another database:                return true
  observe the committed head (fence)
  if ToVersion <= current SchemaVersion
     and the digest matches the delta last applied:   record ack(ToVersion); return true

decode the entry

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

> **Relaxed for liveness.** The strict "*every* live node must have applied `FromVersion`"
> barrier would let a single slow or unreachable follower stall all DDL for the full gate timeout.
> The gate therefore proceeds on a **majority** after a backstop delay, which trades the strict
> two-version bound for bounded DDL latency — paired with a **catch-up fence** so a lagging minority
> node never serves results against a stale schema. The full contract is **§6.3**.

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
- **Cluster mode, finite lease (default — 30 s):** the local endpoint plus the peers in
  `Raft.GetActiveNodes(lease)` — the leader's **real per-follower liveness** view (Kommander
  tracks each follower's last `AppendLogs` response). A peer the leader has not heard from within
  the lease is presumed dead and dropped from the gate, so DDL completes without it; a
  **slow-but-alive** peer (still answering Raft, even if it has applied no schema delta) stays in
  the active set and must still ack. The gate runs on the schema leader (the proposer), so
  `GetActiveNodes` reflects its follower reachability. 30 s is well above the Raft heartbeat
  interval, so a healthy-but-idle follower is never false-evicted.
- **Cluster mode, infinite lease (strict, opt-in via config `-1`):** the local endpoint plus every
  peer from `Raft.GetNodes()` — the gate waits for **every configured** member. Strictest (never
  false-evicts) but a crashed-but-configured node freezes DDL until `SchemaAckWaitTimeout`.

Acks are keyed on `Raft.GetLocalEndpoint()`, so each node reports under its real Raft identity.

**Liveness is sourced from Raft activity, not from acks.** Because membership already filters out
dead peers via `GetActiveNodes`, the `SchemaAckTracker` itself is given an **infinite** lease and
simply waits for every member of the (already liveness-filtered) set to ack — it does **not** expire a
member on its own apply-derived `LastSeen`. This is what prevents false eviction: a Raft-alive but
schema-idle node has no fresh ack, but it is still in the active set, so the gate keeps waiting for
it. (The tracker's `LastSeen` field is retained as a recorded version stamp but no longer drives
liveness.) The membership filter — `SchemaAckLiveNodeLease` on `EmbeddedKahuna` — **defaults to 30 s**
(config `schema_ack_live_node_lease_ms`, default `30_000`; set `-1` to restore the strict infinite
lease that waits on every configured node). Both it and `SchemaAckWaitTimeout` are tunable on
`EmbeddedKahuna` and via the `schema_ack_live_node_lease_ms` / `schema_ack_wait_timeout_ms` config
keys (validated at startup).

> **Ack transport.** `SchemaAckTracker` is a per-`EmbeddedKahuna` instance field (no
> `static`). After each local apply, `RecordAndPublishSchemaApplied` records the ack locally and
> fires a best-effort notification to the current schema-partition leader via `ISchemaAckSender`
> (`HttpSchemaDdlForwarder` in production, `InProcessSchemaAckRelay` in tests). The leader records
> the remote ack via `RecordRemoteSchemaAck`, so `WaitForAllLiveAsync` observes real follower
> progress in multi-process deployments. The in-process fixture exercises the same transport path
> through `InProcessSchemaAckRelay` rather than relying on co-location. The gate's existing
> timeout remains the correctness backstop for dropped acks (leader change, network loss).

### 6.3 DDL liveness under a slow/partitioned node — quorum backstop + catch-up fence

The strict barrier of §6.2 (every live node must ack `FromVersion` before the next DDL) makes DDL
hostage to the slowest node: a single follower that is slow to apply, or unreachable, stalls *all*
DDL for the full `SchemaAckWaitTimeout` (30s). The cluster replaces that with a two-part contract — a
**liveness** rule that bounds DDL latency, and a **safety** fence that keeps the relaxation sound.

**Liveness — the quorum backstop.** `SchemaAckTracker.WaitForAllLiveAsync` returns a
`SchemaAckOutcome`:

- `FullConvergence` — every live node acked (the fast path, normal operation);
- `QuorumBackstop` — after `SchemaAckQuorumBackstopDelay` (default **10s**, on `EmbeddedKahuna`) a
  **majority** `⌊N/2⌋+1` of live nodes acked. The committed Raft log already guarantees the delta is
  durable on that majority, so DDL is safe to proceed; minority laggards catch up from the log;
- `Timeout` — neither, by the gate deadline → DDL fails.

The proposer treats `FullConvergence` and `QuorumBackstop` as success. The 10s default is generous
enough to clear a Raft leadership election (3–6s) so a transient election is not mistaken for a slow
follower. **Guarantee:** *DDL completes within the gate timeout whenever a majority of the cluster
applies the delta, regardless of what the minority does — a single slow/unreachable follower caps
DDL latency at ~`SchemaAckQuorumBackstopDelay`, not the full timeout.* Set the delay to
`Timeout.InfiniteTimeSpan` to restore the strict "every node must ack" behaviour.

**Why this needs a fence.** The backstop runs on *both* gates, including the §6.2 pre-proposal
barrier. So the proposer can advance `N → N+1` while a minority node is still at `N-1`; under
sustained DDL plus a persistently-slow node the divergence is otherwise **unbounded**, and a node
behind on the schema partition but caught up on a *data* partition would decode a row written under a
newer schema with its stale layout. The quorum backstop therefore weakens the two-version *bound*,
and the fence restores the *guarantee that no node ever serves results against a stale schema*.

**Safety — the catch-up fence.** Each node tracks `DatabaseDescriptor.HeadSchemaVersion`: the highest
schema-log `ToVersion` it has **received** (committed in Raft and delivered to `ApplyAsync` /
`RestoreAsync`), updated monotonically and lock-free via `ObserveSchemaEntryHead` *before* the schema
lock is taken — so the head/applied gap is visible to concurrent DML even mid-apply. The rule:

> If `HeadSchemaVersion − Schema.SchemaVersion > 1`, at least two committed schema deltas are in this
> node's apply pipeline but not yet materialised. The node **rejects reads and DML** for that database
> with the retryable `SchemaCatchingUp` (`CADB0503`) error until it catches up. A gap of exactly 1
> (the entry being applied, the two-version bound) is tolerated.

Enforced in `TableOpener.Open` — the choke point every query and DML passes through, checked *before*
the descriptor cache, so a node that falls behind after opening a table is fenced on its next
operation, and **re-admitted automatically** once apply catches the head (gap ≤ 1). The fence is
sound for the real threat model: a node lagging *schema-apply* while caught up on *data* has an
advancing `HeadSchemaVersion` (the entry was received) and is fenced; a *fully* partitioned node
receives neither new schema nor new data, so it has nothing newer to mis-decode.

**What the fence cannot see: an entry that was never delivered.** `HeadSchemaVersion` advances
only when an entry reaches `ApplyAsync`/`RestoreAsync`. Kommander delivers each committed entry
exactly once, to whichever subscribers exist at that instant, and never redelivers it — so a delta
that commits while a database is *unopened* on some node, or inside the open-time gap between the
checkpoint read and the subscription registration (`DatabaseOpener.LoadDatabase`), leaves that
node's head equal to its applied version. The fence passes, no error is raised anywhere, and the
node serves the stale schema indefinitely (observed in production as one node answering a third of
a cluster's traffic with `TableDoesntExist` for a full run). The freshness reconciler in §6.4 is
the repair for exactly this blind spot.

Because the fence fires in `TableOpener.Open` *before* any write or schema-version pin, the in-flight
transaction is untouched and the same operation is safe to re-run. `ExecuteNonSQLQuery` exploits this:
it auto-retries a `SchemaCatchingUp` DML a few times with a short exponential backoff, so a brief lag
clears transparently and the caller only ever sees `CADB0503` if the node stays behind across all
attempts. (The commit-routing `TransactionMustRetry`/`CADB0504` is different — it is thrown after the
transaction is already spent, so the caller must restart the whole operation from a new transaction
rather than have the executor retry it in place.)

**Net contract (the revised two-version invariant).** DDL proceeds once a **majority** has applied
the delta (bounded latency); a node more than one version behind the committed head **fences itself**
(rejects DML, `SchemaCatchingUp`) until it converges — so a lagging minority node never serves
results against a stale schema.

> **Operational note.** A `QuorumBackstop` outcome and a fenced node both mean a follower is lagging,
> and both are surfaced: the post-commit gate logs a warning **naming the lagging endpoints** (and the
> timeout error names the nodes that never acked), and always-on `SchemaMetrics` counters track
> `QuorumBackstopActivations` and `SchemaCatchingUp` `FenceRejections` (global + per-database) so a
> lagging follower is observable as a number, not just a log line. (Wiring those counters to an
> external metrics exporter is left for when the codebase grows one.)

### 6.4 Schema freshness reconciliation — repairing undelivered deltas

The delivery model has a structural hole the fence cannot cover (§6.3): a committed delta reaches
only the subscribers registered at commit time. A database subscribes per node, only while open,
and `DatabaseOpener.LoadDatabase` registers the subscription *after* it reads the checkpoint — so a
delta that commits while the database is unopened on a node, or inside that load-to-register gap,
is consumed with no subscriber and is gone. The node's in-memory catalog silently diverges from the
cluster with `head == applied`, and nothing in the ack gate blocks it: a node with no ack record is
deliberately skipped (it "hasn't opened the database").

`SchemaFreshnessReconciler` closes the hole using the durable checkpoint as a staleness detector
and repair source. The proposer persists `{db}/meta/*` after every committed delta (§6.1), so the
persisted `{db}/meta/version` is a cluster-visible floor on the committed head. The reconciler:

1. probes that one key (a single KV read, single-flight per descriptor, cooldown-limited);
2. when it is ahead of memory, loads a full snapshot (version, tables, views, system blob) in one
   transaction **without** the schema lock — holding the lock across KV reads would stall the apply
   pipeline, which yields on that lock;
3. installs the snapshot under `Schema.Semaphore` only if it is *still* ahead — a concurrent live
   apply that raced past the snapshot wins and the snapshot is discarded (monotonic install);
4. clears the descriptor cache, invalidates cached results/statistics for every relation the swap
   touched, advances the fence head, and acks the reached version to the leader's gate.

Three triggers, all funnelled through the same reconciler:

- **Open-time** — `DatabaseOpener.LoadDatabase` re-probes right after registering the subscription,
  closing the load-to-register gap for any delta whose checkpoint already landed;
- **Miss-triggered** — `TableOpener.Open` probes before letting `TableDoesntExist` surface: a miss
  may be a user typo, but it is also the only signal a node that missed a `CREATE TABLE` will ever
  produce. The cooldown bounds the cost of typo storms to one KV read per second per database;
- **Periodic sweep** — `SchemaFreshnessSweeper` (cluster mode only, `SchemaFreshnessCheckIntervalMs`,
  default 10s) probes every open database each tick, catching the silent variants — a missed
  `DROP TABLE`/`ADD COLUMN` produces no miss on this node, and a missed `CREATE` whose checkpoint
  landed after the open-time probe stays invisible until something queries it.

A delayed live delivery that arrives *after* a reconcile jumped past it is absorbed by
`ApplyAsync`'s idempotency: the structural `WasSchemaDeltaApplied` check recognizes the entry's
effect in the reloaded schema and re-acks. What the reconciler deliberately does **not** repair is a
checkpoint that is itself behind the committed log (persist exhaustion, §10) — that remains the
restart-replay path's job, because only the log knows more than the checkpoint.

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
(followers forward DDL via §5.3) and only in cluster mode (`isClusterMode == true`).

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
  `SchemaAckWaitTimeout`, the DDL throws. Under the strict infinite live-node lease (opt-in) this is
  also how a dead-but-un-evicted member surfaces; the default finite lease (30 s) instead ages a dead
  node out of the gate so DDL recovers on its own.

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

- ~~**Renames.**~~ **Implemented.** `RENAME TABLE`, `RENAME COLUMN`, and
  `RENAME INDEX` are metadata-only operations: the immutable `Id` is preserved so no rows
  or index entries move, and `TableSchema.Columns`/`Indexes` are updated in place. Column
  renames propagate retroactively to all `SchemaHistory` snapshots (the rename is a label
  change; positional/ID-keyed encoding means every row decodes correctly under the new name
  regardless of schema version). All three syntaxes are exposed exclusively through SQL via
  the `/execute-sql-ddl` route; no typed REST endpoints are added. Forwarding and cluster
  convergence use the existing `ExecuteDdlInTransaction` + replication path.
- ~~**Ack transport / per-instance tracker.**~~ **Implemented.** `SchemaAckTracker` is now
  a per-`EmbeddedKahuna` instance. `RecordAndPublishSchemaApplied` sends follower acks to the
  leader via `ISchemaAckSender` (HTTP in production, in-process relay in tests). See §6.2.
- **Auto-recovery without restart.** Restart-replay durability is implemented: on open,
  the restore path reads the persisted checkpoint version as a *floor*, replays committed schema
  entries to head, re-persists the checkpoint, and clears the degraded flag (`SchemaReplicator.
  OnSchemaRestoreFinishedAsync`, covered by the schema-restore replay tests). Live recovery from a
  *stale in-memory catalog* is also implemented: the freshness reconciler (§6.4) detects a
  checkpoint version ahead of memory and reloads in place, without a restart, via the open-time,
  miss-triggered, and periodic-sweep probes. The remaining gap is the inverse case — a live node
  whose *checkpoint* is behind the committed log after persist exhaustion re-persisting in place;
  that recovery still rides the restart-replay path, because only the log carries what the
  checkpoint lost.
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
