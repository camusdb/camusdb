# Distributed Schema — Architecture & Developer Guide

> **Audience:** engineers maintaining or extending CamusDB's catalog/DDL layer.
> **Scope:** how schema (tables, columns, indexes) is changed, replicated, persisted,
> versioned, and made visible across a cluster. 

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

---

## 2. Component map

| Layer | Type | Responsibility |
|---|---|---|
| SQL / executor | `CommandExecutor` | Entry point for DDL & DML. Owns the DDL transaction, schema-version *pinning*, and follower→leader *forwarding*. |
| Catalog | `CatalogsManager` | Builds deltas, validates them, applies them (`ApplySchemaDelta`), persists per-object metadata, loads metadata on open. |
| Replication glue | `SchemaReplicator` | Bridges Kahuna's apply/restore callbacks to `CatalogsManager`. Owns the leader-stages-then-applies dance and ack recording. |
| KV / consensus | `EmbeddedKahuna` | Routes schema deltas to a Raft partition, replicates+commits them, fans them out to local subscribers, tracks per-node acks. |
| Liveness | `SchemaAckTracker` | Per-database, per-node "last applied version" map; powers the two-version invariant gate. |
| Models | `SchemaChangeLogEntry`, `SchemaOp`, `SchemaElementState`, `TableSchema`, `TableColumnSchema`, `TableIndexSchema`, `DatabaseIndexObject` | The serialized delta, the operation kinds, the online-state enum, and the in-memory/persisted shapes. |
| Storage | `RowEncoder`, `KvTableStore` | Positional row encode/decode with element-state visibility; row/index scans. |
| Transactions | `KvTransaction`, `KvTransactionsManager` | Carry schema-version *pins* and validate them at commit. |

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

`SchemaOp` is intentionally small. Note that **index DDL (`AddIndex`/`DropIndex`) is not
yet routed through this log** — see §8. The enum has the slots reserved, but today indexes
live in a separate `SystemSchema` (see §7.3).

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

A non-leader that is asked to do DDL cannot propose. Two mechanisms exist:

- **Production:** `CommandExecutor.TryForward*Async` checks `AmISchemaLeaderAsync`; if not
  leader, it forwards the *DDL ticket* to the leader via `ISchemaDdlForwarder` (the
  intended production transport over the cluster's node-to-node channel), then waits for
  the forwarded change to apply locally.
- **Test-only:** `ISchemaReplicationForwarder` (internal) forwards the raw entry. This is
  wired only in the in-process multi-node test harness.

> The production HTTP/node forwarder, the endpoint map, and idempotent retry semantics are
> tracked as DS5 carry-forwards — see the tasks doc. The *protocol* works over real Kahuna
> gRPC today; the missing piece is the follower→leader DDL *routing* transport for a real
> multi-process deployment.

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

  isLeader = AmILeader(partition)
  if isLeader:
      staged = Clone(schema); ApplySchemaDelta(staged, entry)   // stage
      PersistCheckpointAsync(staged, ...)                        // durable FIRST
          └─ on failure: log and return true WITHOUT advancing or acking
      ApplySchemaDelta(database.Schema, entry)                   // then mutate in place
  else:
      ApplySchemaDelta(database.Schema, entry)                   // followers mutate in place

  InvalidateAppliedTableDescriptor(...)   // drop cached descriptor on DropTable
  record ack(ToVersion)
```

Why leaders stage-then-apply: the leader persists the checkpoint on a *clone* before
mutating live state, so a persistence failure leaves in-memory schema untouched and does
**not** advance the version or record an ack. Followers apply in place because their
persistence happens through the normal committed-log replay machinery.

`RestoreAsync` (log recovery) is a separate, simpler path: it applies in version order and
logs+skips out-of-order entries; it does not do the leader check or the staged persist.

**Idempotency keys to remember:**
- `WasSchemaDeltaApplied(schema, entry)` checks the *effect* (table present? column present?)
  so a re-delivered entry is recognized as already-applied rather than re-run.
- Apply is gated on `FromVersion == current` and `ToVersion <= current` skips. Together
  these make redelivery and restart-replay safe.

### 6.2 The two-version invariant (DS7)

Borrowed from CockroachDB/Yugabyte: at any instant the cluster tolerates at most **two
adjacent schema versions** in use. We enforce it as a **proposal barrier**:

> Before proposing `FromVersion -> ToVersion`, every *live* node must have already applied
> `FromVersion`.

This is `WaitForPreviousVersionAcksAsync`, called *first* in
`ReplicateAndWaitLocalApplyAsync`. It means a second DDL client cannot race ahead and
stack a third version onto a cluster where some node is still on version N-1.

Implemented by `SchemaAckTracker` (per-db `{node → lastAppliedVersion}` and
`{node → lastSeen}`):

- `RegisterLocalSchemaAckNode` / `RecordLocalSchemaApplied` / `UnregisterLocalSchemaAckNode`
  are called by `SchemaReplicator.Register` (on database open, seeding the *current* loaded
  version so a fresh node doesn't stall the first `FromVersion=0` gate), by the apply path,
  and by descriptor close.
- `WaitForAllLiveAsync(db, version, timeout, liveNodeLease)` blocks until every live member
  has acked `version`, or throws on timeout.

**Liveness vs correctness trade-off:** `SchemaAckLiveNodeLease` defaults to
`Timeout.InfiniteTimeSpan`, i.e. registered nodes never expire by elapsed time. There is no
real heartbeat at this layer yet, so we choose correctness (never silently drop a slow-but-
alive follower from the quorum) over liveness (a crashed, un-unregistered node freezes DDL
until `SchemaAckWaitTimeout`). The lease parameter is a hook for future heartbeat
integration. Both timeouts are tunable on `EmbeddedKahuna` (`SchemaAckWaitTimeout`,
`SchemaAckLiveNodeLease`).

> The tracker is process-global/`static` purely to support the in-process multi-node test
> harness; it is **not** a multi-process ack transport. Replacing it with real
> heartbeat/membership is a DS10/DS11 carry-forward.

---

## 7. In-memory model, persistence layout, and positional rows

### 7.1 In-memory shapes

- `Schema` — per database. Holds `SchemaVersion` (the monotonic counter), `Semaphore`
  (serializes apply/validate), and `Tables : Dictionary<name, TableSchema>`.
- `TableSchema` — `Id` (immutable), `Version`, `Name` (mutable), `Columns`,
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
{db}/meta/system                        → SystemSchema (indexes live here today)
{db}/meta/table/{tableId}               → one TableSchema (current version, no history)
{db}/meta/history/{tableId}/{version}   → one past column layout (TableSchemaHistory)
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

### 7.3 Indexes: `SystemSchema`, not the schema log (yet)

Index metadata is a `DatabaseIndexObject` stored inside `SystemSchema`
(`{db}/meta/system`), **separate** from `Schema.Tables`. It carries:

```
Id, Name, TableId, ColumnIds, Type (Unique/Multi),
StartOffset  // online-backfill checkpoint: last completed rowId
State        // SchemaElementState
```

This is why index DDL does not currently flow through `SchemaChangeLogEntry`/Raft —
replicated index ownership/backfill into `Schema.Tables` is deferred (DS9 builds the local
online-backfill machinery; DS10/DS11 add cluster-wide completion).

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
or sees half-built structures:

```
AddColumn:  Absent → DeleteOnly → WriteOnly → Public
DropColumn: Public → WriteOnly → DeleteOnly → Absent
AddIndex:   Absent → DeleteOnly → WriteOnly → (backfill) → Public
```

State semantics (`SchemaElementStateRules`):

| State | Readable (user) | Writable (DML) |
|---|---|---|
| `Public` | ✅ | ✅ |
| `WriteOnly` | ❌ | ✅ |
| `DeleteOnly` | ❌ | ❌ (delete-time only) |
| `Absent` | ❌ | ❌ |

`SetElementState` is the `SchemaOp` that advances one element across **adjacent** states
(validated transitions; same-state is a no-op that does not bump version/history).

### What's wired today vs deferred

- **DS6 (foundation):** the state enum, the model fields, and `SetElementState` apply
  + validation exist. **`AddColumn` currently lands directly in `Public`** (single step),
  because nothing yet *drives* the staged sequence. Until the coordinator exists, a column
  must never be left non-`Public`.
- **DS8 (DML honors states):** all read/write paths respect the table above —
  - `RowEncoder` encodes only writable columns; decodes with current-state visibility.
  - Insert/update target validation rejects non-writable columns; update/delete reload a
    **writable** row view so `WriteOnly` data survives a rewrite.
  - Query binding/planning and `SHOW COLUMNS/INDEXES/CREATE TABLE` expose only `Public`
    elements (`SchemaElementStateRules.IsReadableIndex/IsWritableIndex` centralize the
    composite "index + all its columns" check).
  - DML/read transactions **pin** each touched table's `(version, identity)` and the
    commit path rejects the transaction if the schema moved underneath it (see §9).
- **DS9 (resumable index backfill, local foundation):** `ADD INDEX` installs the index
  `WriteOnly`, streams existing rows via `KvTableStore.ScanRows`, writes index entries, and
  flips to `Public` only after the backfill completes. `DatabaseIndexObject.StartOffset`
  holds a rowId checkpoint and `ScanRows(afterRowId:)` can resume from it.
- **Deferred (DS7 coordinator / DS10 / DS11):** a resumable **coordinator job** that emits
  the successive `SetElementState` deltas, waits for the `FromVersion` ack gate between each
  one, runs the index backfill with independent checkpoint commits, drives cluster-wide
  completion, and adds automatic retry on schema-version conflict.

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

If either check fails the commit is rejected. (Automatic retry against the new version is a
deferred carry-forward; today the transaction simply fails.)

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

- **Persist-failure on leader apply:** logged; the version is **not** advanced and no ack is
  recorded, so the cluster stays consistent and the proposer's spin-wait eventually times
  out rather than reporting a phantom success.
- **Out-of-order / gap on apply:** thrown (apply) or logged+skipped (restore). A gap means a
  node is missing a delta; restore replays in order.
- **DDL transaction abort (`ExecuteDdlInTransaction`):** rolls back the KV transaction, then
  runs an optional `onAbort` compensation. `ADD INDEX` uses this to remove the phantom
  in-memory index (it mutates `table.Indexes` + `SystemSchema` *before* commit, so a failed
  backfill must undo those in-memory mutations). Compensation errors are swallowed+logged so
  they never mask the original exception. Hard crashes need no compensation — the node
  reloads from persisted (rolled-back) metadata on restart.
- **Two-version gate timeout:** if a live node never acks `FromVersion`/`ToVersion` within
  `SchemaAckWaitTimeout`, the DDL throws. With the infinite live-node lease this is also how
  a dead-but-un-evicted member surfaces (a DS11 test target).

---

## 11. End-to-end example: `ALTER TABLE robots ADD COLUMN age INT` on a 3-node cluster

```
Client → Node B (a follower)
  CommandExecutor.AlterTable
    TryForwardAlterTableAsync: B is not the schema leader for `mydb`
      → forward ticket to leader Node A via ISchemaDdlForwarder
      → await forwarded apply locally, return

Node A (schema leader)
  ExecuteDdlInTransaction (holds SchemaDdlSemaphore, opens tx)
    AlterTableReplicatedAsync
      under Schema.Semaphore: build entry {From: 7, To: 8, AddColumn age(id=X), Public}
                              ValidateSchemaDelta on a clone → ok
      ReplicateAndWaitLocalApplyAsync:
        1. WaitForPreviousVersionAcks(7): A, B, C all already applied v7 ✔
        2. ReplicateSchemaChangeAsync:
             Raft propose (partition = hash("mydb/meta"), not 0)
             Raft commit (quorum)
             InvokeLocalSchemaApply on A  → ApplyAsync: stage+persist, mutate, ack v8
           (B and C receive via OnReplicationReceived → ApplyAsync → persist/mutate/ack v8)
        3. spin until A.SchemaVersion >= 8 && column age present ✔
        4. WaitForSchemaAcks(8): A, B, C all acked v8 ✔
    commit tx, release semaphore
  return success → forwarded back to B → back to client
```

After this returns, every node has `age` at `Public`, schema version 8, and the column's
immutable id `X` baked identically everywhere. Existing rows (written at v7) decode with
the v7 layout but are read with v8 visibility: `age` reads as absent/null until updated.

---

## 12. Invariants checklist (don't break these)

1. **Schema log is single-partition per database** (`GetPrefixPartitionKey($"{db}/meta")`,
   never partition 0). Ordering depends on it.
2. **Deltas are adjacent `FromVersion → FromVersion+1`** and IDs are assigned once by the
   proposer and reused verbatim on apply.
3. **Apply mutates the existing `TableSchema` instance in place** (`Version++`). Pinning and
   query visibility depend on this identity.
4. **Acks are recorded only after a delta is actually applied** (and after a successful
   persist on the leader path).
5. **The two-version gate is checked *before proposing*** the next change.
6. **Row bytes are positional and ID-keyed.** Names are metadata; renames never rewrite data.
7. **Decode layout = row's version; decode visibility = current/pinned version.**
8. **No element may be left in a non-`Public` state** until the staged coordinator drives it
   to completion; treat any pre-existing non-`Public` element as `Public` on load as a safety
   net.

---

## 13. Where to look next

- Task-by-task status, acceptance criteria, and outstanding carry-forwards:
  [`distributed-schema-changes-tasks.md`](./distributed-schema-changes-tasks.md)
  (DS0–DS12; DS0–DS9 are implemented foundations, DS5R/DS10/DS11/DS12 remain).
- Design rationale and the CockroachDB/Yugabyte comparison:
  [`distributed-schema-changes-plan.md`](./distributed-schema-changes-plan.md).
- Tests that double as executable documentation:
  - `CamusDB.Tests/Storage/TestRowEncoder.cs` — positional encode/decode, visibility,
    history layout, drop+re-add identity.
  - `CamusDB.Tests/Storage/TestKvTableStore.cs` — scan ordering + `afterRowId` resume.
  - `CamusDB.Tests/Catalogs/TestSchemaReplicator.cs`, `TestEmbeddedKahuna.cs` — ack gate,
    leader/follower apply, follower forwarding.
  - `CamusDB.Tests/CommandsExecutor/TestTableAlterer.cs` — online index add + backfill.
  - `CamusDB.Tests/Cluster/TestMultiPartitionRouting.cs` — partition routing.

### Key carry-forwards still open

- **DS5R:** rename ops (`RenameTable`/`RenameIndex`/`RenameColumn`) end-to-end.
- **DS5:** production follower→leader DDL forwarder + endpoint map + idempotent retries.
- **DS7 coordinator:** drive staged `SetElementState` sequences with the ack gate between
  each, then let `AddColumn` start in `DeleteOnly`; auto-retry on schema-version conflict.
- **DS9 → DS10/DS11:** coordinator-owned checkpoint commits, leader-change resume,
  cluster-wide index completion; replace the in-process static `SchemaAckTracker` with real
  heartbeat/membership before enabling timed lease expiry.
