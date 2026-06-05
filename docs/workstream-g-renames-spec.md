# Workstream G — Renames (DS5R) implementation spec

> **Scope:** add `RENAME TABLE` / `RENAME COLUMN` / `RENAME INDEX` as first-class, replicated,
> metadata-only schema changes. Renames must never rewrite a single row or index entry, must
> converge cluster-wide through the existing schema log, and must drain the old name safely across
> the two-version window.
>
> **Why it's cheap:** rows are encoded **positionally and keyed by immutable `Id`**, not by name
> (architecture doc §7.4). A rename changes only metadata. The hard parts are therefore not the
> data — they are (a) the immutable-`Name` model records, (b) re-keying `Schema.Tables` for a
> table rename, (c) pin/visibility invalidation, and (d) draining the freed name.
>
> **Independent:** depends only on the existing replicated-schema machinery (A done, B/C/D done).
> No coordinator staging — a rename is a single atomic delta, not an online-state ladder.

---

## 0. Current state (grounded)

- `SchemaOp` (`Catalogs/Models/SchemaOp.cs`) ends at `SetElementState = 6`. No rename ops.
- `AlterTableOperation` already declares `RenameColumn`, but **nothing handles it** — `AlterTable`
  only dispatches `AddColumn`/`DropColumn` (`CommandExecutor.cs:850-852`). There are no
  `RenameTable`/`RenameIndex` tickets or commands at all.
- Model mutability:
  - `TableSchema`: `Id`, `Version`, `Name` are all `{ get; set; }` — **mutable**.
  - `TableColumnSchema.Name` and `TableIndexSchema.Name` are `{ get; }` — **immutable**. A
    column/index rename must **replace the record in its list** with a renamed copy that preserves
    the immutable `Id` (and type/state/default/columnIds), mirroring how `ApplyIndexElementState`
    replaces an index entry.
- `Schema.Tables` is `Dictionary<name, TableSchema>` (`ApplyCreateTable` does `Tables.Add(name, …)`
  / `ApplyDropTable` does `Tables.Remove(name)`). A **table** rename is therefore a **re-key**:
  remove the old key, add the new key, keep the *same* `TableSchema` instance, mutate its `.Name`.
- Pinning (`CommandExecutor.PinSchemaVersion`):
  ```csharp
  isStillValid: () => database.Schema.Tables.TryGetValue(table.Name /*captured*/, out var cur)
                      && cur.Id == table.Id
  currentVersion: () => table.Schema.Version
  ```
  So a **table** rename invalidates pinned txns automatically (old name no longer in `Tables`);
  a **column/index** rename keeps the table name, so it must **bump `TableSchema.Version`** to make
  the pin re-validate.

---

## G1 — `SchemaOp` values, payload, validation

- **Goal:** the wire vocabulary for renames.
- **Files:** `Catalogs/Models/SchemaOp.cs`, `Catalogs/Models/SchemaChangePayloads.cs`,
  `Catalogs/MetaJsonContext.cs` (register new payload/enum for source-gen JSON),
  `Commands/Validator/Validators/*`.
- **Change:**
  1. **Append** (never renumber — wire/JSON compatibility):
     ```
     RenameTable  = 7,
     RenameColumn = 8,
     RenameIndex  = 9,
     ```
  2. Add one discriminated payload (mirrors `SchemaElementKind` from SetElementState):
     ```csharp
     public enum SchemaRenameKind { Table, Column, Index }

     public sealed class SchemaRenamePayload
     {
         public string TableName { get; set; } = "";   // current table name (the table being touched)
         public SchemaRenameKind Kind { get; set; }
         public string? ElementName { get; set; }       // old column/index name; null for Table
         public string NewName { get; set; } = "";      // new table/column/index name
     }
     ```
     Register `SchemaRenamePayload` + `SchemaRenameKind` in `MetaJsonContext`.
  3. **Validation** (executor-level pre-check + the `ValidateSchemaDelta` dry-run both catch it):
     - target exists (table/column/index present under the old name);
     - `NewName` is a valid identifier and **not already taken** in its scope (table names in the
       db; column names in the table; index names in the table);
     - `NewName != old name` (reject or treat as no-op — recommend reject with a clear error);
     - cannot rename system objects (`~pk`, internal columns) — reuse existing guards.
- **Done when:** payloads round-trip through `Serializator`/`MetaJsonSerializer`; validation
  rejects duplicate/empty/unknown targets with typed `CamusDBException`.
- **Size:** S.

## G2 — Apply, `WasSchemaDeltaApplied`, `GetEntryTableName`

- **Goal:** every node applies a rename deterministically, in place, with correct pin/visibility
  invalidation and **no row/index rewrite**.
- **Files:** `Catalogs/CatalogsManager.cs` (dispatch + `ApplyRename*` + `WasSchemaDeltaApplied` +
  `GetEntryTableName`), `Catalogs/SchemaReplicator.cs` (descriptor eviction).
- **Change — `ApplySchemaDelta` dispatch** (add three cases) →
  - **`ApplyRenameTable`:** re-key `schema.Tables` (`Remove(old)`, `Add(new, sameInstance)`),
    set `tableSchema.Name = newName`. **Keep the same `TableSchema` instance** (pinning/visibility
    identity). No `TableSchema.Version` bump is required — the re-key already invalidates pins
    (old name gone) and row decode is `Id`-keyed. It still advances the database `SchemaVersion`
    chain like every delta.
  - **`ApplyRenameColumn`:** locate the column **by immutable `Id`** (resolve the `Id` from the old
    name first), replace the list entry with `new TableColumnSchema(id: same, name: NewName, type,
    notNull, default, state)`. **Bump `tableSchema.Version`** (so the pin re-validates and a query
    bound to the old name is rejected) and append a `SchemaHistory` entry for consistency with the
    other column ops. *(Note: because decode maps old rows to current columns by `Id`, history is
    belt-and-suspenders here — old rows decode regardless; bumping `Version` is the load-bearing
    part for pin/visibility.)*
  - **`ApplyRenameIndex`:** replace the `TableSchema.Indexes` entry with a renamed copy (same `Id`,
    `ColumnIds`, `Type`, `State`, `StartOffset`). **Do NOT bump `TableSchema.Version`** — indexes
    aren't part of row layout (consistent with `ApplyAddIndex`/`ApplyIndexElementState`).
- **`SchemaReplicator.InvalidateAppliedTableDescriptor`:** evict the cached `TableDescriptor` on
  **all three** rename ops so the next open rebuilds with the new name(s). For a table rename the
  descriptor is name-addressed, so eviction is mandatory.
- **`WasSchemaDeltaApplied`:** add predicates — applied iff the **new** name exists (and, for table
  rename, the old key is gone): table → `Tables.ContainsKey(NewName)`; column → table has a column
  named `NewName`; index → table has an index named `NewName`. Makes redelivery/restart-replay
  idempotent.
- **`GetEntryTableName`:** add `RenameTable/RenameColumn/RenameIndex => DecodePayload<SchemaRename
  Payload>(entry).TableName` (and for `RenameTable`, the entry's table name is the **old** name —
  keep it consistent with how the proposer built the entry, since this drives the ack-gate keying).
- **Done when:** a single-node and cluster apply leaves the object reachable only by its new name,
  the old name free, the same `TableSchema` instance preserved, and existing rows still decoding.
- **Size:** M.

## G3 — Draining the old name across the two-version window

- **Goal:** the freed old name cannot be reused before the cluster has converged, and in-flight
  work referencing the old name resolves sanely.
- **What's already given for free:**
  - The **two-version invariant** (`ReplicateAndWaitLocalApplyAsync` waits for every live node to
    ack before the next DDL) means a `rename A→B` is fully converged before any subsequent
    `create A` / `rename C→A` can propose. So name reuse is automatically gated — no extra draining
    machinery is needed; just make sure **G1 uniqueness validation runs against the post-rename
    schema** so reuse during the same window is rejected.
  - **In-flight reads:** a read-only autocommit `SELECT` holds its `TableDescriptor` snapshot and
    decodes against the pinned version, so it **completes** even though the name changed underneath
    (it doesn't run commit-time pin validation). An **explicit** transaction pinned to the renamed
    table/column **fails cleanly at commit** (`isStillValid`/version check) — the correct, existing
    behaviour for any concurrent DDL.
- **Change:** mostly *tests + a doc note*; confirm the uniqueness check in G1 consults the live
  (post-apply) schema, and that descriptor eviction (G2) prevents a stale descriptor from serving
  the old name after convergence.
- **Done when:** reusing the old name succeeds only after convergence; an autocommit read started
  before the rename completes; an explicit pinned txn fails with a typed error.
- **Size:** S–M (mostly tests).

## G4 — Executor entry points, SQL, forwarding, tests

- **Goal:** wire renames end-to-end (SQL → executor → replicated apply → converge), including
  follower forwarding, and prove it.
- **Files:** `Commands/Executor/CommandExecutor.cs`, new tickets under
  `Commands/Executor/Models/Tickets/` (`RenameTableTicket`; reuse `AlterTableTicket` for column,
  `AlterIndexTicket` for index), `Commands/Executor/ISchemaDdlForwarder.cs` +
  `HttpSchemaDdlForwarder.cs` + `App/Controllers/SchemaDdlForwardController.cs`,
  `SQLParser/*` grammar.
- **Change:**
  1. **Column rename** reuses the `AlterTable` path: handle `AlterTableOperation.RenameColumn` (the
     enum value already exists) → build a `RenameColumn` delta → `*ReplicatedAsync` when
     `!OwnsKahuna`, local apply otherwise. Forwarding already works via `ForwardAlterTableAsync`.
  2. **Table rename** needs a `RenameTableTicket` + `CommandExecutor.RenameTable(...)` +
     `ISchemaDdlForwarder.ForwardRenameTableAsync` + a controller case. **Index rename** adds a
     `RenameIndex` op to `AlterIndexOperation` + `CommandExecutor.AlterIndex` case +
     `ForwardAlterIndexAsync` (already exists) routing.
  3. Renames are **single deltas** (no coordinator/staging): route through `ExecuteDdlInTransaction`
     → `*ReplicatedAsync` → `ReplicateAndWaitLocalApplyAsync`. Carry a stable `operationId` for the
     C-path dedup, same as other DDL.
  4. **SQL grammar:** `ALTER TABLE t RENAME TO t2`, `ALTER TABLE t RENAME COLUMN c TO c2`,
     `ALTER INDEX i RENAME TO i2` (or the project's chosen syntax). Identifiers normalised to
     lowercase at parse time (existing behaviour).
- **Tests (G4):**
  - **Single-node round-trip** (each kind): rename → reopen → object persisted under the new name,
    old name absent, existing rows still decode by `Id` (`TestPersistentIndexSchema`-style for
    index; a `TestTableAlterer`-style for column/table).
  - **Cluster convergence** (in `CamusDB.Cluster.Tests`): rename on the leader → every node sees the
    new name without reopen; `SELECT`/`FORCE_INDEX` by the new name works on all nodes; old name
    rejected everywhere.
  - **Draining/concurrency:** an autocommit `SELECT` begun before the rename completes; an explicit
    pinned txn on the renamed object fails cleanly; reuse of the freed name succeeds after
    convergence and is rejected before.
  - **Forwarded rename:** issue the rename on a follower → forwarded to leader → converges (reuse
    the `FollowerForwardedDdlAppliesAndConvergesAcrossNodes` pattern).
  - **Atomicity under leadership change:** because a rename is a single delta, a forced leader change
    mid-rename leaves it either fully applied or not at all — assert no half-renamed state (no
    coordinator resume needed; contrast with the staged add/index flows).
- **Done when:** all three renames work single-node and converge cluster-wide by the new name with
  zero row/index rewrites; the cluster + unit suites are green.
- **Size:** M (column rename is small via the existing AlterTable path; table/index rename add the
  new ticket/forwarder/grammar surface).

---

## Invariants to preserve (don't break these)

1. **No row or index bytes move on a rename** — decode is `Id`-keyed; never rewrite data.
2. **Preserve the `TableSchema` instance on a table rename** (re-key the dict, mutate `.Name`) so
   pin/visibility closures keep observing the same object — consistent with the
   "apply mutates in place" invariant (architecture doc §9).
3. **Immutable `Id` is never regenerated** on rename — a column/index keeps its `Id`, only `Name`
   changes (replace-the-record pattern). This is what lets old rows keep decoding.
4. **Column rename bumps `TableSchema.Version`; index rename does not** (row-layout rule); table
   rename relies on the dict re-key for pin invalidation.
5. **Evict the cached `TableDescriptor`** on every rename so stale name-addressed descriptors don't
   serve the old name post-convergence.
6. **Name reuse is gated by the two-version invariant** — no bespoke draining machinery; just
   validate uniqueness against the live post-apply schema.

## Recommended order

`G1 (ops+payload+validation) → G2 (apply+idempotency+eviction) → G4-column (cheapest, via AlterTable)
→ G4-table/index (new tickets+grammar+forwarders) → G3/G4 tests → architecture-doc update`.

The architecture doc's "Renames" future-work bullet (§13) and the "renames are free" §7.4 narrative
should be updated to "implemented" once G lands.
