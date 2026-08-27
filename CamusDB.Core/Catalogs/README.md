# Catalogs

Owns the schema (catalog) of every database: its tables, views, indexes, constraints and comments.

`CatalogsManager` is the **entry point and nothing more** — every member is a one-line delegation.
Callers across the engine hold one, which is why it stays stable while the work behind it is divided
by responsibility.

## Layout

| Path | Type | Owns |
|------|------|------|
| `.` | `CatalogsManager` | the facade; delegation only |
| `.` | `RelationCatalog` | create / alter / drop / rename / relink / truncate a relation |
| `.` | `ViewCatalog` | views and materialized-view state |
| `.` | `TableCommentWriter` | COMMENT ON, single-node path |
| `.` | `SchemaReplicator` | bridges Kahuna's apply and restore callbacks to the catalog |
| `.` | `SchemaChangeCoordinator` | drives a staged element change across its states |
| `Replication/` | `SchemaChangeEntryFactory` | builds every `SchemaChangeLogEntry` |
| `Replication/` | `SchemaChangePublisher` | the Raft round-trip, the apply wait, both ack gates |
| `Replication/` | `SchemaElementReplicator` | column, index, constraint, settings, comment deltas |
| `Apply/` | `SchemaDeltaApplier` | dispatch, payload decoding, idempotency predicates |
| `Apply/` | `TableDeltaApplier`, `ColumnDeltaApplier`, `IndexDeltaApplier`, `ViewDeltaApplier`, `ConstraintDeltaApplier`, `ElementStateApplier` | one delta family each |
| `Meta/` | `MetaKeys` | every metadata key; the routing invariant lives here |
| `Meta/` | `MetaKeyWriter`, `SchemaMetaStore` | KV input/output, raw and typed |
| `Meta/` | `SchemaLoader`, `SchemaHistoryStore` | the open path and lazy schema history |
| `Meta/` | `SchemaCheckpointWriter` | the durable checkpoint written after a commit |
| `Meta/` | `OrphanTableStore`, `CoordinatorJobStore`, `MaterializedViewRefreshJobStore`, `ContentsRetirementStore`, `BranchMetaCopier` | the per-object record families |

## Three rules worth knowing before you edit here

**Apply must never persist.** A committed delta is applied to in-memory schema inside the schema
partition's commit pipeline, on every node. A KV write from there re-enters the same partition and
deadlocks it. The proposer persists the checkpoint afterwards. No type under `Apply/` takes an
`IKahuna` or a `KvTransaction`, so this is a compiler error rather than a review comment.

**Never replicate while holding the schema lock.** Replication re-enters that same pipeline, which
yields on the lock. Build and validate the delta under the lock, release it, then publish. Several
methods assert `Schema.LockDepth == 0` for exactly this reason.

**Metadata keys separate their sub-fields with `:`, never `/`.** Kahuna routes by the substring
before the last `/`, so every metadata key must keep `{dbId}/meta` as that prefix. A `/` in a
sub-field scatters the family across partitions, where the single scan the load path and the
database purge both perform can no longer reach it. The data is not lost, it is invisible.
`MetaKeys` states this, and `TestMetaKeys` enforces it for every family.

## Key types

| Type | Purpose |
|------|---------|
| `TableSchema` | in-memory description of a relation (columns, indexes, constraints, settings) |
| `TableColumnSchema` | column name, type, nullability |
| `TableIndexSchema` | index name, type, covered columns, staged state |
| `SchemaChangeLogEntry` | one DDL operation, as replicated through Raft |
| `SchemaCheckpoint` | a snapshot of the schema at a point in time |
