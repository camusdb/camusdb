# Catalogs

Manages the schema (catalog) for all databases and tables.

`CatalogsManager` is the single authority for creating, altering, and dropping tables and indexes. It persists schema state as a log of `SchemaChangeLogEntry` operations in the Kahuna KV store and reconstructs in-memory `TableSchema` objects by replaying that log on open.

`SchemaReplicator` forwards DDL changes to follower nodes so every node in the cluster converges to the same schema without a separate schema-sync protocol.

Key types:

| Type | Purpose |
|------|---------|
| `TableSchema` | In-memory description of a table (columns, indexes) |
| `TableColumnSchema` | Column name, type, nullability |
| `TableIndexSchema` | Index name, type (unique/multi), covered columns |
| `SchemaChangeLogEntry` | Immutable record of one DDL operation applied to the schema |
| `SchemaCheckpoint` | Snapshot of the full schema at a point in time |
