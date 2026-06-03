# Storage

KV-backed data access layer built on top of [Kahuna](https://github.com/kahunakv/kahuna).

## Key layout

All keys for a table share a leading `{tableId}` segment so Kommander routes the whole table to one partition:

```
Primary rows:      {tableId}:r/{rowIdHex24}
Unique index:      {tableId}:i:{indexId}/{encodedKey}
Non-unique index:  {tableId}:i:{indexId}/{encodedKey}{rowIdHex24}
```

## Main types

| Type | Purpose |
|------|---------|
| `KvTableStore` | Per-table data access — insert/update/delete rows and index entries, range scans |
| `KeyEncoder` | Encodes typed column values into a comparable byte sequence for index keys |
| `RowEncoder` | Encodes and decodes a full table row to/from a `byte[]` using `Serializator` |
| `EmbeddedKahuna` | Bootstraps the in-process Kahuna node and exposes the `IKahuna` interface |
| `ISchemaReplicationForwarder` | Contract for forwarding DDL changes to follower nodes |

All write methods on `KvTableStore` accept a `KvTransaction` so they accumulate acquired locks and modified keys for a 2-phase commit.
