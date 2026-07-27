# Databases

## Lifecycle

Databases in CamusDB must be **created explicitly** before use. There is no magic-creation — opening or querying a name that has not been registered throws `DatabaseDoesntExist` (error code `CADB0010`).

```sql
CREATE DATABASE mydb;
```

The optional `IF NOT EXISTS` clause makes the statement a no-op when the database already exists:

```sql
CREATE DATABASE IF NOT EXISTS mydb;
```

Dropping a database removes its registry entry and, in cluster mode, purges all key-spaces from the shared node:

```sql
DROP DATABASE mydb;
DROP DATABASE IF EXISTS mydb;  -- no-op when absent
```

## Immutable identity

Every database is assigned an **immutable id** (a 24-character hex ObjectId, e.g. `6a1f2c3d4e5f000000000001`) at creation time. All storage keys — table rows, index entries, schema log entries, statistics — are prefixed with this id, not with the human-readable name.

The name is a display-only label. It is stored in the registry (`_system/dbregistry/db:{name}` in cluster mode, or a per-process map in standalone mode) and used only to look up the id. Once the id is resolved, the name plays no further role in routing or storage.

Consequences:

- The id directory (`{DataDirectory}/{id}/`) is never renamed or moved by any CamusDB operation.
- In cluster mode, all four key-space prefixes (`{id}/`, `{id}:`, `{tableId}:r/`, `{tableId}:i:{indexId}/`) are stable across renames.
- Two databases on the same cluster node have distinct id prefixes and never share key space, even if their tables have the same name.

## Rename

```sql
RENAME DATABASE old TO new;
ALTER DATABASE old RENAME TO new;   -- equivalent
```

Both spellings are accepted and produce the same operation; the `ALTER` form matches
`ALTER TABLE t RENAME TO …`. Neither requires a context database — the statement names its target —
and neither opens a transaction.

Rename is a **registry-only** operation. It swaps the `name→id` binding atomically (a single KV transaction) and leaves the id, the id-based directory, all Kahuna keys, table ids, and the cached descriptor untouched.

Guarantees:
- `OPEN(old)` throws `DatabaseDoesntExist` immediately after the rename completes.
- `OPEN(new)` resolves to the same id and returns the same cached descriptor.
- There is no window where neither name resolves — the registry swap is atomic.
- In-flight operations on the database continue uninterrupted; they observe the old display name until the descriptor is naturally recycled (harmless — the name is display-only).
- Renaming to an existing or reserved name throws `DatabaseAlreadyExists` or `DatabaseNameReserved`.

## Error codes

| Code | Constant | When thrown |
|------|----------|-------------|
| `CADB0010` | `DatabaseDoesntExist` | Open / query / DDL on an unregistered name |
| `CADB0012` | `DatabaseAlreadyExists` | `CREATE DATABASE` or `RENAME … TO` when the target name is already registered |
| `CADB0014` | `SystemSpaceCorrupt` | Database directory or `kv/` sub-directory is missing without a `creating.lock` sentinel |
| `CADB0018` | `DatabaseNameReserved` | `CREATE DATABASE` or `RENAME … TO` a reserved name (`_system`, `information_schema`) |
| `CADB0019` | `DatabaseCreationIncomplete` | Standalone: `creating.lock` found but `kv/` absent — the process crashed mid-creation; drop and recreate to recover |

## Reserved names

The following names are reserved and cannot be used for user databases:

- `_system` — internal registry and schema-replication namespace
- `information_schema` — reserved for future SQL compatibility
