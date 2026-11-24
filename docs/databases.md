# Databases

## Lifecycle

Databases in CamusDB must be **created explicitly** before use. Opening or querying a name that has not been registered throws `DatabaseDoesntExist` (error code `CADB0010`).

```sql
CREATE DATABASE mydb;
```

With the optional `IF NOT EXISTS` clause, the statement does nothing when the database already exists.

```sql
CREATE DATABASE IF NOT EXISTS mydb;
```

Dropping a database removes its registry entry. In cluster mode, it also purges all key-spaces of the database from the shared node.

```sql
DROP DATABASE mydb;
DROP DATABASE IF EXISTS mydb;  -- no-op when absent
```

## Immutable identity

Each database is assigned an **immutable id** (a 24-character hex ObjectId, e.g. `6a1f2c3d4e5f000000000001`) at creation time. All storage keys, such as table rows and index entries, are prefixed with this id. The human-readable name is not part of any storage key.

The name is a display-only label. It is stored in the registry (`_system/dbregistry/db:{name}` in cluster mode, or a per-process map in standalone mode) and used only to look up the id. Once the id is resolved, the name plays no further role in routing or storage.

Because storage keys use the id, the on-disk and key-space layout does not depend on the database name.

- No CamusDB operation renames or moves the id directory (`{DataDirectory}/{id}/`).
- In cluster mode, all four key-space prefixes (`{id}/`, `{id}:`, `{id}:{tableId}|r/`, `{id}:{tableId}|i:{indexId}/`) are stable across renames.
- Two databases on the same cluster node have distinct id prefixes. Their key spaces are separate, even if their tables have the same name.

## Rename

```sql
RENAME DATABASE old TO new;
ALTER DATABASE old RENAME TO new;   -- equivalent
```

Both spellings are accepted and produce the same operation; the `ALTER` form matches
`ALTER TABLE t RENAME TO …`. Neither requires a context database — the statement names its target —
and neither opens a transaction.

Rename is a **registry-only** operation. It swaps the `name→id` binding atomically in a single KV transaction. The id, the id-based directory, the Kahuna keys, the table ids and the cached descriptor are left untouched.

A completed rename gives the following guarantees.

- `OPEN(old)` throws `DatabaseDoesntExist` immediately after the rename completes.
- `OPEN(new)` resolves to the same id and returns the same cached descriptor.
- There is no window where neither name resolves — the registry swap is atomic.
- In-flight operations on the database continue uninterrupted; they observe the old display name until the descriptor is recycled. This is harmless. The name is display-only.
- Renaming to an existing or reserved name throws `DatabaseAlreadyExists` or `DatabaseNameReserved`.

## Error codes

| Code | Constant | When thrown |
|------|----------|-------------|
| `CADB0010` | `DatabaseDoesntExist` | Open / query / DDL on an unregistered name, or, with authentication on, on a database on which the caller has no grant. HTTP 404, gRPC `NotFound` |
| `CADB0012` | `DatabaseAlreadyExists` | `CREATE DATABASE` or `RENAME … TO` when the target name is already registered |
| `CADB0014` | `SystemSpaceCorrupt` | Database directory or `kv/` sub-directory is missing, and no `creating.lock` sentinel is present |
| `CADB0018` | `DatabaseNameReserved` | `CREATE DATABASE` or `RENAME … TO` a reserved name (`_system`, `information_schema`) |
| `CADB0019` | `DatabaseCreationIncomplete` | Standalone: `creating.lock` found but `kv/` absent — the process crashed mid-creation; drop and recreate to recover |

## Reserved names

User databases cannot use these reserved names.

- `_system` — internal registry and schema-replication namespace
- `information_schema` — reserved for future SQL compatibility
