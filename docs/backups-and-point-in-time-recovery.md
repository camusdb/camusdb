# Backups and point-in-time recovery

CamusDB exposes the backup and point-in-time-recovery (PITR) machinery of its embedded Kahuna storage
engine through a small server-admin HTTP API. Because every CamusDB database lives in **one** shared
Kahuna node, a backup is **node-wide** — it captures all databases at once — and these are server-level
operations, not database-scoped ones.

For the underlying model (base images, WAL replay, retention windows, coordinated cluster cuts, and why
the design is safe) see the Kahuna guide `backups-and-point-in-time-recovery-guide.md` in the Kahuna
repository. This document covers only how to use it from CamusDB.

## Enabling backups

Backups are **off by default**. Set a backup directory in the `kahuna:` block of `config.yml` (or via
the equivalent CLI/env override) and restart the server:

```yml
kahuna:
  backup_dir: /opt/camusdb/backups        # required to enable backups; must not be blank
  pitr_window_seconds: 3600               # how far back a restore may target; > 0 and <= 21600 (6h). Default 3600 (1h)
  base_snapshot_interval_seconds: 1800    # base-image cadence; > 0 and <= pitr_window_seconds. Default 1800 (30m)
```

When `backup_dir` is unset, every endpoint below returns **HTTP 503** (`BackupNotConfigured`). The same
directory holds both the catalog manifests and the per-backup artifacts.

**Restore is disabled by default.** `POST /v1/restore` returns **HTTP 403** (`RemoteRestoreDisabled`)
until you set `restore_root` to a server-owned directory that all restore targets must live under
(destinations are confined to it). `allow_unconfined_remote_restore: true` lifts the confinement and is
insecure — avoid it in production.

**Retention/GC is automatic.** Old backups are garbage-collected after each backup and on a periodic
tick per `backup_retention_max_chains` / `_max_age_seconds` / `_max_bytes`; GC deletes only whole chains
(always leaving a valid full root for every retained leaf). `POST /v1/backups/gc?dryRun=true` previews
what would be reclaimed; `dryRun=false` runs it on demand.

## Authorization

When authentication is enabled, all backup and restore endpoints require a **superuser** bearer token
(the same bar as user administration). Present it as `Authorization: Bearer <token>`, and use HTTPS —
a token over plaintext is refused when TLS is required. When authentication is disabled, the endpoints
are open (consistent with the rest of the engine).

## HTTP API

All endpoints are versioned under `/v1`. Requests and responses are JSON with camelCase fields.

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/v1/backups/full` | — | Take a full backup now. |
| `POST` | `/v1/backups/incremental` | `{ "parentBackupId": "<guid>" }` | Take an incremental backup on top of a parent. |
| `POST` | `/v1/backups/coordinated` | — | Take a cluster-wide coordinated full backup (production recommendation for clusters). |
| `GET`  | `/v1/backups` | — | List all backups in the catalog. |
| `GET`  | `/v1/backups/{id}/chain` | — | Resolve and validate the chain ending at `{id}` (root-first); the head carries `minRecoverablePhysicalMs`/`maxRecoverablePhysicalMs`. |
| `POST` | `/v1/backups/gc?dryRun=<bool>` | — | Preview (`dryRun=true`) or run backup retention + orphan sweep. |
| `POST` | `/v1/restore` | `{ "leafBackupId": "<guid>", "targetDir": "/abs/data-root", "targetTimeMs": 0 }` | Offline restore into a fresh CamusDB **data root**. |

`targetTimeMs` is Unix epoch **milliseconds**; `0` means "latest recoverable point in the chain". A
non-zero value must fall inside the selected chain's **exact recoverable coverage**
(`minRecoverablePhysicalMs`..`maxRecoverablePhysicalMs`, reported by the chain/restore responses) —
otherwise it is rejected with `RestorePointOutOfWindow` (HTTP 422). Recoverability is a property of the
chain, **not** of how much wall-clock time has passed, so an archived backup stays restorable.

Backups verify every artifact (size + SHA-256) before publish and before restore and fail closed on a
missing/corrupt/extra file. An incremental that can no longer be based on its parent is transparently
taken as a full instead — the response then carries `requestedKind`/`actualKind`/`substitutionReason`
so the substitution is visible rather than silent.

Error codes surface as JSON `{ "status": "failed", "code": "...", "message": "..." }` with a matching
HTTP status: `BackupNotConfigured` → 503, `RemoteRestoreDisabled` → 403, `BackupChainInvalid` → 422,
`BackupCorruptArtifact` → 422, `RestoreTargetConflict` → 409, `RestorePointOutOfWindow` → 422,
`BackupNeedsFullBackup` → 409, `BackupExactCheckpointUnavailable` → 409, `InsufficientPrivilege` → 403,
`BackupTopologyChanged` → 503, `BackupNotCoordinator` → 421, `BackupInsecureRoot` → 500.

### Cluster, consistency, and authenticity

- **Consistency.** A coordinated backup (`/v1/backups/coordinated`) is a single consistent HLC cut across
  all partitions; a cross-partition transaction cannot be torn (both capture and restore cut on the
  shared commit HLC, not per-shard WAL time). Issue coordinated backups on the **coordinator** node — a
  non-coordinator returns `BackupNotCoordinator` (421); if the topology changes mid-backup it aborts with
  `BackupTopologyChanged` (503) and publishes nothing, so retry once stable.
- **Cluster identity.** Set `kahuna.backup_cluster_id` identically on every node. Manifests carry it (and
  the coordinator node), and a restore refuses to chain artifacts from a different cluster or a stale
  topology. Listings surface `clusterId` / `coordinatorNode`.
- **Authenticity.** Set `kahuna.backup_mac_key_file` (the same HMAC-SHA-256 key file on every node, kept
  outside `backup_dir`) to sign manifests; a node with a key then refuses an unsigned or tampered
  manifest. The backup/restore root must be owner-only (0700) and not a symlink, or operations fail with
  `BackupInsecureRoot`.
- **Confidentiality.** There is **no encryption at rest** — artifacts are plaintext protected by
  filesystem permissions and the integrity MAC. Keep `backup_dir` on an access-controlled, ideally
  encrypted, volume.

### Examples

```sh
# Take a full backup
curl -sX POST http://localhost:5000/v1/backups/full -H "Authorization: Bearer $TOKEN"

# Take an incremental on top of it
curl -sX POST http://localhost:5000/v1/backups/incremental \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"parentBackupId":"<full-guid>"}'

# List and inspect a chain
curl -s http://localhost:5000/v1/backups -H "Authorization: Bearer $TOKEN"
curl -s http://localhost:5000/v1/backups/<leaf-guid>/chain -H "Authorization: Bearer $TOKEN"

# Restore to a point in time into a fresh directory
curl -sX POST http://localhost:5000/v1/restore \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"leafBackupId":"<leaf-guid>","targetDir":"/data/restored","targetTimeMs":1750000000000}'
```

## Restore is offline — the runbook

Taking backups (full/incremental/coordinated), listing, and chain validation are **online** and safe
while the server serves traffic. **Restore is offline**: it rebuilds the data into a *fresh* directory
and does **not** touch the running server's storage. There is no hot in-place restore. To use a
restored image:

1. **Restore into a fresh data root.** Call `POST /v1/restore` with a `targetDir` that is a new,
   empty/absent directory and is **not** the live `data_dir` (nor its `kv`/`wal` subdirectories — the
   request is rejected otherwise). The running server is unaffected. CamusDB lays out the restored
   storage under `{targetDir}/kv` and creates an empty `{targetDir}/wal`, so `targetDir` is a complete,
   bootable CamusDB data directory. The response echoes it back as `dataRoot`.
2. **Stop the CamusDB server.**
3. **Start a fresh server with `data_dir = dataRoot`.** No manual file moves are needed — the node's
   storage (`{dataRoot}/kv/{revision}`) and WAL (`{dataRoot}/wal`) resolve directly to the restored
   image. Keep the same `kahuna.storage`/revision settings the backup was taken with.
4. **(Cluster only)** the restored node holds data as of the restore point; it is admitted to
   membership and caught up by normal Raft replication. Whole-cluster disaster recovery restores every
   node to one coordinated point and brings the cluster back up.

## Notes and limits

- **Coordinated vs full on the embedded node.** For a single embedded node the two are effectively
  equivalent; coordinated matters for real multi-node clusters, where it takes one consistent cut
  across all partitions.
- **Incremental fallback.** If an incremental's parent has aged past the retention floor, Kahuna cannot
  produce a contiguous increment; take a fresh full backup instead.
- **Retention cost.** Retained WAL is roughly `pitr_window_seconds × write throughput`, plus the base
  images overlapping the window. Choose the window from how far back you realistically need to recover.
- **Local filesystem only** in this version — there is no object-storage backup target yet.
