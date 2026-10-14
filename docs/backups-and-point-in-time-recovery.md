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
   image. Keep the same `kahuna.storage`/revision settings the backup was taken with. A backup
   is bound to the storage revision it was taken under: a server on a newer revision opens a
   different directory and does not read it. To carry a backup across revisions, restore it with
   the CamusDB version that took it and then dump it logically — see
   [logical-dump-and-reimport.md](logical-dump-and-reimport.md).
4. **(Cluster only)** whole-cluster disaster recovery restores every node to one coordinated point and
   brings the cluster back up. To add a node to a **running** cluster from a backup, use the seeded
   join below: a restored data root started alone does not keep its data.

## Seeding a joining node from a backup

A node that joins a running cluster (`join_existing: true`) with an empty WAL presents to the leader
at index 0. The leader then sends it a full snapshot, and that snapshot overwrites the restored store,
so a restored data root alone saves nothing. The seeded join fixes this. Before the node joins, it
replays the backup chain into its store and writes one committed checkpoint per partition into its
WAL at the index the chain reached. The leader then sends only the entries after that index.

1. **Restore the chain into a fresh data root** with `POST /v1/restore` on any node, as in step 1
   above. Note the `leafBackupId` and the `targetTimeMs` you used.
2. **Copy the data root to the new host** and use it as the new node's `data_dir`. Do not start a
   node on it before the next step: its WAL must stay empty.
3. **Make the backup catalog readable on the new host** and set `kahuna.backup_dir` to it. The seed
   reads the chain from there, not from the data root.
4. **Start the node with the seed options:**

   ```yml
   mode: cluster
   peers: [10.0.0.1:7070, 10.0.0.2:7070]   # seeds: members of the running cluster
   join_existing: true
   pitr_bootstrap_from: <leafBackupId>
   pitr_bootstrap_target_time_ms: 0         # the targetTimeMs of the restore; 0 = latest
   kahuna:
     backup_dir: /opt/camusdb/backups
   ```

   or `--join-existing true --pitr-bootstrap-from <leafBackupId>` on the command line.
5. **Remove `pitr_bootstrap_from`** from the configuration after the node is up.

The node refuses to start, with `PitrBootstrapFailed` (`CADB0710`) and a message that names the cause,
when one of these conditions is true:

- `data_dir` does not hold the restored image (`{data_dir}/kv/{revision}` is missing or empty). The
  replay builds on the full backup's image and does not install it.
- The WAL (`{data_dir}/wal/{revision}`) is not empty. The node ran before, or an earlier seed stopped
  part way. Copy a fresh data root into place, or remove `pitr_bootstrap_from` to start the node as it is.
- The store or the WAL is not RocksDB.
- A manifest in the chain fails authentication: `kahuna.backup_mac_key_file` is set and the manifest
  is unsigned or signed with another key. The seed checks manifests exactly as a restore does.
- The chain belongs to another cluster: a manifest carries a `kahuna.backup_cluster_id` that differs
  from this node's. The seeded indexes would describe the other cluster's log. A chain with no
  cluster id (taken before the id was set) is accepted.
- The chain cannot be resolved, or the target is outside the chain's coverage or older than the
  leader's retention window (`pitr_window_seconds` plus `base_snapshot_interval_seconds`). Past that
  window the leader no longer holds the entries after the seeded index, so it sends a snapshot anyway.
- An earlier seed attempt in the same process failed.

`pitr_bootstrap_target_time_ms` must match the `targetTimeMs` of the restore. With a different value,
the store and the WAL describe two different points in time.

## Notes and limits

- **Coordinated vs full on the embedded node.** For a single embedded node the two are effectively
  equivalent; coordinated matters for real multi-node clusters, where it takes one consistent cut
  across all partitions.
- **Incremental fallback.** If an incremental's parent has aged past the retention floor, Kahuna cannot
  produce a contiguous increment; take a fresh full backup instead.
- **Retention cost.** Retained WAL is roughly `pitr_window_seconds × write throughput`, plus the base
  images overlapping the window. Choose the window from how far back you realistically need to recover.
- **Local filesystem only** in this version — there is no object-storage backup target yet.
- **Large values are included.** A compressed or out-of-line column value (see
  [Large values](storage-layout.md)) is ordinary data in the store. A backup captures the whole store
  and the WAL, not a list of key spaces, so it needs no extra step, and a restore to any point reads
  each row with the values it pointed at then.
