# Node stats: disk, CPU and memory of a node

> **Audience:** operators and monitoring systems that must know whether a CamusDB node can keep
> doing its job — mainly, whether its data disk is filling up.
> **Scope:** the node stats call over gRPC and REST, the fields it returns, how each value is
> measured, and the `MONITOR` privilege that lets a monitoring account read it.

A node answers one read-only call with the state of its disk volumes, CPU, and memory. A monitor can
call it once a minute. The call is cheap, and it does not depend on Prometheus
or on any other exporter.

- **gRPC:** `rpc GetNodeStats (NodeStatsRequest) returns (NodeStatsReply)` on the `CamusSql` service,
  next to `Ping`.
- **REST:** `GET /v1/admin/stats`.

Both transports return the same fields. The REST field names are the camel-case forms of the proto
field names (`available_bytes` becomes `availableBytes`).

In a cluster, the call answers for **the node that receives it**. To monitor a cluster, call each
node.

## 1. Calling it

This REST example is for a node with authentication on. It logs in first and then reads the stats
with the token.

```sh
TOKEN=$(curl -s -X POST https://db1:5095/login -H 'Content-Type: application/json' \
  -d '{"user":"monitoring","password":"..."}' | jq -r .token)

curl -s -H "Authorization: Bearer $TOKEN" https://db1:5095/v1/admin/stats
```

With gRPC, the .NET client (`CamusDB.Grpc.Client`) exposes the call as `GetNodeStatsAsync`.

```csharp
NodeStatsReply stats = await conn.GetNodeStatsAsync();
DiskVolumeStats data = stats.Disks[0];
bool full = data.BelowWatermark;
```

Any gRPC client can call `CamusSql/GetNodeStats` with the bearer token in the `authorization`
metadata, as for any other call. See [grpc-client-protocol.md](grpc-client-protocol.md).

## 2. Who may call it

The reply names filesystem paths and capacity. That is operational metadata. This means that the call
has a separate access rule.

| Authentication | Who may call it |
| -------------- | --------------- |
| Off | Anyone who can reach the port, as with the dashboard. |
| On | A superuser, or an account with the `MONITOR` privilege. |

Other accounts get `InsufficientPrivilege` (`CADB0517`): HTTP `403`, or gRPC `PERMISSION_DENIED`. A
request with a missing or invalid token gets `401` (gRPC `UNAUTHENTICATED`). The route is **not** on the
anonymous list next to `/ping` and `/health`.

A monitoring account that is granted `MONITOR` does not need the superuser attribute.

```sql
CREATE USER monitoring IDENTIFIED BY '...';
GRANT MONITOR ON *.* TO monitoring;
```

`MONITOR` is valid on `*.*` only. It is not part of `ALL PRIVILEGES`, and it gives no access to data.
An account whose only privilege is `MONITOR` cannot read a table, and it sees no database in `SHOW
DATABASES`. See [sql-authentication.md §4](sql-authentication.md#4-granting-privileges).

## 3. The reply

### The -1 rule

All sizes are in bytes, and all values are numbers. A value that the node cannot read is `-1`. It is
never reported as `0`. Each `-1` value has an entry in `errors` that names the field and gives the
reason. One failed probe never fails the call.

```json
"errors": [
  { "field": "memory.gc_committed_bytes", "message": "no garbage collection has completed yet" }
]
```

The `field` value is the proto path of the value, for example `cpu.load_average_1m` or
`disks[wal].engine_bytes`.

### `node`

| Field | Meaning |
| ----- | ------- |
| `node_id` | The node's Raft node id. Empty early in boot. |
| `endpoint` | The node's Raft endpoint (`host:port`). Empty early in boot. |
| `version` | The server's version, with the build commit. |
| `cluster_mode` | `true` for a cluster node, `false` for a standalone node. |
| `uptime_seconds` | Seconds since the process started, as an integer. |
| `sampled_at_unix_ms` | When the node built the reply, in Unix epoch milliseconds (UTC). |

### `disks`

The first entry is always the volume of `data_dir` (role `data`). A second entry (role `wal`) is
present if the Raft WAL directory (`{data_dir}/wal`) is on a different volume, for example when you
mount a separate disk there. Otherwise, there is no second entry.

| Field | Meaning |
| ----- | ------- |
| `role` | `data` or `wal`. |
| `path` | The directory that this entry measures. |
| `mount_point` | The mount point of `path`. Empty until the first storage measurement (§4). |
| `total_bytes` | Size of the volume. |
| `free_bytes` | Free space of the volume. |
| `available_bytes` | Free space that the server's user can write. This value is smaller than `free_bytes` when the file system has reserved blocks. |
| `used_bytes` | `total_bytes` minus `free_bytes`. |
| `min_free_disk_bytes` | The write-admission watermark for this volume. A `wal` entry shows `0`. The gate does not watch that volume. |
| `below_watermark` | `true` while writes on the node are blocked because this volume is too full. Always `false` on a `wal` entry. |
| `engine_bytes` | Bytes of the storage engine's own files on this volume: the key/value store, plus the WAL when it is on the same volume. `0` for an in-memory engine. |

**`below_watermark` is the write gate's own answer.** When `available_bytes` on the data volume falls
below `min_free_disk_bytes` (default 64 MiB), new writes return
`InsufficientDiskSpace` (`CADB0536`). Reads, DDL and `DROP` continue to work. This means that you can
free space. The
stats call reads the same cached probe and the same live watermark that the gate reads. So when
`below_watermark` is `true`, writes on that node fail, and when it is `false`, the gate admits them.
A change of `min_free_disk_bytes` (for example with `SET CLUSTER SETTING`) shows in the next call.

A useful alert is earlier than the gate: for example, `available_bytes < 4 × min_free_disk_bytes`.
Alert thresholds and history belong to the caller.

### `cpu`

| Field | Meaning |
| ----- | ------- |
| `process_cpu_percent` | CPU of the server process, as a share of all `processor_count` processors of the node, from 0 to 100. |
| `process_cpu_window_ms` | The time window of `process_cpu_percent`. Normally 60000 (one minute). See §4. |
| `processor_count` | Processors as the .NET runtime sees them, with container CPU limits applied. |
| `load_average_1m`, `load_average_5m`, `load_average_15m` | The host's load averages on Linux and macOS. In a container, they describe the host. They do not describe the container. `-1` on other systems. |

### `memory`

| Field | Meaning |
| ----- | ------- |
| `process_working_set_bytes` | Resident memory of the server process. |
| `gc_heap_bytes` | Size of the managed heap at the last garbage collection. Before the first collection, the bytes allocated until now. |
| `gc_committed_bytes` | Memory that is committed by the garbage collector at the last collection. This value can stay much higher than `gc_heap_bytes` after a load peak. `-1` until the first collection completes. |
| `gc_memory_limit_bytes` | The managed heap limit that is in force: `DOTNET_GCHeapHardLimit`, the bytes that `DOTNET_GCHeapHardLimitPercent` gives, or the runtime's default in a memory-limited container (75% of the limit). **`0` means that no limit is in force.** The heap can then grow until the host runs out of memory. |
| `host_total_bytes` | The memory that the node can use: the cgroup memory limit when one applies, else the host's physical memory. |
| `host_available_bytes` | The part of `host_total_bytes` that is available now. See §4. |

`gc_memory_limit_bytes` tells "high and limited" from "high and unbounded". A node with a high
`process_working_set_bytes` and a `gc_memory_limit_bytes` of `0` relies on the operating system to
stop it. Set a container memory limit or `DOTNET_GCHeapHardLimitPercent` for that node.

## 4. How each value is measured

The call does not probe, scan or sleep on the request path. One call costs about a microsecond of
server time when the caches are warm.

| Values | Source | Age |
| ------ | ------ | --- |
| `disks[*]` sizes, `below_watermark` | One `statfs`-class probe per volume. The data volume shares the cache of the write gate. | Not more than 2 s. |
| `process_cpu_percent` | A background sampler records the process CPU time at a 5 s interval. The value is the average over the last minute of samples. | Not more than 5 s. |
| Load averages, `host_*`, `gc_memory_limit_bytes` | The same background sampler, at a 5 s interval. | Not more than 5 s. |
| `engine_bytes`, `mount_point`, WAL volume detection | The background sampler scans the engine's directories and reads the mount table, at a 60 s interval. | Not more than 60 s. |
| `process_working_set_bytes`, `gc_*` heap values, `processor_count` | Values maintained by the .NET runtime. | Live. |

Some values are not available just after the node starts.

- The sampler takes its first sample when the node starts. Until it has a second sample, 5 s later,
  `process_cpu_percent` is the average over the process's lifetime so far, and
  `process_cpu_window_ms` shows that lifetime.
- The window grows to one minute during the first minute.

**Host memory.**

- On Linux, if no cgroup limit applies, the node reads `MemTotal` and `MemAvailable` from
  `/proc/meminfo`.
- With a cgroup memory limit (v2 `memory.max`, or v1 `memory.limit_in_bytes`), `host_total_bytes` is
  that limit.
- In that case, `host_available_bytes` is the limit minus the cgroup's working set. The working set is
  the usage less the inactive file cache, the same calculation as `docker stats`.
- If the host's `MemAvailable` is lower, `host_available_bytes` shows `MemAvailable`.
- On macOS, the node reads `hw.memsize` and `kern.memorystatus_level`, with a resolution of 1% of
  physical memory.

**Mount points.** The node resolves symbolic links in `data_dir` and in the WAL directory. Then it
finds the longest mount point that contains each path. On macOS, the APFS data volume is joined to
`/` by firmlinks. This means that a path under `/Users` shows `/` as its mount point. The sizes are
correct in that case. This is because the probe reads the volume where the path is stored.

## 5. Related documents

- The `MONITOR` privilege and the other grants: [sql-authentication.md](sql-authentication.md)
- The gRPC contract: [grpc-client-protocol.md](grpc-client-protocol.md) and
  [`camus_sql.proto`](../CamusDB.Grpc.Contracts/Protos/camus_sql.proto)
- The .NET client: [grpc-dotnet-client.md](grpc-dotnet-client.md)
- The browser dashboard, which shows a different view of the same node:
  [operator-dashboard.md](operator-dashboard.md)
- The same watermark applied to query spill files: [spill-to-disk.md](spill-to-disk.md)
