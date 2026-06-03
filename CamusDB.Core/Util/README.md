# Util

General-purpose utilities shared across CamusDB.Core.

| Sub-package | Contents |
|-------------|---------|
| `ObjectIds/` | `ObjectIdValue` — a compact 96-bit identifier (three `int` fields). `ObjectId` converts it to/from a 24-character lowercase hex string. `ObjectIdGenerator` produces monotonically increasing IDs combining wall-clock time and a per-process counter. |
| `Time/` | `HLCTimestamp` — a Hybrid Logical Clock timestamp (wall time + counter) used to order events across nodes without requiring perfectly synchronized clocks. |
| `Hashes/` | `XXHash` — a fast 32-bit non-cryptographic hash (XXHash32) used for routing keys to Kahuna buckets. |
| `Comparers/` | `DescendingComparer<T>` — reverses any `IComparer<T>` for descending sorts in sorted collections. |
| `Diagnostics/` | `ValueStopwatch` — a zero-allocation stopwatch backed by `Stopwatch.GetTimestamp()`, used for internal latency measurements. |
