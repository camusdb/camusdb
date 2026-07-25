# Statistics

Collects, persists, and exposes per-table statistics for the cost-based optimizer.

`StatisticsManager` is the central registry. `TableStatistics` holds a table's persisted snapshot:
row count, per-index entry counts, per-column min/max, equi-depth histograms, per-column and
per-key-tuple NDV (number of distinct values), plus the auto-analyze staleness fields
(`MutationsSinceAnalyze`, `LastAnalyzedAt`). Stats are advisory — the planner uses them as hints and
never relies on them for correctness — and are persisted to Kahuna under `{dbId}:stats:{tableId}`.

## How stats are produced

- **DML tracking.** `TrackInsert` / `TrackUpdate` / `TrackDelete` keep row count, index entry counts,
  and min/max live as rows change (called from both the ticket-based and SQL DML paths), and
  accumulate the `MutationsSinceAnalyze` counter that drives staleness.
- **`ANALYZE TABLE`** (`TableAnalyzer`) rebuilds histograms and NDV from a scan. It counts exactly.
- **Automatic background ANALYZE** refreshes stale tables on their own via `TableAnalyzer.AnalyzeBackgroundAsync`
  and `AutoAnalyzeScheduler`, using bounded sketches so peak memory doesn't scale with table size:
  - `HyperLogLog.cs` — dependency-free approximate distinct-count sketch (NDV).
  - `ReservoirSampler.cs` — fixed-capacity uniform sample (histogram input).
  See [`docs/automatic-analyze.md`](../../docs/automatic-analyze.md) for the full design (staleness
  triggers, throttling, lock-free reads, atomic delta-safe publish, cluster ownership/fence, config).

## How stats are consumed

`CostEstimator` and `CardinalityEstimator` turn these into selectivity and row-count estimates for
access-path, join-algorithm, and join-order decisions. See [`docs/query-planner.md`](../../docs/query-planner.md).

## Key invariants

- Publication (`PublishAsync`) writes one complete generation in a single KV transaction and merges any
  DML committed during the scan, so persisted stats never end up a partial mix of old and new.
- Min/max is replaced by a fresh `ANALYZE` (to correct delete-drift); histograms/NDV are rebuilt
  wholesale; row/index counts are corrected by delta so concurrent writes survive.
