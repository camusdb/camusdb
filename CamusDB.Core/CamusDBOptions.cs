/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Core;

/// <summary>
/// The complete, immutable configuration of one engine instance: every value that used to live as a
/// mutable process-wide static now lives here, on an object owned by whoever composed the engine
/// (the host at startup, or a test in its set-up).
///
/// <para>Immutability is the point. A value read at the start of a query cannot change underneath it
/// mid-execution, and two engines configured differently can run side by side in one process — which
/// a shared mutable static made impossible, and which is what lets independently-configured tests run
/// concurrently instead of serializing on global state.</para>
///
/// <para>Override with a <c>with</c> expression rather than assignment:
/// <c>CamusDBOptions.Default with { SpillEnabled = true }</c>. Values that are genuinely fixed (the
/// primary-key index name, hard format limits) are not here — they are compile-time constants on
/// <see cref="CamusDBConstants"/>.</para>
/// </summary>
public sealed record CamusDBOptions
{
    /// <summary>Built-in defaults, used where no configuration file or override applies.</summary>
    public static readonly CamusDBOptions Default = new();

    /// <summary>
    /// The directory where the database files and directories will be stored. Because it is
    /// per-instance, two engines in one process can hold separate data directories — which is how a
    /// test gives itself an isolated directory without an ambient override.
    ///
    /// <para>The default is computed inline rather than read from a static field: a static field
    /// declared after <see cref="Default"/> would still be null while <see cref="Default"/>'s own
    /// initializer runs, leaving the shared defaults with a null data directory.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public string DataDirectory { get; init; } = Path.GetFullPath("Data");

    /// <summary>
    /// Minimum interval, in milliseconds, between background flushes of advisory table
    /// statistics to durable Kahuna storage, per table. Statistics are updated in
    /// memory on every DML but only persisted at most once per this interval, so a write
    /// burst produces a single disk write rather than one per row.
    ///
    /// Special values:
    ///   <c>0</c>  — flush as soon as possible after each change (overlapping flushes are
    ///              still coalesced); highest durability, highest write amplification.
    ///   <c>-1</c> — never auto-flush; statistics are persisted only by an explicit
    ///              <c>FlushAsync</c> (e.g. on database close). Lowest write amplification,
    ///              but in-memory deltas are lost on a crash.
    /// Any positive value caps flush frequency to roughly once per interval per table.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int StatsFlushIntervalMs { get; init; } = 5000;

    /// <summary>
    /// Row count threshold below which ANALYZE performs a full scan; above it, rows are
    /// sampled by reading the first N rows in storage order. 0 = always full scan.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int StatsAnalyzeSampleRows { get; init; } = 100_000;

    /// <summary>
    /// Number of equi-depth histogram buckets ANALYZE builds per column.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int StatsHistogramBuckets { get; init; } = 100;

    // ── Automatic (background) ANALYZE ────────────────────────────────────────────────────────
    // Auto-analyze keeps optimizer statistics fresh without a user running ANALYZE, while staying
    // low-priority: it reads a lock-free snapshot (never blocks/aborts foreground work), bounds
    // peak memory with sampling, throttles its scan rate, and backs off under load.

    /// <summary>
    /// Master switch for automatic background <c>ANALYZE</c>. On by default: stale statistics make the
    /// cost-based planner choose badly, and requiring an operator to schedule <c>ANALYZE</c> by hand is
    /// the common way that happens. Set false to restore manual-only ANALYZE — the scheduler then never
    /// runs and nothing else changes.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public bool AutoAnalyzeEnabled { get; init; } = true;

    /// <summary>
    /// Interval between auto-analyze staleness sweeps, in milliseconds. Only the schema/registry
    /// leader sweeps, so a table is analyzed once per cluster. <c>&lt;= 0</c> also disables the loop.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int AutoAnalyzeCheckIntervalMs { get; init; } = 60_000;

    /// <summary>
    /// Staleness fraction (CockroachDB's <c>fraction_stale_rows</c>): a table is stale once its
    /// mutations since the last ANALYZE reach <c>fraction · rowCount + minStaleRows</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public double AutoAnalyzeFractionStaleRows { get; init; } = 0.20;

    /// <summary>
    /// Absolute floor of mutations before a table is ever considered stale (CockroachDB's
    /// <c>min_stale_rows</c>) — stops tiny tables and light churn from re-analyzing constantly.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public long AutoAnalyzeMinStaleRows { get; init; } = 500;

    /// <summary>Maximum background analyses running at once on this node. Keep small (1–2).</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int AutoAnalyzeMaxConcurrent { get; init; } = 1;

    /// <summary>
    /// Scan-rate cap for a background analyze, in rows/second — the primary CPU/IO throttle that
    /// keeps a background scan from saturating a core or the KV read path. <c>&lt;= 0</c> disables throttling.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int AutoAnalyzeMaxRowsPerSecond { get; init; } = 50_000;

    /// <summary>
    /// Reservoir sample size per orderable column for background histograms — the memory bound.
    /// Peak memory is a function of this, not table size.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int AutoAnalyzeHistogramSampleRows { get; init; } = 10_000;

    /// <summary>
    /// HyperLogLog precision (index bits) for background NDV sketches; register count is
    /// <c>2^precision</c>. 11 ⇒ ~2 KB/column, ~2.3% error. Valid range 4..16.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int AutoAnalyzeHllPrecision { get; init; } = 11;

    /// <summary>
    /// Foreground-load surge protector: when the number of in-flight foreground transactions
    /// exceeds this, the scheduler skips starting (and cancels a running) background analyze this
    /// sweep and retries when the node is quieter. <c>&lt;= 0</c> disables load-based backoff.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int AutoAnalyzeLoadPauseThreshold { get; init; } = 16;

    /// <summary>
    /// How many rows the background scan processes between successive mid-scan re-checks of ownership
    /// (leadership) and foreground load. Smaller reacts faster to a lost lease or a load surge but adds
    /// per-batch overhead; larger amortizes the (async) leadership probe. Clamped to at least 1.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int AutoAnalyzeOwnershipCheckRows { get; init; } = 1000;

    // Row-level TTL. Per-table tuning (which column expires, how often, batch sizes, rate limits)
    // lives on the table as storage parameters, because the right rate for a 10M-row session table
    // is not the right rate for a small one and a node-global cap would force one table's setting
    // onto every other. The TtlDefault* options below supply the fallback a table that sets nothing
    // resolves to; the remainder are genuinely node- or cluster-level.

    /// <summary>
    /// Master switch for the row-level TTL sweep. On by default: a table only enters the sweep once it
    /// sets <c>ttl_expiration_expression</c>, so a database whose tables configure no TTL sees nothing
    /// beyond an idle poll, while a table that <em>did</em> ask for expiry does not silently retain rows
    /// forever. Set false to stop the sweep node-wide — expired rows are then never collected.
    /// Equivalent in role to CockroachDB's <c>sql.ttl.job.enabled</c> cluster setting.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public bool TtlEnabled { get; init; } = true;

    /// <summary>
    /// Fallback for the per-table <c>ttl_job_cron</c> storage parameter: how often a table's sweep
    /// runs. Matches CockroachDB's default, which moved from <c>@hourly</c> to <c>@daily</c>. Only the
    /// <c>@macro</c> forms are accepted for now (see <see cref="Catalogs.Models.TtlCron"/>).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public string TtlDefaultJobCron { get; init; } = "@daily";

    /// <summary>
    /// Fallback for <c>ttl_select_batch_size</c>: rows read per batch while scanning a span for
    /// expired rows. Larger amortizes the round-trip; smaller shortens the read transaction.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlDefaultSelectBatchSize { get; init; } = 500;

    /// <summary>
    /// Fallback for <c>ttl_delete_batch_size</c>: rows deleted per transaction. Deliberately far
    /// smaller than the select batch — each row costs one mutation <em>per index entry</em> plus the
    /// row itself, so the mutation count grows with the table's index count and must stay well under
    /// <see cref="MaxMutationsPerTransaction"/>. A short delete transaction is also what keeps
    /// foreground writers from queueing behind the sweep's exclusive locks.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlDefaultDeleteBatchSize { get; init; } = 100;

    /// <summary>
    /// Fallback for <c>ttl_select_rate_limit</c>, in rows/second. <c>0</c> means <b>unlimited</b>, not
    /// "stopped" — the scan is already bounded by the delete rate downstream of it.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlDefaultSelectRateLimit { get; init; } = 0;

    /// <summary>
    /// Fallback for <c>ttl_delete_rate_limit</c>, in rows/second — the primary throttle on the sweep's
    /// write load. <c>0</c> means unlimited.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlDefaultDeleteRateLimit { get; init; } = 100;

    /// <summary>
    /// How many spans a table's primary keyspace is divided into per run. Deliberately far more than
    /// the number of workers: row ids are time-ordered ObjectIds, so a uniform split of the hex space
    /// is skewed on the append-heavy tables TTL targets (most rows land in the newest prefix).
    /// Over-splitting lets work-stealing absorb that skew without coupling TTL to <c>ANALYZE</c>
    /// freshness. Rounded up to the next power of 16 so spans align on hex-digit boundaries.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlSpansPerTable { get; init; } = 64;

    /// <summary>
    /// Spans one node processes concurrently. Keep small: each span holds a Kahuna session and issues
    /// exclusive-lock deletes, and the point of the sweep is to be invisible to foreground traffic.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int TtlMaxConcurrentSpansPerNode { get; init; } = 1;

    /// <summary>
    /// Foreground-load surge protector, mirroring <see cref="AutoAnalyzeLoadPauseThreshold"/>: when
    /// in-flight foreground transactions exceed this, the sweep pauses at the next batch boundary
    /// rather than only at span start. <c>&lt;= 0</c> disables load-based backoff. CamusDB extension —
    /// CockroachDB has no equivalent.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int TtlLoadPauseThreshold { get; init; } = 16;

    /// <summary>
    /// Lease on a claimed span, in milliseconds, renewed while the owner processes it. A worker that
    /// crashes stops renewing, the lease lapses, and another worker reclaims the span and resumes from
    /// its checkpoint — so a dead node costs one lease period, not a stalled run.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlSpanLeaseMs { get; init; } = 30_000;

    /// <summary>
    /// How often a span's owner re-stamps its lease. Must be well under <see cref="TtlSpanLeaseMs"/> so
    /// replication lag or a delayed tick cannot let a live owner's lease lapse under it.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TtlSpanLeaseRenewIntervalMs { get; init; } = 10_000;

    /// <summary>
    /// Max keys the non-transactional <c>DROP DATABASE</c> keyspace purge scans and deletes per batch.
    /// The purge pages through each bucket one batch at a time so peak memory is bounded to this many
    /// keys regardless of how large a database (or branch overlay) is — a full database drop can span
    /// every table and row. Kept modest so the purge never materialises an entire keyspace at once.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int KeyspacePurgeBatchSize { get; init; } = 512;

    /// <summary>
    /// Retention window, in milliseconds, for orphaned (deferred-dropped) root databases and tables:
    /// after <c>now - DroppedAt</c> exceeds this, the garbage collector may physically reclaim the
    /// orphan; until then it stays recoverable via <c>CREATE ... RELINK TO</c>. Independent of the
    /// Kahuna PITR/WAL window (which is unrelated to physical row keys). A value <c>&lt;= 0</c> disables
    /// automatic reclamation — orphans are kept until an explicit <c>DROP ... FORCE</c> / manual purge.
    /// Default is 7 days. Surfaced as <c>expires_at</c> by <c>SHOW ORPHAN DATABASES/TABLES</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public long OrphanRetentionMs { get; init; } = 7L * 24 * 60 * 60 * 1000;

    /// <summary>
    /// Attaches an in-process listener to the embedded Kommander and Kahuna
    /// <see cref="System.Diagnostics.Metrics.Meter"/>s so <c>SHOW ENGINE STATS</c> can report them.
    /// Independent of the <c>diagnostics:</c> exporter section: those meters are published whether or
    /// not an exporter is configured, and observing them in-process costs one delegate call plus a
    /// dictionary lookup per measurement. When false no listener is created and the meters revert to
    /// their zero-cost unobserved state, and <c>SHOW ENGINE STATS</c> returns no rows (never an
    /// error, so a script probing a fleet behaves uniformly). Default on; the flag exists as an
    /// escape hatch for benchmarking the measurement overhead itself.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public bool EngineMetricsEnabled { get; init; } = true;

    /// <summary>
    /// Interval, in milliseconds, of the background <c>OrphanReclaimer</c> sweep that physically
    /// reclaims orphaned databases/tables past <see cref="OrphanRetentionMs"/>. The sweep runs on a
    /// single elected node (registry-partition leader). A value <c>&lt;= 0</c> disables the loop
    /// entirely (used by tests that drive a sweep manually). Default is 5 minutes.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int OrphanReclaimIntervalMs { get; init; } = 5 * 60 * 1000;

    /// <summary>
    /// How long a database descriptor may sit unused before this node releases it, in milliseconds.
    /// A value <c>&lt;= 0</c> disables idle eviction entirely, so a database stays open until it is
    /// explicitly closed, dropped, or the process ends. Default is 15 minutes.
    ///
    /// <para>This is what bounds the memory a node spends on databases it is not serving. Without it
    /// the open set only ever grows: every database ever touched keeps its catalog, transactions
    /// manager, schema-apply subscription and per-table state resident for the process lifetime,
    /// which at thousands of registered databases is the dominant cost of an otherwise idle node.</para>
    ///
    /// <para>The window is also a correctness parameter, not only a tuning one. Eviction is safe
    /// because a descriptor being resolved right now cannot look idle — see
    /// <c>DatabaseEvictor</c> — so shrinking this toward zero erodes the margin that argument rests
    /// on. Values below a second are for tests that drive the sweep deliberately, not for
    /// deployment.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int DatabaseIdleEvictionMs { get; init; } = 15 * 60 * 1000;

    /// <summary>
    /// Lease duration, in milliseconds, of a database-registry drop-intent fence (the mutex taken by
    /// <c>DROP</c>/<c>RELINK</c>/the orphan GC per database id and per table). The fence's KV key carries
    /// this as a native expiry: a holder that crashes without releasing frees the fence once the lease
    /// lapses, so a dead owner can no longer block relink/GC of an id indefinitely. A live holder renews
    /// the lease in the background every <see cref="FenceLeaseRenewIntervalMs"/> for as long as it holds
    /// the fence, so a long operation (a large keyspace purge) is never interrupted. Keep it comfortably
    /// longer than a typical fenced operation and longer than the renew interval. Default is 30 seconds.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int FenceLeaseMs { get; init; } = 30_000;

    /// <summary>
    /// How often, in milliseconds, a fence holder renews its drop-intent lease
    /// (see <see cref="FenceLeaseMs"/>). Must be well under the lease so replication lag or a delayed
    /// tick cannot let a still-live holder's lease lapse. Default is a third of the lease.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int FenceLeaseRenewIntervalMs { get; init; } = 10_000;

    /// <summary>
    /// Lease duration, in milliseconds, of the Kahuna snapshot-floor hold a branch database
    /// acquires on its immediate parent at fork time. The hold pins the parent's MVCC history at
    /// <c>forkT</c> so branch as-of reads stay correct under aggressive revision retention. The
    /// hold must be renewed well inside this window for as long as the branch exists (see the
    /// leader-owned renewer); if renewal stops, the hold lapses after one lease and the branch's
    /// frozen view can be reclaimed. Chosen coarse so lease renewals are not a hot Raft path.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int BranchSnapshotHoldLeaseMs { get; init; } = 300_000;

    /// <summary>
    /// Effective Kahuna point-in-time-recovery retention window, in seconds — how far back a restore may
    /// target. Mirrors <c>kahuna.pitr_window_seconds</c> (→ <see cref="Kahuna.EmbeddedKahunaOptions.PitrWindow"/>)
    /// so the restore admin path can reject a target time older than <c>now - window</c> (or in the
    /// future) without re-reading the embedded node's options. Default 3600 (1 hour), matching Kahuna's
    /// default; this is an upper bound on recoverability — actual reach is still limited by which backups
    /// survive in the chain.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int PitrWindowSeconds { get; init; } = 3600;

    /// <summary>
    /// Sliding TTL for the SQL parser AST cache, in seconds.
    /// A successfully-parsed <c>NodeAst</c> is kept in the cache for this many seconds after
    /// the last hit; each cache hit extends the deadline by the same interval.
    /// <para>
    /// Special values:
    ///   <c>&lt;= 0</c> — cache is disabled; every call to <c>SQLParserProcessor.Parse</c>
    ///                    lexes and parses from scratch.
    /// </para>
    /// Default: <c>300</c> (5 minutes), matching Kahuna's script-cache TTL.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int SqlParserCacheTtlSeconds { get; init; } = 300;

    /// <summary>
    /// Maximum number of entries the SQL parser AST cache may hold at any moment.
    /// When the cache is at capacity new statements are silently dropped until the background
    /// sweep reclaims expired entries. This is a safety bound against floods of unique ad-hoc
    /// SQL, not a precise LRU.
    /// <para>
    ///   <c>0</c> — no cap (unbounded, same risk as Kahuna's pure-TTL cache).
    /// </para>
    /// Default: <c>2048</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int SqlParserCacheMaxEntries { get; init; } = 2048;

    /// <summary>
    /// How often, in seconds, the background sweep task removes expired SQL parser cache entries.
    /// Must be &gt; 0.
    /// Default: <c>60</c> seconds.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int SqlParserCacheSweepSeconds { get; init; } = 60;

    /// <summary>
    /// Maximum number of rows the hash-join build phase may materialise before degrading, used
    /// only when <see cref="SpillEnabled"/> is <c>false</c>: the build falls back to a nested-loop
    /// join for correctness. When spill is enabled the build instead routes to the Grace/hybrid
    /// hash join once it exceeds <see cref="SpillEffectiveThreshold"/>, so this cap does not apply.
    /// Default: 1_000_000 rows.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int HashJoinMaxBuildRows { get; init; } = 1_000_000;

    /// <summary>
    /// Opt a table's row and eligible secondary-index key spaces into Kahuna key-range routing
    /// instead of the default hash routing. When enabled,
    /// <see cref="Commands.Executor.Controllers.TableOpener"/> registers each space on the local
    /// node at open time (the Kahuna registry is node-local and not replicated, so every node
    /// opens-and-registers independently), and the range-lock path switches from prefix locks to
    /// Kahuna range locks (prefix locks are rejected on ranged spaces).
    ///
    /// Second slice: secondary indexes whose key columns are all non-String ASCII-encoding
    /// types (Integer64/Float64/Bool/Id/Null) are also registered and range-locked. String-keyed
    /// indexes stay hash-routed until the persistence comparator is aligned. Kahuna
    /// auto-split/merge is not wired (logical range routing + per-range locks work without it).
    ///
    /// <b>Operational requirement:</b> key-range routing requires <c>InitialPartitions ≥ 2</c>
    /// in <c>config.yml</c>. With a single partition the Kahuna registry call is a silent no-op
    /// (stays hash-routed, range locks transparently fall back to the single-partition hash path),
    /// so enabling this flag on a single-partition node is safe but has no effect. A startup
    /// warning is emitted when the flag is on and <c>InitialPartitions &lt; 2</c>. Production
    /// clusters must set <c>initial_partitions: 2</c> (or more) to activate key-range sharding.
    ///
    /// Default off. Set via <c>key_range_sharding</c> in <c>config.yml</c>; the
    /// <c>CAMUS_KEY_RANGE_SHARDING</c> environment variable overrides YAML when set.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public bool KeyRangeShardingEnabled { get; init; }

    /// <summary>
    /// Enables planner fragmentation of eligible queries into per-span scan fragments executed
    /// through a <c>Gather</c> exchange. Eligible today: snapshot/read-only single-table full
    /// primary-row scans over a key-range-sharded table whose placement reports more than one
    /// span; everything else plans and executes exactly as with the flag off. Fragmented plans
    /// bypass the plan cache (their shape depends on live placement, which the cache key does
    /// not carry).
    ///
    /// <para>Cluster scope: nodes must agree, or a statement planned on one node would assume
    /// an execution model a peer refuses. Restart-class: the planner and executor read it
    /// through the engine's constructed options; per-statement flips would also make the
    /// plan-cache bypass decision unstable.</para>
    ///
    /// Default off — plans and results are byte-identical to the non-distributed engine.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public bool DistributedQueryExecutionEnabled { get; init; }

    /// <summary>
    /// Number of Raft data partitions active in this cluster. Populated from the
    /// <c>initial_partitions</c> config key at startup. Used by <see cref="PlacementReader"/>
    /// to approximate the remote-data fraction for <c>NetworkFactor</c> cost estimates.
    ///
    /// Default: 1 (single-partition / single-node). Tests that do not set this explicitly
    /// keep the single-node behaviour (NetworkFactor = 0).
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public int ClusterPartitionCount { get; init; } = 1;

    /// <summary>
    /// Weight applied to bytes shipped over the network in the network cost model.
    /// <c>NetworkFactor ≈ remoteRows × rowWidthBytes × NetWeight</c>.
    ///
    /// Calibrated so that one remote row fetch (≈ 100 bytes) costs ≈ 1.0 cost unit —
    /// matching one local KV point lookup. Set to 0.0 to disable network cost entirely
    /// (equivalent to single-node behaviour).
    ///
    /// Default: 0.01 (100 bytes × 0.01 = 1.0 cost unit per remote row).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public double NetWeight { get; init; } = 0.01;

    /// <summary>
    /// Enables cost-based access-path selection in the query planner. When <c>true</c> (default),
    /// the planner enumerates all viable index steps for tables that have been ANALYZEd, costs
    /// each against the full-scan baseline, and picks the cheapest; never-ANALYZEd tables keep
    /// rule-based plans (DML-maintained row counts alone would engage the cost model with fixed
    /// fallback selectivities). When <c>false</c>, the rule-based (score-based) path is used
    /// unchanged, so plans are byte-identical to the heuristic planner regardless of whether
    /// statistics have been collected. Set via <c>cost_based_access_path_enabled</c>
    /// in <c>config.yml</c>.
    ///
    /// <para>Still opt-in. Costing engages on a DML-maintained row count alone, so a table that has
    /// never been analyzed is costed without histograms or NDV and can be planned worse than the
    /// heuristic — and a small table never reaches <see cref="AutoAnalyzeMinStaleRows"/>, so nothing
    /// fills those in for it. Turning it on also changes which keys a read touches, which changes
    /// what an optimistic transaction folds into its read set.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public bool CostBasedAccessPathEnabled { get; init; } = true;

    /// <summary>
    /// Enables cost-based join-order enumeration (System-R–style DP) in the query planner.
    /// When <c>true</c> (default), the planner costs all left-deep orderings and picks the
    /// cheapest using INLJ vs hash-join cost asymmetry. When <c>false</c>, the rule-based
    /// heuristic (<see cref="JoinOrderOptimizer"/>) is used and plans are byte-identical
    /// to the heuristic planner's output. Joins wider than
    /// <c>JoinEnumerator.MaxTablesForEnumeration</c> always fall back to the heuristic
    /// regardless of this flag. Set via <c>cost_based_join_order_enabled</c> in <c>config.yml</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public bool CostBasedJoinOrderEnabled { get; init; } = true;

    /// <summary>
    /// Enables the per-process query plan cache.
    ///
    /// When <c>true</c>, the planner caches its access-path decision (which index or full scan)
    /// and join ordering keyed by <see cref="QueryShapeId"/> (a literal-independent fingerprint).
    /// A cache hit skips cost enumeration and re-binds the current query's predicates into the
    /// cached structural decision.
    ///
    /// Plan-stability tradeoff: because the cache key ignores literal values, a non-selective
    /// query (e.g. <c>WHERE status = 'all'</c>) can inherit the access path cached by a
    /// selective query of the same shape (e.g. <c>WHERE status = 'rare'</c>). Both decisions
    /// are correct — the planner would have chosen the same index for both — but the cache
    /// prevents re-scoring when ANALYZE produces new statistics that would change the choice.
    /// Schema changes (DDL) do invalidate the cache; ANALYZE alone does not.
    ///
    /// Disabled by default following the project convention that all cost-based optimizations
    /// are opt-in (<c>cost_based_access_path_enabled</c>, <c>cost_based_join_order_enabled</c>).
    /// Set via <c>plan_cache_enabled</c> in <c>config.yml</c>. The cache size is bounded by
    /// <see cref="PlanCacheMaxEntries"/>; set either to 0 to disable.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool PlanCacheEnabled { get; init; } = false;

    /// <summary>
    /// Maximum number of entries held by the per-process plan cache (LRU eviction when the
    /// limit is exceeded). Set via <c>plan_cache_max_entries</c> in <c>config.yml</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int PlanCacheMaxEntries { get; init; } = 512;

    /// <summary>
    /// Cluster-wide default isolation level applied when a transaction is begun without an
    /// explicit level. Individual transactions may override this via the begin-request field
    /// or via <c>SET TRANSACTION ISOLATION LEVEL …</c>.
    ///
    /// Default: <see cref="CamusIsolationLevel.Serializable"/> — every new transaction is
    /// serializable unless it overrides this. Set to <see cref="CamusIsolationLevel.ReadCommitted"/>
    /// (via <c>default_isolation_level: read_committed</c> in <c>config.yml</c>) to opt out.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public CamusIsolationLevel DefaultIsolationLevel { get; init; } = CamusIsolationLevel.Serializable;

    /// <summary>
    /// Default Kahuna concurrency strategy applied when a transaction is begun without an explicit
    /// locking mode. <see cref="Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Pessimistic"/>
    /// (acquire-then-write, block on conflict) preserves the historical behavior. An individual
    /// transaction can opt into <see cref="Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic"/>
    /// via the <c>locking</c> argument to <c>KvTransactionsManager.BeginAsync</c>; the coordinator then
    /// defers <b>write-write</b> conflict detection to commit-time read-set/write-intent validation
    /// instead of taking explicit exclusive write locks.
    ///
    /// <para><b>Scope of "lock-free."</b> Optimistic is fully lock-free only under Read Committed. Under
    /// the default Serializable isolation, reads and scans still take shared range/point predicate locks
    /// and a following write upgrades them to exclusive — that gating is on the isolation level, not on
    /// this setting. So a Serializable+Optimistic transaction is a <b>hybrid</b>: optimistic write and
    /// read-set validation combined with the predicate locks that keep it phantom-free. Serializable is
    /// intentionally not weakened to lock-free.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking DefaultTransactionLocking { get; init; } = global::Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Pessimistic;

    /// <summary>
    /// Default admission priority applied when a transaction is begun without an explicit one.
    /// <see cref="Kahuna.Shared.KeyValue.TransactionPriority.Normal"/> is the neutral value and keeps
    /// behavior identical to before priorities existed. A transaction can override it per request
    /// (HTTP/gRPC <c>priority</c>) or with <c>SET TRANSACTION PRIORITY</c> before its first statement.
    ///
    /// <para><b>This is inert on a default-configured node.</b> Priority is consulted only by the
    /// Kahuna node's admission gate, which is disabled while the concurrency ceiling
    /// (<c>kahuna.max_concurrent_sessions</c>) is zero — its default. Until a ceiling is set, every
    /// transaction is admitted immediately and this value is recorded for observability only.</para>
    ///
    /// <para><b>It orders starts, not execution.</b> Once admitted, a low-priority transaction competes
    /// for CPU, I/O and locks exactly like any other — nothing is preempted or throttled, and lock
    /// conflicts ignore priority entirely. Use it to decide who waits at the door, not to make
    /// background work cheap while it runs.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public global::Kahuna.Shared.KeyValue.TransactionPriority DefaultTransactionPriority { get; init; } = global::Kahuna.Shared.KeyValue.TransactionPriority.Normal;

    /// <summary>
    /// Milliseconds a transaction will queue at the node's admission gate before it is refused and the
    /// caller told to retry. <c>0</c> — the default — leaves the node's own default budget in force.
    ///
    /// <para><b>This is the door-wait, not the transaction lifetime.</b> The two are deliberately
    /// separate: <see cref="MaxSerializableTransactionLifetimeMs"/> is how long an admitted transaction
    /// may live (and doubles as the abandoned-session reaper window), whereas this bounds how long an
    /// unadmitted one waits to begin. A transaction that intends to run for an hour is not thereby
    /// willing to wait an hour to start, so passing the lifetime here would make a saturated node hold
    /// requests open instead of shedding them.</para>
    ///
    /// <para>Like every other knob in the admission family this is inert while
    /// <c>kahuna.max_concurrent_sessions</c> is zero: with no ceiling admission completes synchronously
    /// and nothing ever queues. The node clamps whatever is asked for here to its own
    /// <c>kahuna.max_admission_wait_ms</c>, so an operator's ceiling always wins.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TransactionAdmissionWaitMs { get; init; }

    /// <summary>
    /// Default read-set validation policy applied when a transaction is begun without an explicit
    /// value. <see cref="Kahuna.Shared.KeyValue.ReadValidation.None"/> keeps the historical behavior:
    /// pessimistic transactions rely on their locks alone and do not fold read observations. A
    /// transaction can opt into <see cref="Kahuna.Shared.KeyValue.ReadValidation.TrackAndValidate"/>
    /// to additionally validate its read set at commit even while pessimistic. Optimistic transactions
    /// always validate their read set regardless of this value.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public global::Kahuna.Shared.KeyValue.ReadValidation DefaultReadValidation { get; init; } = global::Kahuna.Shared.KeyValue.ReadValidation.None;

    /// <summary>
    /// Default commit-decision durability applied when a transaction is begun without an explicit
    /// value. <see cref="Kahuna.Shared.KeyValue.DecisionDurability.BestEffort"/> keeps the decision in
    /// memory (it can be lost if the coordinator crashes before its next checkpoint) and is the
    /// historical behavior. A transaction can opt into
    /// <see cref="Kahuna.Shared.KeyValue.DecisionDurability.Durable"/> so the commit decision is
    /// written to durable storage before the outcome is returned; the coordinator assigns the record
    /// anchor from the first confirmed persistent write, so no client-side anchor plumbing is needed.
    /// Durable mode rejects a transaction that confirmed any ephemeral modification.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public global::Kahuna.Shared.KeyValue.DecisionDurability DefaultDecisionDurability { get; init; } = global::Kahuna.Shared.KeyValue.DecisionDurability.BestEffort;

    /// <summary>
    /// Initial TTL, in milliseconds, requested for each Kahuna range lock acquired by a serializable
    /// read-write transaction. Range-lock acquisitions are registered with the transaction coordinator,
    /// so once the lock is held the coordinator renews its lease every collection-interval tick for the
    /// life of the session and releases it on finalize — the client no longer heartbeats it.
    ///
    /// <para><b>Timing invariant:</b> this initial TTL must comfortably exceed the coordinator's
    /// collection interval (its renewal tick period, 60 s by default), because the lock must survive
    /// from acquisition until the first server renewal. A value below that interval would let the lock
    /// lapse before the coordinator renews it, silently breaking serializable isolation. 150 s survives
    /// well past the first 60 s tick.</para>
    ///
    /// <para>A zero or negative value tells Kahuna to hold the lock indefinitely (no TTL).</para>
    ///
    /// Default: 150 000 ms (150 s).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int RangeLockExpiresMs { get; init; } = 150_000;

    /// <summary>
    /// Maximum wall-clock lifetime, in milliseconds, for a <see cref="CamusIsolationLevel.Serializable"/>
    /// + <see cref="CamusTransactionMode.ReadWrite"/> transaction. Acts as an absolute backstop: once
    /// this duration elapses from <c>BeginAsync</c>, any subsequent operation (range lock acquisition,
    /// commit) throws <see cref="CamusDBErrorCodes.TransactionLifetimeExceeded"/>.
    ///
    /// <para>The coordinator renews a live session's range locks so they never expire due to TTL; this
    /// cap only bounds runaway transactions. It is also supplied to Kahuna as the session
    /// <c>Timeout</c>, so an abandoned session's locks are reclaimed by the server reaper after this
    /// window plus its grace period. A zero or negative value disables the deadline (useful in tests).</para>
    ///
    /// Default: 3 600 000 ms (1 hour).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxSerializableTransactionLifetimeMs { get; init; } = 3_600_000;

    /// <summary>
    /// Total wall-clock budget, in milliseconds, for retrying a single <c>COMMIT</c>, <c>ROLLBACK</c>,
    /// or working-set close against the coordinator's non-terminal <c>MustRetry</c> — "the outcome is
    /// not known yet; retry the same handle". Once the budget is exhausted the finalize surfaces
    /// <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/> and the caller must retry the
    /// <em>same</em> finalize; the business operation is never replayed, because the write may already
    /// have committed.
    ///
    /// <para>This is a budget, not an attempt count, because the conditions behind <c>MustRetry</c> are
    /// measured in time rather than in tries: a leadership flip mid-finalize, an in-progress drain, a
    /// participant whose write was shed after ageing in the pre-dispatch queue, or a durable decision
    /// not yet marked complete. Under CPU saturation each of those takes longer in wall-clock but not
    /// more attempts, so a fixed attempt cap silently shrinks the real budget exactly when it is needed
    /// most. It must comfortably exceed the storage layer's own transient-shedding threshold, otherwise
    /// a single shed round consumes the whole budget and a recoverable in-doubt commit is reported as
    /// an error.</para>
    ///
    /// <para><c>&lt;= 0</c> disables retrying: the finalize is attempted exactly once and any
    /// <c>MustRetry</c> surfaces immediately.</para>
    ///
    /// Default: 15 000 ms (15 s).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TransactionFinalizeRetryBudgetMs { get; init; } = 15_000;

    /// <summary>
    /// Total wall-clock budget, in milliseconds, for retrying a call against one of the persistent
    /// monotonic counters — the database id, the table id, the registry's cross-node generation stamp —
    /// while the storage layer answers <c>MustRetry</c>.
    ///
    /// <para><c>MustRetry</c> means the Raft partition owning the counter has no confirmed leader at that
    /// instant. Every DDL statement that names a new relation allocates an id first, so this budget is
    /// what decides whether a routine election is invisible or fails a user's CREATE TABLE with
    /// <see cref="CamusDBErrorCodes.SequenceUnavailable"/>.</para>
    ///
    /// <para>A wall-clock budget rather than an attempt count, and sized against the election itself: an
    /// election runs on the order of seconds (Kommander's election timeout is 2–4 s before increments),
    /// while a handful of attempts with short back-off spends its whole allowance in well under one. An
    /// attempt cap also shrinks the effective budget on a saturated node, which makes each attempt slower
    /// without making it more patient — exactly when the wait was most worth making.</para>
    ///
    /// <para><c>&lt;= 0</c> disables retrying: the call is attempted once and any <c>MustRetry</c>
    /// surfaces immediately. Default: 10 000 ms (10 s).</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int SequenceRetryBudgetMs { get; init; } = 10_000;

    /// <summary>
    /// How long, in milliseconds, an explicit (client-driven) transaction may sit idle — no
    /// statement issued against it — before the background reaper rolls it back and releases its
    /// locks. "Idle" is measured from the last time the client referenced the transaction (its
    /// last statement, or its begin), using a monotonic clock immune to wall-clock jumps.
    ///
    /// <para>This targets abandoned transactions: a client that opens a transaction and then
    /// disconnects, crashes, or forgets to commit/rollback. Such a transaction is otherwise never
    /// finalized, so the coordinator would keep renewing its Serializable+RW range locks and every
    /// conflicting transaction would keep aborting. This CamusDB-side reaper reclaims it — freeing the
    /// locks, rolling back the Kahuna transaction, and dropping it from the in-flight map — as does the
    /// Kahuna server reaper once the session <c>Timeout</c> lapses.</para>
    ///
    /// <para>Only transactions tracked by the HTTP transaction coordinator (explicit
    /// <c>/start-transaction</c> sessions and promoted read-only scans) are subject to the reaper;
    /// single-statement autocommit requests finalize within their own request and are never
    /// tracked. A pause longer than this window inside an interactive session is treated as
    /// abandonment and rolled back.</para>
    ///
    /// <para><c>&lt;= 0</c> disables this CamusDB-side reaper. Even when disabled, an abandoned
    /// Serializable+RW transaction's locks are still bounded by the Kahuna session <c>Timeout</c>
    /// (set from <see cref="MaxSerializableTransactionLifetimeMs"/>): the server reaper reclaims the
    /// session after that window plus its grace period, so a disabled CamusDB reaper degrades the
    /// worst case from "prompt cleanup" to "reclaimed by the server after the lifetime cap", never
    /// back to a permanent hold.</para>
    ///
    /// Default: 300 000 ms (5 minutes).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int TransactionIdleTimeoutMs { get; init; } = 300_000;

    /// <summary>
    /// How often, in milliseconds, the background reaper sweeps the in-flight transaction map for
    /// entries idle longer than <see cref="TransactionIdleTimeoutMs"/>. A single timer scans all
    /// tracked transactions, so this cost is one periodic pass regardless of transaction count —
    /// it does not add a timer per transaction.
    ///
    /// <para>Must be positive. Set it well under <see cref="TransactionIdleTimeoutMs"/> so an
    /// abandoned transaction is caught within roughly one sweep of crossing the idle threshold.</para>
    ///
    /// Default: 30 000 ms (30 s).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int TransactionReaperIntervalMs { get; init; } = 30_000;

    /// <summary>
    /// Per-bucket shared-point-lock count at which a Serializable+RW transaction escalates from
    /// individual singleton <c>[key,key]</c> range locks to one whole-bucket <c>[null,null)</c>
    /// Shared range lock. Once escalated, subsequent reads on the same bucket need no additional
    /// lock RPCs — the whole-bucket lock already covers them. Old per-point lock entries remain
    /// in tracking and are released at commit/rollback.
    ///
    /// <para>A lower value escalates earlier (fewer RPCs per read, larger lock granularity);
    /// a very high value effectively disables escalation. Tests may set this to 1–3 to exercise
    /// the escalation path without reading thousands of rows.</para>
    ///
    /// Default: 50.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int LockEscalationThreshold { get; init; } = 50;

    /// <summary>
    /// Maximum concurrently in-flight operations the server executes per <c>CamusSql.BatchExecute</c>
    /// duplex stream before applying backpressure. Bounds one client's fan-out. Default: 64.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int GrpcBatchMaxInFlight { get; init; } = 64;

    /// <summary>
    /// Maximum live prepared statements one <c>CamusSql.BatchExecute</c> stream may register.
    /// <c>0</c> = unbounded. Exceeding it fails the PREPARE with
    /// <see cref="CamusDBErrorCodes.PreparedStatementLimitExceeded"/> rather than evicting an
    /// existing handle, so a client's live handles never stop working underneath it. Default: 512.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int GrpcMaxPreparedStatementsPerStream { get; init; } = 512;

    /// <summary>
    /// Maximum live REST prepared statements one principal may hold on this node. <c>0</c> =
    /// unbounded. Default: 512.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int RestMaxPreparedStatementsPerPrincipal { get; init; } = 512;

    /// <summary>
    /// Maximum live REST prepared statements this node will hold across all principals. <c>0</c> =
    /// unbounded. Guards a single node against a fleet of clients each holding its per-principal
    /// cap. Default: 8192.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int RestMaxPreparedStatements { get; init; } = 8192;

    /// <summary>
    /// How long a REST prepared statement may sit unused before the reaper drops it, in
    /// milliseconds. <c>0</c> disables reaping, so entries then live until they are closed or the
    /// process exits.
    ///
    /// <para>REST has no connection-scoped session the server can trust — connections are pooled,
    /// HTTP/2-multiplexed and load-balanced — so this idle timeout is the backstop for clients that
    /// never close what they prepared. Expiry is also checked on lookup, so an entry that outlives
    /// its timeout between sweeps is never executed. Default: 600000 (10 minutes).</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int PreparedStatementIdleTimeoutMs { get; init; } = 600_000;

    /// <summary>
    /// Interval between REST prepared-statement reaper sweeps, in milliseconds. Default: 60000.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int PreparedStatementSweepIntervalMs { get; init; } = 60_000;

    /// <summary>
    /// Largest statement, in bytes of retained text (database + SQL + parameter names, UTF-16), that
    /// may be prepared on either transport. <c>0</c> = unlimited.
    ///
    /// <para>Counting statements is not the same as bounding memory: a cap of 512 statements permits
    /// 512 × whatever the transport's maximum request size happens to be. This limit is what makes
    /// the count caps meaningful, by bounding the per-entry term. A statement over it is rejected as
    /// invalid input rather than as a quota failure — no amount of closing other statements would
    /// make it fit. Default: 65536 (64 KiB), which is far above any realistic parameterized
    /// statement.</para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int MaxPreparedStatementBytes { get; init; } = 65_536;

    /// <summary>
    /// Total retained prepared-statement text this node will hold for REST clients, in bytes.
    /// <c>0</c> = unlimited. Default: 67108864 (64 MiB).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public long RestMaxPreparedStatementBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// Retained prepared-statement text one REST principal may hold, in bytes. <c>0</c> = unlimited.
    /// Matters most when authentication is disabled, since every caller then shares one anonymous
    /// principal. Default: 8388608 (8 MiB).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public long RestMaxPreparedStatementBytesPerPrincipal { get; init; } = 8L * 1024 * 1024;

    /// <summary>
    /// Retained prepared-statement text one <c>BatchExecute</c> stream may hold, in bytes.
    /// <c>0</c> = unlimited. Default: 8388608 (8 MiB).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public long GrpcMaxPreparedStatementBytesPerStream { get; init; } = 8L * 1024 * 1024;

    /// <summary>
    /// Maximum number of row + secondary-index mutations a single read-write transaction may
    /// accumulate, mirroring Cloud Spanner's per-commit mutation cap.
    ///
    /// <para>One CamusDB mutation = one row-blob write/delete <em>or</em> one secondary-index
    /// entry write/delete. Rows are stored as single KV blobs (not column-per-cell), so each
    /// INSERT counts as <c>1 + K</c> mutations (row + K index entries), and each UPDATE that
    /// touches an indexed column counts <c>1 + 2</c> per changed index (row rewrite + old-entry
    /// delete + new-entry insert). The counter is monotonic — updating the same row twice counts
    /// twice.</para>
    ///
    /// <para>A transaction that would exceed this limit throws
    /// <see cref="CamusDBErrorCodes.TransactionMutationLimitExceeded"/> (<c>CADB0506</c>) before
    /// any of the offending writes are sent to Kahuna. This error is <b>non-retryable</b>: the
    /// caller must split the work into smaller transactions.</para>
    ///
    /// <para><c>&lt;= 0</c> — limit is disabled; equivalent to today's unlimited behaviour.
    /// DDL and backfill transactions always run with limit = 0 regardless of this setting.</para>
    ///
    /// Default: <c>20_000</c> (matches Spanner's historical default).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxMutationsPerTransaction { get; init; } = 20_000;

    /// <summary>
    /// Maximum nesting depth for view-over-view expansion.
    ///
    /// <para>A backstop, not the defense: a cycle is rejected at DDL time by walking the stored
    /// dependency ids, and the expansion stack rejects a cycle again at read time. This cap only
    /// bounds a legitimately-but-absurdly deep chain, so the failure it produces is a clear error
    /// rather than a stack overflow.</para>
    ///
    /// Default: <c>32</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxViewExpansionDepth { get; init; } = 32;

    /// <summary>
    /// Rows written per transaction while a materialized-view <c>REFRESH</c> populates its new
    /// contents.
    ///
    /// <para>Must stay below <see cref="MaxMutationsPerTransaction"/>, and by a margin: each row
    /// costs <c>1 + K</c> mutations for a relation with K indexes, so a chunk size close to the
    /// mutation cap would fail on any indexed materialized view. Refresh chunks precisely because a
    /// single-transaction rebuild cannot work for a relation of any real size.</para>
    ///
    /// Default: <c>10_000</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaterializedViewRefreshChunkRows { get; init; } = 10_000;

    /// <summary>
    /// Whether this node may run materialized-view refresh work. Turning it off does not disable
    /// materialized views — reads and DDL keep working — it only stops this node from executing a
    /// refresh, for a node that should not carry background work.
    ///
    /// Default: <c>true</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool MaterializedViewRefreshEnabled { get; init; } = true;

    /// <summary>
    /// How many times the background sweep may restart a materialized-view refresh that was
    /// interrupted — by a crashed process, or by a node that lost leadership part-way — before it
    /// gives up and leaves the view holding its previous contents.
    ///
    /// <para>A restart is a fresh rebuild, not a continuation: the interrupted run's partial output is
    /// discarded and the body is read again at a new snapshot. Resuming mid-scan would require the
    /// body's row order to be identical across executions, which the planner does not promise for a
    /// join or an aggregate, and a resumed non-deterministic body produces a materialized view with
    /// duplicated and missing rows.</para>
    ///
    /// <para>The cap is what keeps a rebuild that fails <i>deterministically</i> — a source relation
    /// that no longer satisfies a constraint, say — from being retried forever by a janitor nobody is
    /// watching. Set it to <c>0</c> to disable takeover entirely: the sweep then only reclaims the
    /// abandoned staging storage, and the refresh must be issued again by hand.</para>
    ///
    /// Default: <c>3</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaterializedViewRefreshTakeoverAttempts { get; init; } = 3;

    /// <summary>
    /// Wall-clock cap, in milliseconds, for a single lock-acquire retry loop during Serializable
    /// conflicts. Bounds deadlock and persistent lock-conflict latency per operation.
    /// Default: 500 ms.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int LockWaitDeadlineMs { get; init; } = 500;

    /// <summary>
    /// Maximum length (in UTF-16 <c>string.Length</c> units) for any user-facing identifier:
    /// database names, table names, column names, and index names (including rename targets).
    /// <para>
    /// Replaces the former hard-coded 255-character limit with a tighter, configurable cap.
    /// Pre-existing names that exceed this value continue to load — the limit gates only new
    /// creation and rename operations, not existing schema reads.
    /// </para>
    /// <para><c>&lt;= 0</c> — limit is disabled (no length enforcement).</para>
    /// Default: <c>64</c> (matches MySQL / PostgreSQL identifier limits).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxIdentifierLength { get; init; } = 64;

    /// <summary>
    /// Maximum number of user-declared columns allowed per table. Counts the columns visible in
    /// <c>CREATE TABLE</c> and after each <c>ALTER TABLE ADD COLUMN</c>. Internal reserved columns
    /// (e.g. <c>_id</c>) are not user-declarable and do not count toward the cap.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>512</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxColumnsPerTable { get; init; } = 512;

    // ──────────────────────────────────────────────────────────────────────────
    // Spill-to-disk
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Gates all spill-to-disk behaviour. When <c>false</c> (default), blocking query operators
    /// keep today's unbounded in-memory buffers — callers accept the OOM risk on large inputs.
    /// When <c>true</c>, each blocking operator spills sorted runs to temp files once its buffer
    /// exceeds <see cref="SpillThresholdRows"/>. If the temp store is unwritable and spill is
    /// required, <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> is thrown instead of
    /// silently buffering unbounded memory.
    ///
    /// Default: <c>false</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool SpillEnabled { get; init; } = false;

    /// <summary>
    /// Per-operator in-memory row cap before the operator begins spilling to disk.
    /// Applies independently to each blocking operator (sort, hash-join build, GROUP BY, …).
    /// The operator accumulates up to this many rows in memory; when the next row would exceed
    /// the cap, the current in-memory buffer is sorted/serialised and written as a spill run.
    ///
    /// Ignored when <see cref="SpillEnabled"/> is <c>false</c>.
    ///
    /// Default: <c>500_000</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int SpillThresholdRows { get; init; } = 500_000;

    /// <summary>
    /// Maximum number of simultaneously-open spill-run readers during a k-way merge pass.
    /// When the number of spilled runs exceeds this value, a multi-pass merge is performed:
    /// runs are merged in groups of <c>SpillMergeFanIn</c> until a single merged output remains.
    ///
    /// Ignored when <see cref="SpillEnabled"/> is <c>false</c>.
    ///
    /// Default: <c>16</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int SpillMergeFanIn { get; init; } = 16;

    /// <summary>
    /// When set, the query decode path produces slot-backed <c>QueryRow</c> rows — one
    /// <c>ValueSlot[]</c> per row and zero per-cell <c>ColumnValue</c> objects, materialized lazily on
    /// access — so a filtered-out row never allocates a <c>ColumnValue</c> for its projection cells.
    /// The eager <c>ColumnValue[]</c> path (the default) decodes each cell to a <c>ColumnValue</c> up
    /// front. Both paths are value-identical.
    ///
    /// <para>
    /// <b>Default is <c>true</c></b>. It was originally off because the slot path was a
    /// selectivity-dependent trade: with the old whole-row consumers, a non-selective scan or
    /// <c>SELECT *</c> materialized every cell anyway, so the slot array plus lazy cache were pure
    /// overhead (~26% more allocation on <c>SELECT *</c>). That regression case no longer exists:
    /// filter, sort, and the hash operators consume slots natively, and the REST/gRPC response sinks
    /// serialize a projected cell straight from its <c>ValueSlot</c> (<c>QueryRow.TryGetSlot</c>)
    /// without ever materializing a <c>ColumnValue</c> — measured through the real JSON sink,
    /// unfiltered <c>SELECT *</c> is now ~12% faster and ~17% lighter slot-backed, and selective
    /// scans win by more. Set to <c>false</c> as the kill-switch / eager A/B baseline.
    /// See BENCH-RESULTS.md → "Slot-backed decode (A/B)" and "Slot-direct response sinks".
    /// </para>
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool SlotBackedDecode { get; init; } = true;

    /// <summary>
    /// Global policy for borrowed (zero-copy) decode, which backs a decoded scan row with a
    /// <see cref="CamusDB.Core.Storage.Kv.RowView"/> over the raw KV bytes instead of an eager
    /// <c>ValueSlot[]</c> — an unread cell then decodes nothing (not even to a slot) and no per-row slot
    /// array is allocated, a win on selective scans over wide or string/bytes-heavy rows. Three states,
    /// default <see cref="BorrowedDecodePolicy.Adaptive"/>:
    /// <list type="bullet">
    /// <item><see cref="BorrowedDecodePolicy.Adaptive"/> — the scanner opts in per query for the case it
    ///   strictly wins: a residual filter and no row-retaining operator downstream (see
    ///   <c>QueryScanner.ShouldUseBorrowedDecode</c>). Unfiltered <c>SELECT *</c> and retaining scans stay
    ///   eager. This is the production default.</item>
    /// <item><see cref="BorrowedDecodePolicy.ForceBorrowed"/> — borrowed on <i>every</i> scan, including
    ///   <c>SELECT *</c> (a small loss with no rejects to save on) and retaining scans (whose
    ///   retained-memory cost is not yet bounded by copy-on-retain). For A/B measurement.</item>
    /// <item><see cref="BorrowedDecodePolicy.ForceEager"/> — borrowed off everywhere: the kill-switch /
    ///   eager A/B baseline.</item>
    /// </list>
    /// Takes precedence over <see cref="SlotBackedDecode"/> when both would apply. Per-scan control lives
    /// on <see cref="CamusDB.Core.Storage.Kv.RowEncoder.RowDecodeState.BorrowedDecode"/>; each accessed
    /// cell materializes at most once (the row caches it), so borrowed never re-decodes a cell.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public BorrowedDecodePolicy BorrowedDecode { get; init; } = BorrowedDecodePolicy.Adaptive;

    /// <summary>
    /// Upper bound on a single spill-run record's declared payload length, checked before the
    /// reader allocates or rents a buffer for it. A spill file is trusted engine output, but the
    /// frame-length prefix is still read straight off disk, so a corrupt or truncated file could
    /// otherwise name an absurd length and drive an <see cref="OutOfMemoryException"/> or a huge
    /// pool rent. A length that is negative or exceeds this cap is rejected as
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> instead. Generous by default so it
    /// never trips on a legitimately wide row (large Bytes/Array columns).
    ///
    /// Default: <c>256 MiB</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int SpillMaxFrameBytes { get; init; } = 256 * 1024 * 1024;

    /// <summary>
    /// <b>Test-only override.</b> When non-null, replaces <see cref="SpillThresholdRows"/> with
    /// this value for every operator, forcing spill on tiny inputs so the spill code path runs
    /// deterministically in unit tests without large data sets. Has no production use.
    ///
    /// Operators read this via <see cref="SpillEffectiveThreshold"/>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int? ForceSpillThresholdRows { get; init; } = null;

    /// <summary>
    /// Returns the effective spill-row threshold: <see cref="ForceSpillThresholdRows"/> when set
    /// (test override), otherwise <see cref="SpillThresholdRows"/>.
    /// </summary>
    public int SpillEffectiveThreshold => ForceSpillThresholdRows ?? SpillThresholdRows;

    /// <summary>
    /// Maximum number of user-visible secondary indexes allowed per table. The implicit primary-key
    /// index (<c>~pk</c>) and any internal <c>~</c>-prefixed indexes are exempt and do not count.
    /// Checked on each <c>ALTER INDEX ADD INDEX / ADD UNIQUE</c> operation.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>64</c> (matches MySQL's per-table secondary-index cap).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxIndexesPerTable { get; init; } = 64;

    /// <summary>
    /// Maximum number of columns a single index may span, counting <b>both</b> its key columns and its
    /// stored/payload (<c>INCLUDE</c>) columns. Guards against a covering index that duplicates an
    /// unbounded amount of row data into every entry value. Checked at DDL time when an index is
    /// created (standalone <c>CREATE/ALTER INDEX</c> and inline <c>CREATE TABLE</c> constraints).
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>32</c> (matches SQL Server's key+included column ceiling).
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxIndexColumns { get; init; } = 32;

    /// <summary>
    /// Maximum encoded byte size of an index entry's stored/payload (<c>INCLUDE</c>) tuple. Guards
    /// against a single oversized <c>String</c>/<c>Bytes</c>/<c>Array</c> included value bloating every
    /// index entry, its replication, and the enclosing transaction batch. Checked at write time
    /// (INSERT/UPDATE/backfill) after the tuple is encoded and before the KV mutation is issued; a row
    /// whose tuple exceeds this is rejected with <see cref="CamusDBErrorCodes.SchemaLimitExceeded"/>.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>4096</c> (4 KiB); tune against Kahuna's preferred value/batch sizes.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxIndexIncludeTupleBytes { get; init; } = 4096;

    /// <summary>
    /// Maximum number of tables allowed in a single database. Checked at <c>CREATE TABLE</c>
    /// time against the database's current persisted table set. <c>CREATE TABLE IF NOT EXISTS</c>
    /// that resolves to an already-existing table is exempt — the table already counts.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>10000</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Cluster)]
    public int MaxTablesPerDatabase { get; init; } = 10_000;

    /// <summary>
    /// PBKDF2-HMAC-SHA256 iteration count used when hashing a user password
    /// (<c>CREATE USER … IDENTIFIED …</c> / <c>ALTER USER … IDENTIFIED …</c>). Deliberately high so a
    /// stolen catalog cannot be brute-forced cheaply; the value in force is stored <b>with</b> each
    /// credential so raising this later does not invalidate existing hashes (they verify at their own
    /// stored count and can be upgraded on next login in the enforcement phase). OWASP's current
    /// PBKDF2-HMAC-SHA256 floor is 600,000.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public int PasswordHashIterations { get; init; } = 600_000;

    /// <summary>
    /// Master switch for SQL authentication and authorization. When <c>false</c> (the default and the
    /// state of every existing deployment), no credentials are required and no privilege checks run —
    /// the auth catalog is inert metadata and behavior is exactly as before Phase 2. When <c>true</c>,
    /// requests must present a valid bearer token and every statement is privilege-checked; the server
    /// refuses to start with auth enabled and an empty catalog unless a bootstrap secret is supplied
    /// (fail-closed — never an open admin window).
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public bool AuthenticationEnabled { get; init; } = false;

    /// <summary>
    /// One-time bootstrap superuser name, read from an external secret provider / environment at
    /// startup (never a plaintext password in <c>config.yml</c>). Empty when unset. Paired with
    /// <see cref="BootstrapSuperuserPassword"/>: on first start with an empty catalog they create
    /// exactly one superuser via a transactional create-if-absent; once any user exists the bootstrap
    /// values are ignored with a warning.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public string BootstrapSuperuser { get; init; } = "";

    /// <summary>Bootstrap superuser cleartext password from the external secret. Empty when unset. Used
    /// once at first start and never persisted in cleartext — see <see cref="BootstrapSuperuser"/>.</summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public string BootstrapSuperuserPassword { get; init; } = "";

    /// <summary>
    /// Server-side key used to HMAC access-token secrets before they are stored in the session
    /// catalog, so a catalog leak does not yield usable tokens. Common to every cluster node, sourced
    /// from an external secret/environment, never stored in the catalog. When empty (auth disabled or
    /// unconfigured) tokens are not issued. A future key id enables rotation.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public string AccessTokenServerKey { get; init; } = "";

    /// <summary>
    /// Absolute lifetime of an access token. Short by design — there is no sliding expiry or refresh
    /// token initially; re-login is the refresh. Expired tokens are rejected with
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Cluster)]
    public TimeSpan AccessTokenTtl { get; init; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Maximum staleness of a per-node authorization cache hit. A token/privilege snapshot cached on
    /// one node is trusted for at most this long before it revalidates the credential/authorization
    /// epochs against the catalog, so a revoke on another node takes effect within this bound. Set to
    /// zero to force an authoritative lookup on every request (immediate revocation, higher cost).
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public TimeSpan AuthenticationCacheTtl { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum number of password-verification (PBKDF2) operations allowed to run concurrently across
    /// all logins. Bounds the CPU an attacker can tie up with a login flood, since each verification is
    /// deliberately expensive. Excess logins queue briefly; sustained saturation surfaces as
    /// <see cref="CamusDBErrorCodes.TooManyAuthAttempts"/>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int LoginKdfMaxConcurrency { get; init; } = 8;

    /// <summary>Maximum failed-or-succeeded login attempts per normalized account per rolling minute
    /// before further attempts are rejected with <see cref="CamusDBErrorCodes.TooManyAuthAttempts"/>.</summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int LoginMaxAttemptsPerMinute { get; init; } = 20;

    /// <summary>Upper bound on the per-node authenticated-principal cache size, so random/invalid token
    /// ids cannot grow it without bound.</summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int AuthenticationCacheMaxEntries { get; init; } = 10_000;

    /// <summary>
    /// Upper bound on the login rate-limiter's tracked (account, source) keys. Beyond it the limiter
    /// purges expired windows and, if still full, fails login closed — so a flood of unique account or
    /// source values cannot grow memory without bound.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public int LoginRateLimitMaxEntries { get; init; } = 100_000;

    /// <summary>
    /// When authentication is enabled, refuse credential-bearing requests that arrive over a plaintext
    /// (non-TLS) connection, since a bearer token or password on the wire is trivially stolen. A
    /// loopback peer is exempted so single-host development works without certificates. Default
    /// <c>true</c> (secure by default); set false only for a trusted network where TLS is terminated
    /// elsewhere.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool RequireTlsWhenAuthEnabled { get; init; } = true;

    /// <summary>
    /// Shared secret authenticating node-to-node cluster forwarding routes (<c>/internal/*</c>) when
    /// authentication is enabled, so a public user cannot drive them. Sourced from an external secret /
    /// environment, common to every node. When authentication is enabled and this is empty, the
    /// internal routes are refused (fail-closed) — they must be configured before a cluster runs with
    /// auth on.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public string NodeSecret { get; init; } = "";

    /// <summary>
    /// Number of row ids buffered from a non-covering secondary-index scan before issuing one
    /// <c>GetRowsBatch</c> call. Batching collapses N sequential Kahuna actor round-trips into
    /// one per page, keeping primary-row fetches snapshot-consistent with the scan via the
    /// transaction's <c>ReadTimestamp</c>.
    ///
    /// <para>Increasing this value reduces the number of batch calls (fewer round-trips) at the
    /// cost of buffering more ids and deferring the first decoded row further from the first
    /// index entry seen. Decreasing it reduces latency for the first result but increases the
    /// per-batch call overhead. A value of 1 degrades to per-entry fetching (no batching).</para>
    ///
    /// Default: <c>64</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int IndexScanFetchBatchSize { get; init; } = 64;

    /// <summary>
    /// Maximum number of concurrent span workers a single full primary-row table scan may use.
    /// The scan keyspace is divided into disjoint row-id spans (uniform over the ObjectId
    /// timestamp segment, the same partitioning the TTL sweeper uses) and each worker streams
    /// and decodes one span into a bounded buffer while earlier spans are being consumed, so
    /// row order — and therefore every result — is identical to the sequential scan.
    ///
    /// <para>A value of <c>1</c> (the default) disables parallelism and executes the scan on
    /// the pull pipeline exactly as before. Values above 1 trade memory (one bounded row buffer
    /// per span) and Kahuna read concurrency for scan wall-clock. Read per statement from the
    /// database's live options snapshot, so a runtime change applies to the next query.</para>
    ///
    /// Default: <c>1</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int MaxQueryParallelism { get; init; } = 1;

    /// <summary>
    /// Largest hash-join build side (in rows) that may be broadcast to remote probe-span
    /// fragments when distributed query execution is enabled. The decision is made at
    /// execution time, after the build hash table exists, so it gates on the build's
    /// <b>actual</b> row count — never on a cardinality estimate. <c>0</c> disables broadcast
    /// joins entirely.
    ///
    /// <para>Node-scoped on purpose: broadcasting is a coordinator-local strategy choice
    /// (results are identical either way), so per-node divergence changes performance, not
    /// semantics. Read per statement from the database's live options snapshot, so a runtime
    /// change applies to the next query.</para>
    ///
    /// Default: <c>10000</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int BroadcastJoinMaxBuildRows { get; init; } = 10_000;

    /// <summary>
    /// Enables per-lock-acquisition debug log lines (<c>LogLevel.Debug</c>). When <c>false</c>
    /// (default), no lock-trace messages are emitted regardless of the host logging configuration.
    /// Enable only for targeted diagnostics — a busy workload emits one line per lock acquired.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool LockTracingEnabled { get; init; } = false;

    /// <summary>
    /// Enables per-step query-execution trace log lines (<c>LogLevel.Information</c>). When
    /// <c>false</c> (default), no step-trace messages are emitted. Enable only for targeted
    /// diagnostics — each query emits one line per plan step executed.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public bool QueryTracingEnabled { get; init; } = false;

    /// <summary>
    /// Maximum time, in milliseconds, allowed for a single POSIX-style regex match evaluation
    /// (<c>~</c>, <c>~*</c>, <c>!~</c>, <c>!~*</c>). A match that exceeds this limit throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> (in WHERE/HAVING) or
    /// <see cref="CamusDBErrorCodes.CheckConstraintViolation"/> (inside a CHECK constraint) to
    /// guard against ReDoS on pathological patterns.
    /// Set via <c>regex_match_timeout_ms</c> in <c>config.yml</c>. Default: <c>250</c> ms.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int RegexMatchTimeoutMs { get; init; } = 250;

    /// <summary>
    /// Maximum number of compiled <c>Regex</c> instances the engine caches, keyed by
    /// <c>(pattern, ignoreCase)</c>. When the cache is full, new patterns are still compiled
    /// and evaluated but the result is not stored. This bounds memory growth from many distinct
    /// one-off patterns while never failing a query because the cache is full.
    /// Set via <c>regex_cache_max_entries</c> in <c>config.yml</c>. Default: <c>1024</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int RegexCacheMaxEntries { get; init; } = 1024;

    /// <summary>
    /// Resolved Kahuna engine overrides from <c>config.yml</c>. Applied when constructing embedded
    /// nodes in cluster and standalone modes.
    /// </summary>
    public Config.Models.KahunaOptionsConfig Kahuna { get; init; } = new();

    /// <summary>
    /// Which configuration layer supplied each setting, keyed by the underscored YAML name (nested
    /// section keys use their dotted form, e.g. <c>kahuna.wal_sync_writes</c>). Populated by
    /// <see cref="Config.ConfigResolver.Resolve"/> and extended by the host for the environment-only
    /// settings it applies afterwards.
    ///
    /// <para>Purely diagnostic — nothing in the engine reads it to decide behavior; it exists so
    /// <c>SHOW VARIABLES</c> can report not just a value but the layer it came from. Absent keys were
    /// never overridden and are <see cref="Config.ConfigValueSource.Default"/>, which is why the
    /// default is an empty map rather than a fully-populated one: an options record built directly by
    /// a test has no provenance to report, and reporting "default" for all of it is correct.</para>
    /// </summary>
    public IReadOnlyDictionary<string, Config.ConfigValueSource> ValueSources { get; init; }
        = new Dictionary<string, Config.ConfigValueSource>(StringComparer.OrdinalIgnoreCase);

    // ──────────────────────────────────────────────────────────────────────────
    // Query result cache
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Enables the per-node in-memory query result cache (default <c>true</c>). The cache is
    /// opt-in per query — only a <c>SELECT</c> carrying a <c>{cache=…}</c> hint is ever cached —
    /// so with the feature on, hints work without extra configuration. When set to <c>false</c>,
    /// every <c>{cache=…}</c> hint is a bypass and no cache is constructed, so no per-write gate
    /// bookkeeping or cache memory is incurred. Set via <c>query_result_cache_enabled</c> in
    /// <c>config.yml</c>.
    /// </summary>
    [ConfigSetting(ConfigMutability.Restart, ConfigScope.Node)]
    public bool QueryResultCacheEnabled { get; init; } = true;

    /// <summary>
    /// Default TTL in milliseconds for cache entries that do not carry a per-hint TTL override.
    /// Entries that survive this interval without an invalidating write are expired by the
    /// background sweep. Default: 5 000 ms.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheDefaultTtlMs { get; init; } = 5_000;

    /// <summary>Maximum number of entries the cache may hold (LRU eviction when exceeded). Default: 1 024.</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheMaxEntries { get; init; } = 1_024;

    /// <summary>Maximum total bytes across all cached entries. Default: 64 MiB.</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public long QueryResultCacheMaxBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Maximum bytes for a single cache entry. Results larger than this are bypassed. Default: 1 MiB.</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public long QueryResultCacheMaxEntryBytes { get; init; } = 1 * 1024 * 1024;

    /// <summary>Maximum rows in a single cache entry. Results larger than this are bypassed. Default: 10 000.</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheMaxEntryRows { get; init; } = 10_000;

    /// <summary>
    /// Maximum number of dependencies (range + point + schema facts combined) per entry.
    /// When exceeded, the implementation must either promote to a coarser range or bypass
    /// — never store an entry with an incomplete dependency set. Default: 4 096.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheMaxDeps { get; init; } = 4_096;

    /// <summary>Maximum point-key dependencies per entry before promotion or bypass. Default: 2 048.</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheMaxPointDeps { get; init; } = 2_048;

    /// <summary>Maximum range dependencies per entry before promotion or bypass. Default: 256.</summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheMaxRanges { get; init; } = 256;

    /// <summary>
    /// How long, in milliseconds, a waiter in the single-flight gate may block before giving up
    /// and executing the query independently. Default: 250 ms.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheSingleFlightWaitMs { get; init; } = 250;

    /// <summary>
    /// Maximum number of keys probed during strict validation before the entry is treated as
    /// invalid and recomputed. Default: 10 000.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheStrictValidationMaxKeys { get; init; } = 10_000;

    /// <summary>
    /// How often, in milliseconds, the background sweep removes expired entries. Default: 10 000 ms.
    /// </summary>
    [ConfigSetting(ConfigMutability.Runtime, ConfigScope.Node)]
    public int QueryResultCacheSweepIntervalMs { get; init; } = 10_000;
}
