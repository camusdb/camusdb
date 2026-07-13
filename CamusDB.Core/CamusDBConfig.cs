
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions;

namespace CamusDB.Core;

public static class CamusDBConfig
{
    private static readonly string DefaultDataDirectory = Path.GetFullPath("Data");

    // AsyncLocal so each test's async context sees its own override without racing.
    private static readonly AsyncLocal<string?> TestDataDirectoryOverride = new();

    /// <summary>
    /// The directory where the database files and directories will be stored.
    /// Setting this in a test's SetUp affects only that test's async execution context.
    /// </summary>
    public static string DataDirectory
    {
        get => TestDataDirectoryOverride.Value ?? DefaultDataDirectory;
        set => TestDataDirectoryOverride.Value = value;
    }

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
    public static int StatsFlushIntervalMs = 5000;

    /// <summary>
    /// Row count threshold below which ANALYZE performs a full scan; above it, rows are
    /// sampled by reading the first N rows in storage order. 0 = always full scan.
    /// </summary>
    public static int StatsAnalyzeSampleRows = 100_000;

    /// <summary>
    /// Number of equi-depth histogram buckets ANALYZE builds per column.
    /// </summary>
    public static int StatsHistogramBuckets = 100;

    /// <summary>
    /// Max keys the non-transactional <c>DROP DATABASE</c> keyspace purge scans and deletes per batch.
    /// The purge pages through each bucket one batch at a time so peak memory is bounded to this many
    /// keys regardless of how large a database (or branch overlay) is — a full database drop can span
    /// every table and row. Kept modest so the purge never materialises an entire keyspace at once.
    /// </summary>
    public static int KeyspacePurgeBatchSize = 512;

    /// <summary>
    /// Lease duration, in milliseconds, of the Kahuna snapshot-floor hold a branch database
    /// acquires on its immediate parent at fork time. The hold pins the parent's MVCC history at
    /// <c>forkT</c> so branch as-of reads stay correct under aggressive revision retention. The
    /// hold must be renewed well inside this window for as long as the branch exists (see the
    /// leader-owned renewer); if renewal stops, the hold lapses after one lease and the branch's
    /// frozen view can be reclaimed. Chosen coarse so lease renewals are not a hot Raft path.
    /// </summary>
    public static int BranchSnapshotHoldLeaseMs = 300_000;

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
    public static int SqlParserCacheTtlSeconds = 300;

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
    public static int SqlParserCacheMaxEntries = 2048;

    /// <summary>
    /// How often, in seconds, the background sweep task removes expired SQL parser cache entries.
    /// Must be &gt; 0.
    /// Default: <c>60</c> seconds.
    /// </summary>
    public static int SqlParserCacheSweepSeconds = 60;

    /// <summary>
    /// Maximum number of rows the hash-join build phase may materialise before degrading, used
    /// only when <see cref="SpillEnabled"/> is <c>false</c>: the build falls back to a nested-loop
    /// join for correctness. When spill is enabled the build instead routes to the Grace/hybrid
    /// hash join once it exceeds <see cref="SpillEffectiveThreshold"/>, so this cap does not apply.
    /// Default: 1_000_000 rows.
    /// </summary>
    public static int HashJoinMaxBuildRows = 1_000_000;

    /// <summary>
    /// The internal name used to identify primary key indices.
    /// This name should only be changed in a new installation. Changing it after
    /// having databases with tables and data can cause unexpected problems.
    /// </summary>
    public const string PrimaryKeyInternalName = "~pk";

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
    public static bool KeyRangeShardingEnabled;

    /// <summary>
    /// Number of Raft data partitions active in this cluster. Populated from the
    /// <c>initial_partitions</c> config key at startup. Used by <see cref="PlacementReader"/>
    /// to approximate the remote-data fraction for <c>NetworkFactor</c> cost estimates.
    ///
    /// Default: 1 (single-partition / single-node). Tests that do not set this explicitly
    /// keep the single-node behaviour (NetworkFactor = 0).
    /// </summary>
    public static int ClusterPartitionCount = 1;

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
    public static double NetWeight = 0.01;

    /// <summary>
    /// Enables cost-based access-path selection in the query planner. When <c>true</c>, the
    /// planner enumerates all viable index steps, costs each against the full-scan baseline,
    /// and picks the cheapest. When <c>false</c> (default), the rule-based (score-based) path
    /// is used unchanged, so plans are byte-identical to the heuristic planner regardless of
    /// whether statistics have been collected. Set via <c>cost_based_access_path_enabled</c>
    /// in <c>config.yml</c>.
    /// </summary>
    public static bool CostBasedAccessPathEnabled = false;

    /// <summary>
    /// Enables cost-based join-order enumeration (System-R–style DP) in the query planner.
    /// When <c>true</c>, the planner costs all left-deep orderings and picks the cheapest
    /// using INLJ vs hash-join cost asymmetry. When <c>false</c> (default), the rule-based
    /// heuristic (<see cref="JoinOrderOptimizer"/>) is used and plans are byte-identical
    /// to today's output. Joins wider than <c>JoinEnumerator.MaxTablesForEnumeration</c>
    /// always fall back to the heuristic regardless of this flag. Set via
    /// <c>cost_based_join_order_enabled</c> in <c>config.yml</c>.
    /// </summary>
    public static bool CostBasedJoinOrderEnabled = false;

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
    public static bool PlanCacheEnabled = false;

    /// <summary>
    /// Maximum number of entries held by the per-process plan cache (LRU eviction when the
    /// limit is exceeded). Set via <c>plan_cache_max_entries</c> in <c>config.yml</c>.
    /// </summary>
    public static int PlanCacheMaxEntries = 512;

    /// <summary>
    /// Cluster-wide default isolation level applied when a transaction is begun without an
    /// explicit level. Individual transactions may override this via the begin-request field
    /// or via <c>SET TRANSACTION ISOLATION LEVEL …</c>.
    ///
    /// Default: <see cref="CamusIsolationLevel.Serializable"/> — every new transaction is
    /// serializable unless it overrides this. Set to <see cref="CamusIsolationLevel.ReadCommitted"/>
    /// (via <c>default_isolation_level: read_committed</c> in <c>config.yml</c>) to opt out.
    /// </summary>
    public static CamusIsolationLevel DefaultIsolationLevel = CamusIsolationLevel.Serializable;

    /// <summary>
    /// TTL, in milliseconds, granted to each Kahuna range lock acquired by a serializable
    /// read-write transaction. The range-lock heartbeat renews each lock before this window
    /// expires, so live transactions are not bounded by this value.
    ///
    /// <para>A zero or negative value tells Kahuna to hold the lock indefinitely (no TTL).
    /// Tests may lower this to exercise renewal under a short TTL.</para>
    ///
    /// Default: 30 000 ms (30 s).
    /// </summary>
    public static int RangeLockExpiresMs = 30_000;

    /// <summary>
    /// How often, in milliseconds, the background range-lock heartbeat re-acquires every range
    /// lock held by a live Serializable+RW transaction. Must be well under
    /// <see cref="RangeLockExpiresMs"/> to guarantee renewal before expiry.
    ///
    /// <para>Tests may lower this to exercise renewal without waiting 30 s.</para>
    ///
    /// Default: 10 000 ms (10 s) — a third of the 30 s range-lock TTL.
    /// </summary>
    public static int RangeLockHeartbeatIntervalMs = 10_000;

    /// <summary>
    /// Maximum wall-clock lifetime, in milliseconds, for a <see cref="CamusIsolationLevel.Serializable"/>
    /// + <see cref="CamusTransactionMode.ReadWrite"/> transaction. Acts as an absolute backstop: once
    /// this duration elapses from <c>BeginAsync</c>, any subsequent operation (range lock acquisition,
    /// commit) throws <see cref="CamusDBErrorCodes.TransactionLifetimeExceeded"/>.
    ///
    /// <para>With the range-lock heartbeat active, range locks never expire due to TTL; this cap only
    /// bounds runaway transactions. A zero or negative value disables the deadline (useful in tests).</para>
    ///
    /// Default: 3 600 000 ms (1 hour).
    /// </summary>
    public static int MaxSerializableTransactionLifetimeMs = 3_600_000;

    /// <summary>
    /// How long, in milliseconds, an explicit (client-driven) transaction may sit idle — no
    /// statement issued against it — before the background reaper rolls it back and releases its
    /// locks. "Idle" is measured from the last time the client referenced the transaction (its
    /// last statement, or its begin), using a monotonic clock immune to wall-clock jumps.
    ///
    /// <para>This targets abandoned transactions: a client that opens a transaction and then
    /// disconnects, crashes, or forgets to commit/rollback. Such a transaction is otherwise never
    /// finalized, so its Serializable+RW range-lock heartbeat would renew its locks forever and
    /// every conflicting transaction would keep aborting. The reaper reclaims it — freeing the
    /// locks, rolling back the Kahuna transaction, and dropping it from the in-flight map.</para>
    ///
    /// <para>Only transactions tracked by the HTTP transaction coordinator (explicit
    /// <c>/start-transaction</c> sessions and promoted read-only scans) are subject to the reaper;
    /// single-statement autocommit requests finalize within their own request and are never
    /// tracked. A pause longer than this window inside an interactive session is treated as
    /// abandonment and rolled back.</para>
    ///
    /// <para><c>&lt;= 0</c> disables the reaper entirely. Even when disabled, an abandoned
    /// Serializable+RW transaction's locks are still bounded by
    /// <see cref="MaxSerializableTransactionLifetimeMs"/> — the heartbeat stops renewing once that
    /// cap is passed — so a disabled reaper degrades the worst case from "prompt cleanup" to
    /// "reclaimed at the next TTL after the lifetime cap", never back to a permanent hold.</para>
    ///
    /// Default: 300 000 ms (5 minutes).
    /// </summary>
    public static int TransactionIdleTimeoutMs = 300_000;

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
    public static int TransactionReaperIntervalMs = 30_000;

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
    public static int LockEscalationThreshold = 50;

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
    public static int MaxMutationsPerTransaction = 20_000;

    /// <summary>
    /// Wall-clock cap, in milliseconds, for a single lock-acquire retry loop during Serializable
    /// conflicts. Bounds deadlock and persistent lock-conflict latency per operation.
    /// Default: 500 ms.
    /// </summary>
    public static int LockWaitDeadlineMs = 500;

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
    public static int MaxIdentifierLength = 64;

    /// <summary>
    /// Maximum number of user-declared columns allowed per table. Counts the columns visible in
    /// <c>CREATE TABLE</c> and after each <c>ALTER TABLE ADD COLUMN</c>. Internal reserved columns
    /// (e.g. <c>_id</c>) are not user-declarable and do not count toward the cap.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>512</c>.
    /// </summary>
    public static int MaxColumnsPerTable = 512;

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
    public static bool SpillEnabled = false;

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
    public static int SpillThresholdRows = 500_000;

    /// <summary>
    /// Maximum number of simultaneously-open spill-run readers during a k-way merge pass.
    /// When the number of spilled runs exceeds this value, a multi-pass merge is performed:
    /// runs are merged in groups of <c>SpillMergeFanIn</c> until a single merged output remains.
    ///
    /// Ignored when <see cref="SpillEnabled"/> is <c>false</c>.
    ///
    /// Default: <c>16</c>.
    /// </summary>
    public static int SpillMergeFanIn = 16;

    /// <summary>
    /// <b>Test-only override.</b> When non-null, replaces <see cref="SpillThresholdRows"/> with
    /// this value for every operator, forcing spill on tiny inputs so the spill code path runs
    /// deterministically in unit tests without large data sets. Has no production use.
    ///
    /// Operators read this via <see cref="SpillEffectiveThreshold"/>.
    /// </summary>
    public static int? ForceSpillThresholdRows = null;

    /// <summary>
    /// Returns the effective spill-row threshold: <see cref="ForceSpillThresholdRows"/> when set
    /// (test override), otherwise <see cref="SpillThresholdRows"/>.
    /// </summary>
    public static int SpillEffectiveThreshold =>
        ForceSpillThresholdRows ?? SpillThresholdRows;

    /// <summary>
    /// Maximum number of user-visible secondary indexes allowed per table. The implicit primary-key
    /// index (<c>~pk</c>) and any internal <c>~</c>-prefixed indexes are exempt and do not count.
    /// Checked on each <c>ALTER INDEX ADD INDEX / ADD UNIQUE</c> operation.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>64</c> (matches MySQL's per-table secondary-index cap).
    /// </summary>
    public static int MaxIndexesPerTable = 64;

    /// <summary>
    /// Maximum number of tables allowed in a single database. Checked at <c>CREATE TABLE</c>
    /// time against the database's current persisted table set. <c>CREATE TABLE IF NOT EXISTS</c>
    /// that resolves to an already-existing table is exempt — the table already counts.
    /// <para><c>&lt;= 0</c> — limit is disabled.</para>
    /// Default: <c>10000</c>.
    /// </summary>
    public static int MaxTablesPerDatabase = 10_000;

    /// <summary>
    /// Default maximum length (in UTF-16 <c>string.Length</c> characters) for a <c>String</c>
    /// column declared without an explicit <c>string(N)</c> bound.
    /// Applied at write-validation time (T7); stored as <c>null</c> in the schema metadata.
    /// Value: 2 621 440 characters (~5 MB in the worst-case UTF-16 encoding).
    /// </summary>
    public const int DefaultStringMaxLength = 2_621_440;

    /// <summary>
    /// Default maximum payload length (in bytes) for a <c>Bytes</c> column declared without
    /// an explicit bound.
    /// Applied at write-validation time (T7); stored as <c>null</c> in the schema metadata.
    /// Value: 10 485 760 bytes (10 MB).
    /// </summary>
    public const int DefaultBytesMaxLength = 10_485_760;

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
    public static int IndexScanFetchBatchSize = 64;

    /// <summary>
    /// Enables per-lock-acquisition debug log lines (<c>LogLevel.Debug</c>). When <c>false</c>
    /// (default), no lock-trace messages are emitted regardless of the host logging configuration.
    /// Enable only for targeted diagnostics — a busy workload emits one line per lock acquired.
    /// </summary>
    public static bool LockTracingEnabled = false;

    /// <summary>
    /// Enables per-step query-execution trace log lines (<c>LogLevel.Information</c>). When
    /// <c>false</c> (default), no step-trace messages are emitted. Enable only for targeted
    /// diagnostics — each query emits one line per plan step executed.
    /// </summary>
    public static bool QueryTracingEnabled = false;

    /// <summary>
    /// Maximum time, in milliseconds, allowed for a single POSIX-style regex match evaluation
    /// (<c>~</c>, <c>~*</c>, <c>!~</c>, <c>!~*</c>). A match that exceeds this limit throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> (in WHERE/HAVING) or
    /// <see cref="CamusDBErrorCodes.CheckConstraintViolation"/> (inside a CHECK constraint) to
    /// guard against ReDoS on pathological patterns.
    /// Default: <c>250</c> ms.
    /// </summary>
    public static int RegexMatchTimeoutMs = 250;

    /// <summary>
    /// Maximum number of compiled <c>Regex</c> instances the engine caches, keyed by
    /// <c>(pattern, ignoreCase)</c>. When the cache is full, new patterns are still compiled
    /// and evaluated but the result is not stored. This bounds memory growth from many distinct
    /// one-off patterns while never failing a query because the cache is full.
    /// Default: <c>1024</c>.
    /// </summary>
    public static int RegexCacheMaxEntries = 1024;

    /// <summary>
    /// Resolved Kahuna engine overrides from <c>config.yml</c>. Applied when constructing embedded
    /// nodes in cluster and standalone modes.
    /// </summary>
    public static Config.Models.KahunaOptionsConfig Kahuna = new();

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
    public static bool QueryResultCacheEnabled = true;

    /// <summary>
    /// Default TTL in milliseconds for cache entries that do not carry a per-hint TTL override.
    /// Entries that survive this interval without an invalidating write are expired by the
    /// background sweep. Default: 5 000 ms.
    /// </summary>
    public static int QueryResultCacheDefaultTtlMs = 5_000;

    /// <summary>Maximum number of entries the cache may hold (LRU eviction when exceeded). Default: 1 024.</summary>
    public static int QueryResultCacheMaxEntries = 1_024;

    /// <summary>Maximum total bytes across all cached entries. Default: 64 MiB.</summary>
    public static long QueryResultCacheMaxBytes = 64 * 1024 * 1024;

    /// <summary>Maximum bytes for a single cache entry. Results larger than this are bypassed. Default: 1 MiB.</summary>
    public static long QueryResultCacheMaxEntryBytes = 1 * 1024 * 1024;

    /// <summary>Maximum rows in a single cache entry. Results larger than this are bypassed. Default: 10 000.</summary>
    public static int QueryResultCacheMaxEntryRows = 10_000;

    /// <summary>
    /// Maximum number of dependencies (range + point + schema facts combined) per entry.
    /// When exceeded, the implementation must either promote to a coarser range or bypass
    /// — never store an entry with an incomplete dependency set. Default: 4 096.
    /// </summary>
    public static int QueryResultCacheMaxDeps = 4_096;

    /// <summary>Maximum point-key dependencies per entry before promotion or bypass. Default: 2 048.</summary>
    public static int QueryResultCacheMaxPointDeps = 2_048;

    /// <summary>Maximum range dependencies per entry before promotion or bypass. Default: 256.</summary>
    public static int QueryResultCacheMaxRanges = 256;

    /// <summary>
    /// How long, in milliseconds, a waiter in the single-flight gate may block before giving up
    /// and executing the query independently. Default: 250 ms.
    /// </summary>
    public static int QueryResultCacheSingleFlightWaitMs = 250;

    /// <summary>
    /// Maximum number of keys probed during strict validation before the entry is treated as
    /// invalid and recomputed. Default: 10 000.
    /// </summary>
    public static int QueryResultCacheStrictValidationMaxKeys = 10_000;

    /// <summary>
    /// How often, in milliseconds, the background sweep removes expired entries. Default: 10 000 ms.
    /// </summary>
    public static int QueryResultCacheSweepIntervalMs = 10_000;
}
