
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
    /// Sliding TTL for the SQL parser AST cache, in seconds (PC1).
    /// A successfully-parsed <c>NodeAst</c> is kept in the cache for this many seconds after
    /// the last hit; each cache hit extends the deadline by the same interval.
    /// <para>
    /// Special values:
    ///   <c>&lt;= 0</c> — cache is disabled; every call to <c>SQLParserProcessor.Parse</c>
    ///                    lexes and parses from scratch (pre-PC1 behaviour).
    /// </para>
    /// Default: <c>300</c> (5 minutes), matching Kahuna's script-cache TTL.
    /// Wired to <c>config.yml</c> in PC3.
    /// </summary>
    public static int SqlParserCacheTtlSeconds = 300;

    /// <summary>
    /// Maximum number of entries the SQL parser AST cache may hold at any moment (PC2).
    /// When the cache is at capacity new statements are silently dropped until the background
    /// sweep reclaims expired entries. This is a safety bound against floods of unique ad-hoc
    /// SQL, not a precise LRU.
    /// <para>
    ///   <c>0</c> — no cap (unbounded, same risk as Kahuna's pure-TTL cache).
    /// </para>
    /// Default: <c>10_000</c>.
    /// Wired to <c>config.yml</c> in PC3.
    /// </summary>
    public static int SqlParserCacheMaxEntries = 2048;

    /// <summary>
    /// How often, in seconds, the background sweep task removes expired SQL parser cache entries (PC2).
    /// Must be &gt; 0.
    /// Default: <c>60</c> seconds.
    /// Wired to <c>config.yml</c> in PC3.
    /// </summary>
    public static int SqlParserCacheSweepSeconds = 60;

    /// <summary>
    /// Maximum number of rows the hash-join build phase may materialise before falling back
    /// to nested-loop for correctness (disk spilling is not implemented).
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
    /// Resolved Kahuna engine overrides from <c>config.yml</c>. Applied when constructing embedded
    /// nodes in cluster and standalone modes.
    /// </summary>
    public static Config.Models.KahunaOptionsConfig Kahuna = new();
}
