
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
    /// <summary>
    /// The directory where the database files and directories will be stored.
    /// </summary>
    public static string DataDirectory = Path.GetFullPath("Data");

    /// <summary>
    /// Minimum interval, in milliseconds, between background flushes of advisory table
    /// statistics (R8) to durable Kahuna storage, per table. Statistics are updated in
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
    /// Second slice (C3): secondary indexes whose key columns are all non-String ASCII-encoding
    /// types (Integer64/Float64/Bool/Id/Null) are also registered and range-locked. String-keyed
    /// indexes stay hash-routed until the persistence comparator is aligned (C3b). Kahuna
    /// auto-split/merge is not wired (logical range routing + per-range locks work without it).
    ///
    /// <b>Operational requirement (C6):</b> key-range routing requires <c>InitialPartitions ≥ 2</c>
    /// in <c>config.yml</c>. With a single partition the Kahuna registry call is a silent no-op
    /// (stays hash-routed, range locks transparently fall back to the single-partition hash path),
    /// so enabling this flag on a single-partition node is safe but has no effect. A startup
    /// warning is emitted when the flag is on and <c>InitialPartitions &lt; 2</c>. Production
    /// clusters must set <c>initial_partitions: 2</c> (or more) to activate key-range sharding.
    ///
    /// Default off. Toggle via the <c>CAMUS_KEY_RANGE_SHARDING</c> environment variable.
    /// </summary>
    public static bool KeyRangeShardingEnabled =
        string.Equals(Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING"), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING"), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Cluster-wide default isolation level applied when a transaction is begun without an
    /// explicit level. Individual transactions may override this via the begin-request field
    /// or via <c>SET TRANSACTION ISOLATION LEVEL …</c>.
    ///
    /// Default: <see cref="CamusIsolationLevel.ReadCommitted"/> — existing behaviour unchanged.
    /// Set to <see cref="CamusIsolationLevel.Serializable"/> to make every new transaction
    /// serializable unless it overrides this.
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
}
