
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public static class CamusDBConfig
{
    /// <summary>
    /// The directory where the database files and directories will be stored.
    /// </summary>
    public static string DataDirectory = Path.GetFullPath("Data");

    /// <summary>
    /// The maximum number of pages held on each bucket
    /// </summary>
    public static int BufferPoolSize = 65536 / Environment.ProcessorCount;

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
    /// Opt a table's row key space (<c>{tableId}:r</c>) into Kahuna key-range routing instead of
    /// the default hash routing. When enabled, <see cref="Commands.Executor.Controllers.TableOpener"/>
    /// registers the row space on the local node at open time (the Kahuna registry is node-local and
    /// not replicated, so every node opens-and-registers independently), and the row range-lock path
    /// switches from prefix locks to Kahuna range locks (prefix locks are rejected on ranged spaces).
    ///
    /// First slice: row space only — indexes stay hash-routed; Kahuna auto-split/merge is not wired
    /// (logical range routing + per-range locks work without it). Default off. Toggle via the
    /// <c>CAMUS_KEY_RANGE_SHARDING</c> environment variable.
    /// </summary>
    public static bool KeyRangeShardingEnabled =
        string.Equals(Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING"), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING"), "true", StringComparison.OrdinalIgnoreCase);
}
