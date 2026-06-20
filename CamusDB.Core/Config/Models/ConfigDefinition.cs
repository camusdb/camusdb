
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Config.Models;

public class ConfigDefinition
{
    public string DataDir { get; set; } = "";

    public string Mode { get; set; } = "standalone";

    public string NodeName { get; set; } = "";

    public string RaftHost { get; set; } = "localhost";

    public int RaftPort { get; set; } = 7070;

    public int InitialPartitions { get; set; } = 1;

    public List<string> Peers { get; set; } = [];

    /// <summary>
    /// Per-peer HTTP base addresses, parallel to <see cref="Peers"/>.
    /// Entry i is the HTTP URL for the node whose Raft endpoint is Peers[i].
    /// When populated and Peers.Count == HttpPeers.Count, the endpoint map uses
    /// these explicit addresses instead of the uniform-port fallback.
    /// Format: "host:httpPort" (e.g. "192.168.1.10:5095").
    /// </summary>
    public List<string> HttpPeers { get; set; } = [];

    /// <summary>
    /// How long a DDL proposer waits for every live node to ack the previous schema
    /// version before giving up (the two-version gate). Milliseconds; must be &gt; 0.
    /// Maps to <c>EmbeddedKahuna.SchemaAckWaitTimeout</c>.
    /// </summary>
    public int SchemaAckWaitTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// How long since the schema leader last heard from a live member (via Raft activity)
    /// before the gate presumes it down and stops blocking on it. Milliseconds, or <c>-1</c>
    /// for an infinite lease (strict — wait on every configured member, no DDL liveness under a
    /// node failure). Must be &gt; 0 or exactly -1. Default 30 s, so a node death does not freeze
    /// subsequent DDL while a healthy-but-idle follower is never false-evicted.
    /// Maps to <c>EmbeddedKahuna.SchemaAckLiveNodeLease</c>.
    /// </summary>
    public int SchemaAckLiveNodeLeaseMs { get; set; } = 30_000;

    /// <summary>
    /// Minimum interval between background flushes of advisory table statistics to
    /// durable storage, in milliseconds, per table. <c>0</c> flushes after every change;
    /// <c>-1</c> disables auto-flush (persist only on explicit flush / close); a positive
    /// value caps flush frequency. Maps to <c>CamusDBConfig.StatsFlushIntervalMs</c>.
    /// </summary>
    public int StatsFlushIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Sliding TTL for the SQL parser AST cache, in seconds (PC1/PC3).
    /// Each cache hit extends the deadline by this interval.
    /// <c>0</c> disables the cache entirely (every parse re-lexes from scratch).
    /// Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBConfig.SqlParserCacheTtlSeconds</c>.
    /// </summary>
    public int SqlParserCacheTtlSeconds { get; set; } = 300;

    /// <summary>
    /// Maximum number of distinct SQL texts the parser AST cache may hold (PC2/PC3).
    /// When the cap is reached, new statements are silently skipped until the background
    /// sweep reclaims expired entries. <c>0</c> = unbounded (no cap).
    /// Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBConfig.SqlParserCacheMaxEntries</c>.
    /// </summary>
    public int SqlParserCacheMaxEntries { get; set; } = 2048;

    /// <summary>
    /// How often, in seconds, the background sweep task removes expired SQL parser cache
    /// entries (PC2/PC3). Must be <c>&gt; 0</c>.
    /// Maps to <c>CamusDBConfig.SqlParserCacheSweepSeconds</c>.
    /// </summary>
    public int SqlParserCacheSweepSeconds { get; set; } = 60;

    /// <summary>Numeric Raft node id for cluster mode. Maps to <c>EmbeddedKahunaOptions.NodeId</c>.</summary>
    public int RaftNodeId { get; set; } = 1;

    /// <summary>HTTP API listen port. Default 5095.</summary>
    public int HttpPort { get; set; } = 5095;

    /// <summary>HTTPS API listen port when a certificate is configured. Default 7141.</summary>
    public int HttpsPort { get; set; } = 7141;

    /// <summary>Path to a PFX certificate for the HTTPS API port; empty disables HTTPS.</summary>
    public string HttpsCertificate { get; set; } = "";

    /// <summary>Path to a PFX certificate for the Raft gRPC port in cluster mode.</summary>
    public string RaftCertificate { get; set; } = "";

    /// <summary>
    /// Cluster-wide default isolation when a transaction omits an explicit level:
    /// <c>serializable</c> or <c>read_committed</c>.
    /// </summary>
    public string DefaultIsolationLevel { get; set; } = "serializable";

    /// <summary>Range-lock TTL in milliseconds. &lt;= 0 disables expiry. Default 30 000.</summary>
    public int RangeLockExpiresMs { get; set; } = 30_000;

    /// <summary>Range-lock heartbeat interval in milliseconds. Must be &lt; <see cref="RangeLockExpiresMs"/> when expiry is enabled.</summary>
    public int RangeLockHeartbeatIntervalMs { get; set; } = 10_000;

    /// <summary>Absolute Serializable+RW transaction lifetime cap in milliseconds. &lt;= 0 disables.</summary>
    public int MaxSerializableTransactionLifetimeMs { get; set; } = 3_600_000;

    /// <summary>Per-bucket shared-point-lock count before whole-bucket escalation. Default 50.</summary>
    public int LockEscalationThreshold { get; set; } = 50;

    /// <summary>Wall-clock cap per lock-acquire retry loop for Serializable conflicts. Default 500 ms.</summary>
    public int LockWaitDeadlineMs { get; set; } = 500;

    /// <summary>
    /// Opt tables into Kahuna key-range routing. Overridden by <c>CAMUS_KEY_RANGE_SHARDING</c> when set.
    /// </summary>
    public bool KeyRangeSharding { get; set; }

    /// <summary>Max UTF-16 length for any identifier (db, table, column, index). &lt;= 0 disables. Default 64.</summary>
    public int MaxIdentifierLength { get; set; } = 64;

    /// <summary>Max user-declared columns per table. &lt;= 0 disables. Default 512.</summary>
    public int MaxColumnsPerTable { get; set; } = 512;

    /// <summary>Max user-visible secondary indexes per table (PK exempt). &lt;= 0 disables. Default 64.</summary>
    public int MaxIndexesPerTable { get; set; } = 64;

    /// <summary>Max tables per database. &lt;= 0 disables. Default 10 000.</summary>
    public int MaxTablesPerDatabase { get; set; } = 10_000;

    /// <summary>Allow-listed Kahuna engine tunables for cluster and standalone nodes.</summary>
    public KahunaOptionsConfig Kahuna { get; set; } = new();

    public bool IsClusterMode => Mode == "cluster" || Peers.Count > 0;

    /// <summary>Parses <see cref="DefaultIsolationLevel"/> to the engine enum.</summary>
    public CamusIsolationLevel ParseDefaultIsolationLevel()
    {
        return DefaultIsolationLevel switch
        {
            "serializable" => CamusIsolationLevel.Serializable,
            "read_committed" => CamusIsolationLevel.ReadCommitted,
            _ => throw Invalid(
                "'default_isolation_level' must be 'serializable' or 'read_committed', got '" +
                DefaultIsolationLevel + "'"),
        };
    }

    /// <summary>
    /// Validates the configuration, throwing <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.InvalidConfig"/> on the first problem found.
    /// Called by <c>ConfigReader.Read</c> so a malformed config fails fast at startup
    /// rather than producing confusing behaviour later (e.g. a zero ack timeout that
    /// makes the two-version gate give up instantly).
    /// </summary>
    public void Validate()
    {
        if (Mode is not ("standalone" or "cluster"))
            throw Invalid($"'mode' must be 'standalone' or 'cluster', got '{Mode}'");

        if (RaftPort is <= 0 or > 65535)
            throw Invalid($"'raft_port' must be in 1..65535, got {RaftPort}");

        if (InitialPartitions < 1)
            throw Invalid($"'initial_partitions' must be >= 1, got {InitialPartitions}");

        if (SchemaAckWaitTimeoutMs <= 0)
            throw Invalid(
                $"'schema_ack_wait_timeout_ms' must be > 0, got {SchemaAckWaitTimeoutMs}");

        if (SchemaAckLiveNodeLeaseMs is not (-1 or > 0))
            throw Invalid(
                "'schema_ack_live_node_lease_ms' must be > 0 or -1 (infinite), got " +
                SchemaAckLiveNodeLeaseMs);

        if (StatsFlushIntervalMs < -1)
            throw Invalid(
                "'stats_flush_interval_ms' must be >= 0 (interval), 0 (immediate), or -1 " +
                $"(disabled), got {StatsFlushIntervalMs}");

        if (SqlParserCacheTtlSeconds < 0)
            throw Invalid(
                $"'sql_parser_cache_ttl_seconds' must be >= 0 (0 = disabled), got {SqlParserCacheTtlSeconds}");

        if (SqlParserCacheMaxEntries < 0)
            throw Invalid(
                $"'sql_parser_cache_max_entries' must be >= 0 (0 = unbounded), got {SqlParserCacheMaxEntries}");

        if (SqlParserCacheSweepSeconds <= 0)
            throw Invalid(
                $"'sql_parser_cache_sweep_seconds' must be > 0, got {SqlParserCacheSweepSeconds}");

        if (HttpPort is <= 0 or > 65535)
            throw Invalid($"'http_port' must be in 1..65535, got {HttpPort}");

        if (HttpsPort is <= 0 or > 65535)
            throw Invalid($"'https_port' must be in 1..65535, got {HttpsPort}");

        if (RaftNodeId <= 0)
            throw Invalid($"'raft_node_id' must be > 0, got {RaftNodeId}");

        if (DefaultIsolationLevel is not ("serializable" or "read_committed"))
            throw Invalid(
                "'default_isolation_level' must be 'serializable' or 'read_committed', got '" +
                DefaultIsolationLevel + "'");

        if (RangeLockExpiresMs > 0 && RangeLockHeartbeatIntervalMs >= RangeLockExpiresMs)
            throw Invalid(
                "'range_lock_heartbeat_interval_ms' must be < 'range_lock_expires_ms' when expiry is enabled");

        if (LockEscalationThreshold <= 0)
            throw Invalid($"'lock_escalation_threshold' must be > 0, got {LockEscalationThreshold}");

        if (LockWaitDeadlineMs <= 0)
            throw Invalid($"'lock_wait_deadline_ms' must be > 0, got {LockWaitDeadlineMs}");

        Kahuna.Validate();

        // Forwarding endpoints: http_peers, when supplied, must be parallel to peers so
        // the raft-endpoint → HTTP base-URI map in Program.cs is unambiguous. An entry
        // count mismatch silently disables the explicit map and falls back to the
        // uniform-port heuristic, which is a misconfiguration we should surface here.
        if (HttpPeers.Count > 0 && HttpPeers.Count != Peers.Count)
            throw Invalid(
                $"'http_peers' has {HttpPeers.Count} entr{(HttpPeers.Count == 1 ? "y" : "ies")} " +
                $"but 'peers' has {Peers.Count}; they must be parallel (one http address per peer)");

        foreach (string peer in Peers)
            ValidateHostPort(peer, "peers");

        foreach (string httpPeer in HttpPeers)
            ValidateHostPort(httpPeer, "http_peers");
    }

    private static void ValidateHostPort(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw Invalid($"'{field}' contains an empty entry");

        int colon = value.LastIndexOf(':');
        if (colon <= 0 || colon == value.Length - 1)
            throw Invalid($"'{field}' entry '{value}' must be in 'host:port' format");

        string portPart = value[(colon + 1)..];
        if (!int.TryParse(portPart, out int port) || port is <= 0 or > 65535)
            throw Invalid($"'{field}' entry '{value}' has an invalid port '{portPart}'");
    }

    private static CamusDBException Invalid(string message)
        => new(CamusDBErrorCodes.InvalidConfig, message);
}
