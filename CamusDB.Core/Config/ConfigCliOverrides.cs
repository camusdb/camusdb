
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// Nullable CLI override surface used by <see cref="ConfigResolver"/> so only explicitly
/// provided flags replace YAML values (CLI &gt; env &gt; YAML &gt; default).
/// </summary>
public sealed class ConfigCliOverrides
{
    public string? Mode { get; init; }

    /// <summary><c>prod</c> or <c>dev</c>; validated when the resolved definition is validated.</summary>
    public string? MemoryProfile { get; init; }

    public string? DataDir { get; init; }

    public string? NodeName { get; init; }

    public int? RaftNodeId { get; init; }

    public string? RaftHost { get; init; }

    public int? RaftPort { get; init; }

    public int? InitialPartitions { get; init; }

    public IReadOnlyList<string>? Peers { get; init; }

    public IReadOnlyList<string>? HttpPeers { get; init; }

    /// <summary>
    /// Null leaves the YAML/default value alone; <c>true</c> makes the node join a running cluster
    /// (seeded from the peer list) instead of bootstrapping one.
    /// </summary>
    public bool? JoinExisting { get; init; }

    public int? SchemaAckWaitTimeoutMs { get; init; }

    public int? SchemaAckLiveNodeLeaseMs { get; init; }

    public int? HttpPort { get; init; }

    public int? HttpsPort { get; init; }

    public string? HttpsCertificate { get; init; }

    public string? RaftCertificate { get; init; }

    /// <summary>
    /// Null leaves the YAML/default value alone; <c>false</c> lets a node behind a TLS-terminating
    /// proxy accept forwarded plaintext requests while authentication is on.
    /// </summary>
    public bool? RequireTlsWhenAuthEnabled { get; init; }
}
