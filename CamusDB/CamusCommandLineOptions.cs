
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;

namespace CamusDB;

/// <summary>
/// CLI flags for CamusDB startup. Nullable properties have no default so only explicitly
/// provided flags override <c>config.yml</c> (see <see cref="CamusDB.Core.Config.ConfigResolver"/>).
/// </summary>
public class CamusCommandLineOptions
{
    [Option("config", Required = false,
        HelpText = "Path to the YAML configuration file. When omitted, CamusDB looks for " +
                   "CAMUS_CONFIG_PATH, then ./camusdb.yml or ./Config/config.yml, then the user " +
                   "configuration (~/.camusdb/config.yml), and finally starts on built-in defaults")]
    public string? ConfigPath { get; set; }

    [Option("mode", Required = false, HelpText = "Run mode: standalone | cluster")]
    public string? Mode { get; set; }

    [Option("data-dir", Required = false, HelpText = "Directory where database files are stored")]
    public string? DataDir { get; set; }

    [Option("memory-profile", Required = false,
        HelpText = "How the unset cache budgets are sized: prod (default) scales them to the machine's " +
                   "memory; dev pins them to ~96 MiB of caches for a node sharing a developer machine. " +
                   "An explicit kahuna.* budget always wins over the profile")]
    public string? MemoryProfile { get; set; }

    [Option("raft-nodename", Required = false, HelpText = "Unique node name in the cluster")]
    public string? RaftNodeName { get; set; }

    [Option("raft-nodeid", Required = false, HelpText = "Unique numeric node id in the cluster")]
    public int? RaftNodeId { get; set; }

    [Option("raft-host", Required = false, HelpText = "Host address for Raft communication")]
    public string? RaftHost { get; set; }

    [Option("raft-port", Required = false, HelpText = "Port for Raft communication")]
    public int? RaftPort { get; set; }

    [Option("initial-cluster", Required = false, HelpText = "Peer host:port list for static discovery", Separator = ' ')]
    public IEnumerable<string>? InitialCluster { get; set; }

    [Option("initial-cluster-partitions", Required = false, HelpText = "Number of Raft partitions")]
    public int? InitialClusterPartitions { get; set; }

    [Option("http-peers", Required = false, HelpText = "Per-peer HTTP host:port list (parallel to --initial-cluster)", Separator = ' ')]
    public IEnumerable<string>? HttpPeers { get; set; }

    [Option("extension-assemblies", Required = false, Separator = ' ',
        HelpText = "Absolute paths of C# extension assemblies to load in SAFE mode")]
    public IEnumerable<string>? ExtensionAssemblies { get; set; }

    [Option("unsafe-extension-assemblies", Required = false, Separator = ' ',
        HelpText = "Absolute paths of C# extension assemblies to load without the SAFE check")]
    public IEnumerable<string>? UnsafeExtensionAssemblies { get; set; }

    [Option("join-existing", Required = false,
        HelpText = "true|false. Join a RUNNING cluster as a new node using --initial-cluster as the seed list, " +
                   "instead of bootstrapping via static discovery (default false). Never guessed: a would-be " +
                   "joiner booted without it forms a separate cluster")]
    public bool? JoinExisting { get; set; }

    [Option("pitr-bootstrap-from", Required = false,
        HelpText = "Backup id (GUID) of the leaf backup a joining node seeds itself from before it joins, so the " +
                   "leader sends it only the entries after the restored point instead of a full snapshot. " +
                   "Requires --join-existing true, kahuna.backup_dir, and a data_dir that holds the restored image")]
    public string? PitrBootstrapFrom { get; set; }

    [Option("pitr-bootstrap-target-time-ms", Required = false,
        HelpText = "Point in time (Unix epoch ms) --pitr-bootstrap-from seeds to; 0 = the latest point the chain " +
                   "covers. Must match the targetTimeMs the data_dir was restored with")]
    public long? PitrBootstrapTargetTimeMs { get; set; }

    [Option("schema-ack-wait-timeout-ms", Required = false, HelpText = "Schema two-version gate ack wait timeout (ms)")]
    public int? SchemaAckWaitTimeoutMs { get; set; }

    [Option("schema-ack-live-node-lease-ms", Required = false, HelpText = "Schema ack live-node lease (ms); -1 = infinite")]
    public int? SchemaAckLiveNodeLeaseMs { get; set; }

    [Option("http-port", Required = false, HelpText = "Port for the HTTP API")]
    public int? HttpPort { get; set; }

    [Option("https-port", Required = false, HelpText = "Port for the HTTPS API")]
    public int? HttpsPort { get; set; }

    [Option("https-certificate", Required = false, HelpText = "Path to PFX certificate for the HTTPS API port; omit to disable HTTPS")]
    public string? HttpsCertificate { get; set; }

    [Option("raft-certificate", Required = false, HelpText = "Path to PFX certificate for the Raft gRPC port")]
    public string? RaftCertificate { get; set; }

    [Option("require-tls-when-auth-enabled", Required = false,
        HelpText = "true|false. When authentication is enabled, refuse credential-bearing requests over plaintext " +
                   "(default true). Set false only behind a TLS-terminating proxy")]
    public bool? RequireTlsWhenAuthEnabled { get; set; }

    public CamusDB.Core.Config.ConfigCliOverrides ToOverrides()
    {
        IReadOnlyList<string>? peers = InitialCluster?.ToList();
        if (peers is { Count: 0 })
            peers = null;

        IReadOnlyList<string>? httpPeers = HttpPeers?.ToList();
        if (httpPeers is { Count: 0 })
            httpPeers = null;

        IReadOnlyList<string>? extensionAssemblies = ExtensionAssemblies?.ToList();
        if (extensionAssemblies is { Count: 0 })
            extensionAssemblies = null;

        IReadOnlyList<string>? unsafeExtensionAssemblies = UnsafeExtensionAssemblies?.ToList();
        if (unsafeExtensionAssemblies is { Count: 0 })
            unsafeExtensionAssemblies = null;

        return new CamusDB.Core.Config.ConfigCliOverrides
        {
            Mode = Mode,
            MemoryProfile = MemoryProfile,
            DataDir = DataDir,
            NodeName = RaftNodeName,
            RaftNodeId = RaftNodeId,
            RaftHost = RaftHost,
            RaftPort = RaftPort,
            InitialPartitions = InitialClusterPartitions,
            Peers = peers,
            HttpPeers = httpPeers,
            ExtensionAssemblies = extensionAssemblies,
            UnsafeExtensionAssemblies = unsafeExtensionAssemblies,
            JoinExisting = JoinExisting,
            PitrBootstrapFrom = PitrBootstrapFrom,
            PitrBootstrapTargetTimeMs = PitrBootstrapTargetTimeMs,
            SchemaAckWaitTimeoutMs = SchemaAckWaitTimeoutMs,
            SchemaAckLiveNodeLeaseMs = SchemaAckLiveNodeLeaseMs,
            HttpPort = HttpPort,
            HttpsPort = HttpsPort,
            HttpsCertificate = HttpsCertificate,
            RaftCertificate = RaftCertificate,
            RequireTlsWhenAuthEnabled = RequireTlsWhenAuthEnabled,
        };
    }
}
