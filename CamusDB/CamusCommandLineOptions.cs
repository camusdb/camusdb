
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;

namespace CamusDB;

public class CamusCommandLineOptions
{
    [Option("mode", Default = "standalone", HelpText = "Run mode: standalone | cluster")]
    public string Mode { get; set; } = "standalone";

    [Option("raft-nodename", Required = false, HelpText = "Unique node name in the cluster")]
    public string RaftNodeName { get; set; } = "";

    [Option("raft-nodeid", Required = false, Default = 1, HelpText = "Unique numeric node id in the cluster")]
    public int RaftNodeId { get; set; } = 1;

    [Option("raft-host", Required = false, Default = "localhost", HelpText = "Host address for Raft communication")]
    public string RaftHost { get; set; } = "localhost";

    [Option("raft-port", Required = false, Default = 7070, HelpText = "Port for Raft communication")]
    public int RaftPort { get; set; } = 7070;

    [Option("initial-cluster", Required = false, HelpText = "Peer host:port list for static discovery", Separator = ' ')]
    public IEnumerable<string> InitialCluster { get; set; } = [];

    [Option("initial-cluster-partitions", Required = false, Default = 1, HelpText = "Number of Raft partitions")]
    public int InitialClusterPartitions { get; set; } = 1;

    [Option("http-port", Required = false, Default = 5095, HelpText = "Port for the HTTP API")]
    public int HttpPort { get; set; } = 5095;

    [Option("https-port", Required = false, Default = 7141, HelpText = "Port for the HTTPS API")]
    public int HttpsPort { get; set; } = 7141;

    [Option("raft-certificate", Required = false, Default = "", HelpText = "Path to PFX certificate for the Raft gRPC port")]
    public string RaftCertificate { get; set; } = "";
}
