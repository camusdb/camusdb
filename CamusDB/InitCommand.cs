
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config;

namespace CamusDB;

/// <summary>
/// Implements <c>camusdb init</c>: writes a starter configuration to the user configuration
/// directory and creates the default data directory.
/// <para>
/// This is what makes an operator's settings safe from a repository checkout. The configuration
/// shipped in the repository is a 26 KB commented reference, not a starting point — it is a tracked
/// file, so editing it in place means a <c>git pull</c> can replace it, and its size makes it a poor
/// thing to diff. What <c>init</c> writes instead is a short file of active keys under the user's
/// home directory, which no checkout can reach.
/// </para>
/// <para>
/// Dispatched from a bare first argument rather than a CommandLineParser verb; see the call site in
/// <c>Program.cs</c> for why the server's flag parsing must stay untouched.
/// </para>
/// </summary>
internal static class InitCommand
{
    /// <summary>True when the process was invoked as <c>camusdb init [...]</c>.</summary>
    internal static bool Matches(string[] args)
        => args.Length > 0 && string.Equals(args[0], "init", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Writes the starter configuration. Returns the process exit code: non-zero when the target
    /// already exists and <c>--force</c> was not given, so a script cannot mistake a refusal for a
    /// successful write.
    /// </summary>
    internal static int Run(string[] args)
    {
        bool force = args.Any(a =>
            string.Equals(a, "--force", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(a, "-f", StringComparison.Ordinal));

        string configPath = ConfigLocator.UserConfigPath();
        string dataDirectory = ConfigLocator.DefaultDataDirectory();

        if (File.Exists(configPath) && !force)
        {
            Console.Error.WriteLine($"{configPath} already exists. Re-run with --force to overwrite it.");
            return 1;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(configPath)!);
        File.WriteAllText(configPath, Template(dataDirectory));
        Directory.CreateDirectory(dataDirectory);

        Console.WriteLine($"Wrote {configPath}");
        Console.WriteLine($"Created {dataDirectory}");
        Console.WriteLine();
        Console.WriteLine("Run 'camusdb' to start a node with this configuration.");
        Console.WriteLine("The full set of options is documented at https://camusdb.github.io/docs/configuration/");

        return 0;
    }

    /// <summary>
    /// The starter file: only keys an operator is likely to want on day one, each on one line, with
    /// the reference material left to the documentation rather than reproduced here.
    /// </summary>
    private static string Template(string dataDirectory) =>
        $"""
        # CamusDB configuration.
        #
        # This file is yours: CamusDB never rewrites it, and no repository checkout can replace it.
        # Only a handful of options appear below — every setting, with its defaults and rationale, is
        # documented at https://camusdb.github.io/docs/configuration/
        #
        # Precedence (highest wins): CLI flag > environment variable > this file > built-in default.

        # Where databases are stored. Back this up; it is the whole state of the node.
        data_dir: {dataDirectory}

        # standalone (single node) or cluster (Raft; requires node_name, raft_* and peers).
        mode: standalone

        # HTTP API port. The gRPC endpoint is on grpc_port (default 5096).
        http_port: 5095

        # Raft partitions. A standalone node wants 1: with one disk there is a single fsync target,
        # so a single partition makes every transaction single-participant and enables the one-phase
        # commit fast path. Raise this only for a cluster, where partitions map to distinct leaders.
        initial_partitions: 1

        """;
}
