/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;

namespace CamusDB.Workload.Cli;

/// <summary>
/// Options common to every workload verb: how to reach the server and which logical dataset to use.
/// </summary>
public abstract class CommonOptions
{
    [Option("endpoint", Required = true, HelpText = "Server endpoint, e.g. http://127.0.0.1:5096 (gRPC port). Comma-separated pool allowed.")]
    public string Endpoint { get; set; } = "";

    [Option("database", Required = true, HelpText = "Workload database name.")]
    public string Database { get; set; } = "";

    [Option("protocol", Default = "grpc", HelpText = "Wire protocol: grpc (default) or rest.")]
    public string Protocol { get; set; } = "grpc";

    [Option("seed", Default = 1847UL, HelpText = "Deterministic seed for ids, payloads, and operation selection.")]
    public ulong Seed { get; set; }

    [Option("rows", Default = 100_000L, HelpText = "Number of seeded rows across all workload tables.")]
    public long Rows { get; set; }

    [Option("tables", Default = 1, HelpText = "Number of workload tables the rows are spread over. 1 (default) uses workload_accounts; more use workload_accounts_00.. and put the dataset on every partition. Must be the same for init and run, and must not exceed --rows.")]
    public int Tables { get; set; }

    [Option("payload-bytes", Default = 256, HelpText = "Size of the deterministic payload string per row.")]
    public int PayloadBytes { get; set; }

    [Option("no-auto-prepare", Default = false, HelpText = "Append MaxAutoPrepare=0 to every connection string (read, write, setup), disabling client auto-prepare.")]
    public bool NoAutoPrepare { get; set; }

    [Option("request-timeout", HelpText = "Per-request timeout in seconds appended to every connection string (client default when omitted).")]
    public int? RequestTimeout { get; set; }
}

/// <summary>
/// Idempotently creates the database/table/index and seeds the deterministic dataset. Setup runs
/// entirely outside any measured interval.
/// </summary>
[Verb("init", HelpText = "Create schema and seed the deterministic dataset (idempotent).")]
public sealed class InitOptions : CommonOptions
{
    [Option("batch", Default = 500, HelpText = "Rows per seeding transaction.")]
    public int Batch { get; set; }
}

/// <summary>
/// Validates the schema/data fingerprint, warms up, runs the measured interval, drains in-flight
/// work, reconciles correctness, and writes results.
/// </summary>
[Verb("run", HelpText = "Run the measured mixed workload and write result artifacts.")]
public sealed class RunOptions : CommonOptions
{
    [Option("output", Required = true, HelpText = "Output directory for artifacts. Must not already exist.")]
    public string Output { get; set; } = "";

    [Option("mode", Default = "open", HelpText = "Load model: open (open-loop, target-ops) or closed (saturation, workers/sweep).")]
    public string Mode { get; set; } = "open";

    [Option("target-ops", Default = 800, HelpText = "Open-loop: submitted operations per second.")]
    public int TargetOps { get; set; }

    [Option("workers", Default = 64, HelpText = "Number of concurrent workers (in-flight ops in closed-loop).")]
    public int Workers { get; set; }

    [Option("concurrency-sweep", HelpText = "Closed-loop: comma-separated worker counts, e.g. 1,8,16,32,64,128.")]
    public string? ConcurrencySweep { get; set; }

    [Option("read-percent", Default = 60, HelpText = "Percent of operations that are read-only point reads.")]
    public int ReadPercent { get; set; }

    [Option("write-percent", Default = 40, HelpText = "Percent of operations that are optimistic read/write transactions.")]
    public int WritePercent { get; set; }

    [Option("writes-per-transaction", Default = 1, HelpText = "Row updates per write transaction (all within the worker's shard).")]
    public int WritesPerTransaction { get; set; }

    [Option("duration", Default = "5m", HelpText = "Measured interval, e.g. 5m, 30s, 10s.")]
    public string Duration { get; set; } = "5m";

    [Option("warmup", Default = "30s", HelpText = "Warm-up period before measurement.")]
    public string Warmup { get; set; } = "30s";

    [Option("drain", Default = "10s", HelpText = "Drain period for in-flight work after measurement.")]
    public string Drain { get; set; } = "10s";

    [Option("connections", Default = 8, HelpText = "gRPC connections to open (each carries a small stream pool); widens client concurrency.")]
    public int Connections { get; set; }

    [Option("max-in-flight", Default = 4096, HelpText = "Open-loop: cap on pending+in-flight ops before a schedule drop is counted.")]
    public int MaxInFlight { get; set; }

    [Option("init-if-missing", Default = false, HelpText = "Seed the dataset first if absent (local convenience; setup is never measured).")]
    public bool InitIfMissing { get; set; }

    [Option("locking", Default = "optimistic", HelpText = "Write-transaction locking: optimistic (default) or pessimistic.")]
    public string Locking { get; set; } = "optimistic";

    [Option("isolation", Default = "read_committed", HelpText = "Write-transaction isolation: read_committed (default) or serializable.")]
    public string Isolation { get; set; } = "read_committed";

    [Option("expect-faults", Default = false, HelpText = "Chaos runs: conflicts and open-loop pacing shortfalls become validity warnings instead of INVALID, and reconciliation tolerates them.")]
    public bool ExpectFaults { get; set; }

    [Option("reconcile-timeout", Default = 600, HelpText = "Seconds reconciliation keeps retrying its aggregate reads while the cluster is still settling, before reporting 'could not verify'. Post-measurement only; it never extends the measured window.")]
    public int ReconcileTimeout { get; set; }

    [Option("no-row-attribution", Default = false, HelpText = "Transfer workloads: skip the per-row balance/version check and judge atomicity on SUM(balance) alone. The aggregate cannot see leaked writes that cancel out, so a run started with this flag can report PASS while atomicity is broken. It costs one full scan before the run and one after; use it only when that scan is genuinely unaffordable.")]
    public bool NoRowAttribution { get; set; }

    [Option("workload", Default = "accounts", HelpText = "Write shape: accounts (shard-disjoint read-modify-write, conflict-free), bank (contended transfers within the dataset with a conserved SUM(balance) invariant), or fanout (bank transfers whose two legs always land in different tables; needs --tables >= 2).")]
    public string Workload { get; set; } = "accounts";
}

/// <summary>
/// Removes only objects created by an explicitly named workload database. Refuses an empty, default,
/// or unrecognized target so a stray invocation can never drop an unrelated database.
/// </summary>
[Verb("cleanup", HelpText = "Drop the named workload database. Refuses empty/default/unknown targets.")]
public sealed class CleanupOptions : CommonOptions
{
    [Option("confirm", Required = true, HelpText = "Must equal the --database value to authorize the drop.")]
    public string Confirm { get; set; } = "";
}

/// <summary>
/// Generates <c>bottleneck-report.md</c> from an existing run's client artifacts plus a server
/// <c>/metrics</c> scrape captured under the same run id. Pure post-processing — connects to nothing.
/// </summary>
[Verb("report", HelpText = "Build bottleneck-report.md from a run's summary.json and a server /metrics scrape.")]
public sealed class ReportOptions
{
    [Option("output", Required = true, HelpText = "Run directory containing manifest.json and summary.json; the report is written here.")]
    public string Output { get; set; } = "";

    [Option("metrics", Required = true, HelpText = "Path to a scraped Prometheus /metrics text file from the server.")]
    public string Metrics { get; set; } = "";
}
