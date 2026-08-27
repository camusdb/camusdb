/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Failure envelope shared by every dashboard endpoint. It matches the shape the rest of the HTTP
/// surface uses (<c>status</c> / <c>code</c> / <c>message</c>) so the browser can render one error
/// path, and so a card that is refused for privilege looks the same as one refused for any other
/// reason.
/// </summary>
public sealed class DashboardFailureResponse
{
    public string Status { get; set; } = "failed";

    public string? Code { get; set; }

    public string? Message { get; set; }
}

/// <summary>
/// Node identity plus the live load gauges, in one payload, because the page renders them as one
/// band and a second round trip would let the two halves disagree by a poll interval.
///
/// <para>Everything here is <b>node-local</b>. The load counters describe this process only, and the
/// readiness fields describe this node's own view of the cluster. Nothing is aggregated across
/// peers.</para>
/// </summary>
public sealed class DashboardSummaryResponse
{
    public string Status { get; set; } = "ok";

    // ── Identity ────────────────────────────────────────────────────────────
    public string LocalEndpoint { get; set; } = "";

    public string LocalRole { get; set; } = "";

    public bool Initialized { get; set; }

    public bool Ready { get; set; }

    public int HostedPartitions { get; set; }

    /// <summary>
    /// True while this node leads a partition with an open backfill-refusal episode or a failing
    /// snapshot rescue. Such a partition can still commit through its healthy quorum, so this is
    /// deliberately not a readiness condition — but it is the degraded state a probe cannot see.
    /// </summary>
    public bool CommitStalled { get; set; }

    public bool ClusterMode { get; set; }

    public bool AuthenticationEnabled { get; set; }

    public string DataDirectory { get; set; } = "";

    public string Version { get; set; } = "";

    public long UptimeSeconds { get; set; }

    // ── Load ────────────────────────────────────────────────────────────────

    /// <summary>Foreground data requests executing right now, from <c>ForegroundRequestGauge</c>.</summary>
    public int InFlightRequests { get; set; }

    /// <summary>Explicit HTTP transactions currently open on this node.</summary>
    public int ActiveTransactions { get; set; }

    public int PreparedStatements { get; set; }

    public long PreparedStatementBytes { get; set; }

    /// <summary>
    /// The poll interval the page should use for this panel, from
    /// <c>CamusDBOptions.DashboardRefreshSeconds</c>, already clamped. Served rather than hardcoded
    /// in the script so an operator can slow a busy node down without shipping a new page.
    /// </summary>
    public int RefreshSeconds { get; set; }

    /// <summary>
    /// True when the caller is a superuser, or when authentication is off. The page uses it to skip
    /// requesting the two privileged panels rather than provoking a 403 it already knows is coming.
    /// </summary>
    public bool CanReadPrivileged { get; set; }
}

/// <summary>One aggregated instrument, as <c>SHOW ENGINE STATS</c> reports it.</summary>
/// <param name="Source">Meter that published it: <c>camusdb.server</c>, <c>kommander</c> or <c>kahuna</c>.</param>
/// <param name="Metric">Instrument name, e.g. <c>camus.request.count</c>.</param>
/// <param name="Tags">Canonical tag string, empty when the instrument carries no tags.</param>
/// <param name="Kind">counter, histogram or gauge.</param>
/// <param name="Count">Number of samples recorded.</param>
/// <param name="Total">Sum, for a counter or histogram. Null for a gauge.</param>
/// <param name="Min">Smallest sample. Histogram only.</param>
/// <param name="Max">Largest sample. Histogram only.</param>
/// <param name="Last">Most recent sample, or the gauge's instantaneous value.</param>
public sealed record DashboardMetricRow(
    string Source,
    string Metric,
    string Tags,
    string Kind,
    long Count,
    double? Total,
    double? Min,
    double? Max,
    double? Last);

/// <summary>
/// The curated instrument set for the engine panel.
///
/// <para><b>Values are raw and cumulative.</b> The collector accumulates from process start and has
/// no reset, and the server holds no previous sample, so it cannot compute a rate. The browser keeps
/// the last sample per instrument and divides by the elapsed monotonic time. That is why
/// <see cref="MonotonicMs"/> is here: a wall clock can step, and a rate computed across a step is
/// wrong.</para>
/// </summary>
public sealed class DashboardMetricsResponse
{
    public string Status { get; set; } = "ok";

    /// <summary>
    /// False when <c>engine_metrics_enabled</c> is off. The row list is then empty. This is reported
    /// as a state rather than an error, because an empty list alone cannot be told apart from a node
    /// that has simply recorded nothing yet.
    /// </summary>
    public bool MetricsEnabled { get; set; }

    /// <summary>The node these numbers belong to. They are never cluster-wide.</summary>
    public string Node { get; set; } = "";

    /// <summary>Wall clock of the sample, for display only.</summary>
    public long SampledAtUnixMs { get; set; }

    /// <summary>Monotonic reading of the same instant. Compute every rate against this, not the clock.</summary>
    public long MonotonicMs { get; set; }

    /// <summary>Instruments dropped by the row cap, so a truncated panel never reads as a complete one.</summary>
    public int Omitted { get; set; }

    public List<DashboardMetricRow> Rows { get; set; } = new();
}

/// <summary>One database as the registry knows it. Reading this opens no database.</summary>
public sealed class DashboardDatabaseRow
{
    public string Name { get; set; } = "";

    public string Id { get; set; } = "";

    /// <summary>Name of the database this one was branched from, or null for a root database.</summary>
    public string? BranchedFrom { get; set; }

    /// <summary>
    /// Whether this node currently holds an open descriptor for it.
    ///
    /// <para>False is an ordinary state, not a fault, and it covers two different histories: a
    /// database nobody has opened since the node started was never in memory at all, and one that
    /// idle eviction reclaimed is exactly the eviction policy working. The page must therefore say
    /// "not loaded" rather than "evicted", which would assert an event that may never have happened
    /// — and it must not colour either state as a warning.</para>
    /// </summary>
    public bool Resident { get; set; }
}

/// <summary>The registry list, filtered to what the caller may see.</summary>
public sealed class DashboardDatabasesResponse
{
    public string Status { get; set; } = "ok";

    public List<DashboardDatabaseRow> Databases { get; set; } = new();
}

/// <summary>One relation inside a database, for the drill-down panel.</summary>
public sealed class DashboardTableRow
{
    public string Name { get; set; } = "";

    public string Kind { get; set; } = "table";
}

/// <summary>
/// Tables of one database. Requested on an explicit click only — see the endpoint's summary for why
/// this must never join the refresh timer.
/// </summary>
public sealed class DashboardTablesResponse
{
    public string Status { get; set; } = "ok";

    public string Database { get; set; } = "";

    public List<DashboardTableRow> Tables { get; set; } = new();
}

/// <summary>One setting, as <c>SHOW VARIABLES</c> reports it, secrets already masked.</summary>
/// <param name="Name">The underscored YAML key.</param>
/// <param name="Value">Effective value, masked for a secret.</param>
/// <param name="Default">Built-in default.</param>
/// <param name="Source">Which layer supplied the value.</param>
/// <param name="Mutability">runtime or restart.</param>
/// <param name="Scope">cluster or node.</param>
public sealed record DashboardVariableRow(
    string Name,
    string? Value,
    string? Default,
    string Source,
    string Mutability,
    string Scope);

/// <summary>One entry of the live cluster-settings overlay.</summary>
public sealed record DashboardClusterSettingRow(string Name, string? Value);

/// <summary>
/// The configuration panel: what this engine was built with, and what the cluster overlay currently
/// changes. Superuser only, matching the bar on both underlying statements.
/// </summary>
public sealed class DashboardConfigResponse
{
    public string Status { get; set; } = "ok";

    public List<DashboardVariableRow> Variables { get; set; } = new();

    /// <summary>
    /// False on an engine composed without a cluster-settings service. The overlay list is then
    /// empty. Reported rather than thrown, because a throw would blank a panel over a condition that
    /// is not an error.
    /// </summary>
    public bool OverlayAvailable { get; set; }

    public List<DashboardClusterSettingRow> ClusterSettings { get; set; } = new();
}

/// <summary>Answer to a dashboard sign-in attempt. It never echoes the password back.</summary>
public sealed class DashboardLoginResponse
{
    public string Status { get; set; } = "ok";

    public string? Code { get; set; }

    public string? Message { get; set; }

    /// <summary>Where the browser should go next on success.</summary>
    public string? Redirect { get; set; }
}
