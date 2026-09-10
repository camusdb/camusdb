/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Results;

/// <summary>
/// The run's provenance: everything needed to reproduce it and to refuse an invalid comparison against
/// a run made under different durability/settlement settings. Durability-relevant server configuration
/// is operator-supplied because the client cannot observe it; the fingerprint and seed tie the run to
/// the exact seeded data. Locking/isolation/auto-prepare/timeout and <c>ExpectFaults</c> are recorded
/// because runs under different concurrency-control or fault-tolerance settings are not comparable —
/// in particular, an <c>ExpectFaults</c> run had validity waivers active. The table count and the
/// workload kind are recorded for the same reason and because the run artifacts need them to be read:
/// the transfer ledger names rows by index, and only the table count says which table an index is in.
///
/// <para>Routing is recorded for the same reason and one more: a client silently ignores routing advice
/// naming a node outside its trust map, so a run configured for learned routing against a wrong or empty
/// map behaves exactly like a run with routing off, and nothing in its numbers says so. Recording both
/// the requested mode and the map is what lets a later reader tell those two runs apart.</para>
/// </summary>
public sealed record RunManifest(
    string ToolVersion,
    string? GitCommit,
    string Endpoint,
    string Database,
    string Protocol,
    string Mode,
    ulong Seed,
    long Rows,
    int PayloadBytes,
    int Tables,
    string WorkloadKind,
    int Workers,
    int Connections,
    int TargetOps,
    int ReadPercent,
    int WritePercent,
    int WritesPerTransaction,
    string Locking,
    string Isolation,
    bool NoAutoPrepare,
    int? RequestTimeoutSeconds,

    /// <summary>
    /// The learned-routing mode the run asked for (<c>Off</c>, <c>Learned</c>, <c>Auto</c>), or null when
    /// none was passed and the driver's own default was in force. This is the <em>requested</em> mode, not
    /// the resolved one: <c>Auto</c> demotes itself to <c>Off</c> when the trust map names fewer than two
    /// reachable addresses, and that demotion happens inside the client where the workload cannot see it.
    /// Read it together with <see cref="RoutingNodes"/>, which is what decides whether it could engage.
    /// </summary>
    string? RoutingMode,

    /// <summary>
    /// The routing trust map as passed (<c>identity=address</c> pairs), or null when none was configured.
    /// The client only ever dials addresses listed here and ignores advice naming anything else, so an
    /// empty or mistaken map is the difference between a routing run and a control run that looks
    /// identical in every other field.
    /// </summary>
    string? RoutingNodes,

    bool ExpectFaults,
    string SchemaFingerprint,
    string StartedAtUtc,
    string Runtime,
    string Os,
    int ProcessorCount,
    string ClientPackageVersion,

    /// <summary>Extra connection-string pairs appended verbatim to every client connection (e.g.
    /// <c>CoalescingDelay=0</c>), or null when none were passed. A client with different batching is a
    /// different client: the latency it measures is not comparable across values.</summary>
    string? ConnectionOptions = null);
