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
    bool ExpectFaults,
    string SchemaFingerprint,
    string StartedAtUtc,
    string Runtime,
    string Os,
    int ProcessorCount,
    string ClientPackageVersion);
