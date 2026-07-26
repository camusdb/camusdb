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
/// the exact seeded data.
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
    int Workers,
    int Connections,
    int TargetOps,
    int ReadPercent,
    int WritePercent,
    int WritesPerTransaction,
    string SchemaFingerprint,
    string StartedAtUtc,
    string Runtime,
    string Os,
    int ProcessorCount,
    string ClientPackageVersion);
