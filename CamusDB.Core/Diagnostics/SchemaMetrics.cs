
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Lightweight, always-on process-wide counters for the distributed schema convergence path.
/// Unlike <see cref="SchemaDiag"/> (opt-in stderr traces for debugging a single run), these are
/// cheap monotonic counters meant to stay live in production so a lagging follower is observable
/// as a number, not just a one-off log line:
///
/// <list type="bullet">
/// <item><b>Fence rejections</b> — how often <see cref="Commands.Executor.Controllers.TableOpener"/>
/// rejected a read/DML with <c>SchemaCatchingUp</c> (CADB0503) because the node's applied schema
/// lagged the committed head by more than one version. A rising count means a node is repeatedly
/// behind and shedding load.</item>
/// <item><b>Quorum-backstop activations</b> — how often the post-commit schema-ack gate proceeded
/// on a majority instead of full convergence (a minority did not ack within the backstop window).
/// A rising count means DDL is regularly outrunning at least one follower.</item>
/// </list>
///
/// Counters are global (one process = one node) with a per-database breakdown for fence rejections.
/// There is no metrics exporter in the codebase yet; these are the in-process surface an operator
/// or test reads directly (and the natural hook point when an exporter is added). <see cref="Reset"/>
/// exists for test isolation.
/// </summary>
public static class SchemaMetrics
{
    private static long fenceRejections;
    private static long quorumBackstopActivations;

    private static readonly ConcurrentDictionary<string, long> fenceRejectionsByDb =
        new(StringComparer.Ordinal);

    /// <summary>Total <c>SchemaCatchingUp</c> (CADB0503) fence rejections across all databases.</summary>
    public static long FenceRejections => Interlocked.Read(ref fenceRejections);

    /// <summary>Total post-commit schema-ack gates that proceeded via the quorum backstop.</summary>
    public static long QuorumBackstopActivations => Interlocked.Read(ref quorumBackstopActivations);

    /// <summary>Fence rejections recorded for a single database (0 if none).</summary>
    public static long FenceRejectionsFor(string database)
        => fenceRejectionsByDb.TryGetValue(database, out long value) ? value : 0;

    /// <summary>Records one fence rejection (a <c>SchemaCatchingUp</c> reject) for <paramref name="database"/>.</summary>
    public static void RecordFenceRejection(string database)
    {
        Interlocked.Increment(ref fenceRejections);
        fenceRejectionsByDb.AddOrUpdate(database, 1, static (_, current) => current + 1);
    }

    /// <summary>Records one quorum-backstop activation of the post-commit schema-ack gate.</summary>
    public static void RecordQuorumBackstop()
        => Interlocked.Increment(ref quorumBackstopActivations);

    /// <summary>Resets all counters. For test isolation only — not used in production.</summary>
    public static void Reset()
    {
        Interlocked.Exchange(ref fenceRejections, 0);
        Interlocked.Exchange(ref quorumBackstopActivations, 0);
        fenceRejectionsByDb.Clear();
    }
}
