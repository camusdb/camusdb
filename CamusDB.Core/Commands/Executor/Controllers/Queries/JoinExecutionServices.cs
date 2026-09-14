/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The collaborators every join operator shares, owned by <see cref="QueryJoinExecutor"/> and
/// handed to each operator at construction time. The operators are split across files but form
/// one execution engine; this holder is what lets them share the filterer, the sorter, the
/// metrics sinks and the live configuration without any of them holding a reference back to the
/// facade.
///
/// <para>Every member except <see cref="Options"/> is fixed for the life of the engine.
/// Instances are created once, in the <see cref="QueryJoinExecutor"/> constructor, never per
/// query and never per row.</para>
/// </summary>
internal sealed class JoinExecutionServices
{
    /// <summary>Evaluates ON predicates and residual WHERE filters. Not thread-safe: a caller that
    /// fans work out to workers must keep every call to it on one thread.</summary>
    public required QueryFilterer Filterer { get; init; }

    /// <summary>Applies an ORDER BY that the plan placed inside the join tree.</summary>
    public required QuerySorter Sorter { get; init; }

    /// <summary>Engine statistics; null only in tests that build the executor bare.</summary>
    public StatisticsManager? Statistics { get; init; }

    /// <summary>Engine logger; null only in tests that build the executor bare (broadcast then stays off).</summary>
    public ILogger<ICamusDB>? Logger { get; init; }

    /// <summary>Node-to-node fragment channel; null in standalone mode. Required for broadcast joins.</summary>
    public IQueryFragmentTransport? FragmentTransport { get; init; }

    /// <summary>Coordinator-side distributed counters; null only in tests that build the executor bare.</summary>
    public DistributedQueryMetrics? DistributedMetrics { get; init; }

    /// <summary>
    /// The configuration snapshot currently published to the engine.
    /// <para>
    /// <see cref="QueryJoinExecutor.ApplyOptions"/> is the only writer. Reference assignment is
    /// atomic and the record itself stays immutable, so a reader pins the snapshot it reads at
    /// the top of an operation: an in-flight operation keeps the snapshot it started with, and a
    /// configuration change takes effect at the next operation boundary. An operator must read
    /// this property rather than capture the record at construction time, or a runtime
    /// configuration change would never reach it.
    /// </para>
    /// </summary>
    public CamusDBOptions Options { get; internal set; }

    public JoinExecutionServices(CamusDBOptions options)
    {
        Options = options;
    }
}
