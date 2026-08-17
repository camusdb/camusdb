
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Controllers;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// The engine-level collaborators that a component extracted out of <see cref="CommandExecutor"/>
/// needs in order to reach the rest of the engine: the shared node, the database registry, the
/// openers, and the statistics manager. Bundled because the alternative is threading the same
/// half-dozen constructor parameters through every extracted service, and because the set is the
/// same one for all of them.
///
/// <para><b>Immutable collaborators only — configuration is deliberately absent.</b> A component
/// that reads a tunable keeps its own <c>CamusDBOptions</c> field and receives published swaps
/// through its own <c>ApplyOptions</c>, wired into <see cref="CommandExecutor.ApplyOptions"/>.
/// Handing a snapshot out from here would look like configuration injection while silently
/// latching one value for the process lifetime, turning every runtime-class setting the component
/// reads into a restart-class one.</para>
///
/// <para>The context is built near the end of the executor's constructor, after every controller
/// it names exists. It cannot be built earlier: <see cref="Controllers.DatabaseOpener"/> is itself
/// constructed with a back-reference to the half-built executor, so the openers only become
/// available part-way through construction.</para>
/// </summary>
internal sealed class ExecutorContext
{
    /// <summary>Engine logger, shared with every component the executor owns.</summary>
    internal ILogger<ICamusDB> Logger { get; }

    /// <summary>
    /// Process-level Kahuna node shared across all databases. Null only on an executor composed
    /// without one, where no KV-backed surface is reachable at all — components must treat a null
    /// node as "there is nothing to do here" rather than as an error.
    /// </summary>
    internal EmbeddedKahuna? SharedNode { get; }

    /// <summary>
    /// The cross-database registry, still opening. Awaited rather than held directly because the
    /// registry opens asynchronously and components are constructed before it is ready.
    /// </summary>
    internal Task<DatabaseRegistry> Registry { get; }

    /// <summary>Opens (and caches) a database descriptor by name.</summary>
    internal DatabaseOpener DatabaseOpener { get; }

    /// <summary>Opens a table descriptor within an already-open database.</summary>
    internal TableOpener TableOpener { get; }

    /// <summary>Optimizer statistics for the engine's tables.</summary>
    internal StatisticsManager Statistics { get; }

    /// <summary>Per-ticket validation, run before an operation touches any state.</summary>
    internal CommandValidator Validator { get; }

    /// <summary>True when this process is a Raft cluster node; false for standalone.</summary>
    internal bool IsClusterMode { get; }

    internal ExecutorContext(
        ILogger<ICamusDB> logger,
        EmbeddedKahuna? sharedNode,
        Task<DatabaseRegistry> registry,
        DatabaseOpener databaseOpener,
        TableOpener tableOpener,
        StatisticsManager statistics,
        CommandValidator validator,
        bool isClusterMode
    )
    {
        Logger = logger;
        SharedNode = sharedNode;
        Registry = registry;
        DatabaseOpener = databaseOpener;
        TableOpener = tableOpener;
        Statistics = statistics;
        Validator = validator;
        IsClusterMode = isClusterMode;
    }
}
