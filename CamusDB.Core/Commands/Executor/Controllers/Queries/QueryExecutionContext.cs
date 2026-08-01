
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The ambient facts a query operator needs that are not the rows themselves: which engine's
/// configuration applies, when to stop, and where spill files belong.
///
/// <para>The query pipeline is deliberately built from small operators that hand streams to one
/// another — a sort feeds a distinct, which feeds a projection — and most of them are pure functions
/// of their input. Configuration is the exception: an operator has to know whether spilling is enabled
/// and at what threshold before it can decide how to process a stream at all. Passing each of those
/// values down every call chain would thread half a dozen parameters through operators that otherwise
/// only pass data, so they travel together here instead.</para>
///
/// <para>It carries no per-operator state and is safe to share across the operators of one query. It
/// is not a general-purpose bag: values belong here only when the whole pipeline needs them. Anything
/// one operator needs stays that operator's parameter.</para>
/// </summary>
public sealed class QueryExecutionContext
{
    /// <summary>Configuration of the engine running this query.</summary>
    public CamusDBOptions Options { get; }

    /// <summary>Cancellation for the whole query; operators propagate it into their own awaits.</summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    /// Directory that backs any spill file this query creates. Defaults to the engine's data
    /// directory, and is separate from <see cref="CamusDBOptions.DataDirectory"/> so a caller can
    /// place spill files elsewhere without pretending the whole engine moved.
    /// </summary>
    public string SpillDirectory { get; }

    public QueryExecutionContext(
        CamusDBOptions options,
        CancellationToken cancellationToken = default,
        string? spillDirectory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        Options = options;
        CancellationToken = cancellationToken;
        SpillDirectory = spillDirectory ?? options.DataDirectory;
    }

    /// <summary>
    /// Context for a query against <paramref name="database"/>, taking the configuration of the engine
    /// that database belongs to. This is the usual way to build one: any code holding a descriptor is
    /// already holding the right configuration.
    /// </summary>
    public static QueryExecutionContext For(DatabaseDescriptor database, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        return new QueryExecutionContext(database.Options, cancellationToken);
    }

    /// <summary>Same context, re-scoped to a different cancellation token.</summary>
    public QueryExecutionContext WithCancellation(CancellationToken cancellationToken)
        => new(Options, cancellationToken, SpillDirectory);
}
