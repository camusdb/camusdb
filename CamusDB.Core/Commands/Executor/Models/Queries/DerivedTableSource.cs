
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Cache;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A subquery in the <c>FROM</c> clause (<c>FROM (SELECT ...) alias</c>).
/// </summary>
/// <param name="Query">Inner select whose projected columns define the derived schema.</param>
/// <param name="Alias">Required alias for the derived source.</param>
/// <param name="CacheHint">
/// A <c>{cache=name}</c> hint written on the source this derived table stands for. Only view
/// expansion sets it: a view reference may carry a hint, and expansion turns that reference into a
/// derived table, so without somewhere to put the hint it would have to be either discarded silently
/// or refused. It is carried so the response can report honestly what became of it.
/// </param>
/// <param name="OwnerName">
/// The name of the principal a view's body runs as, when this derived table is an expanded view.
/// Null for a derived table the user wrote themselves, which runs as the caller.
/// </param>
/// <param name="OwnerId">
/// The owner's immutable id, verified alongside the name so a dropped-and-recreated username cannot
/// inherit the original owner's access.
/// </param>
public sealed record DerivedTableSource(
    SelectQuery Query,
    string Alias,
    CacheHintOptions? CacheHint = null,
    string? OwnerName = null,
    string? OwnerId = null) : QuerySource
{
    /// <summary>
    /// The set-returning function this derived table stands for, or null for a subquery or an
    /// expanded view. When it is set, <see cref="Query"/> is the inner shell
    /// <c>SELECT column FROM function(...)</c>: it is never bound or executed as a query, and the
    /// leaf scanner streams the function's rows directly (see <see cref="TableFunctionSource"/>).
    /// </summary>
    public TableFunctionSource? Function => Query.Source as TableFunctionSource;

    /// <summary>
    /// The set operation this derived table stands for, or null for any other derived table. When it
    /// is set, <see cref="Query"/> is the inner shell <c>SELECT * FROM (q1 UNION q2)</c>: the binder
    /// binds the two operands instead of the shell, and the leaf scanner runs them (see
    /// <see cref="SetOperationSource"/>).
    /// </summary>
    public SetOperationSource? SetOperation => Query.Source as SetOperationSource;

    /// <summary>
    /// The window calls of the windowed SELECT this derived table stands for, or null for any other
    /// derived table. <c>WindowQueryRewriter</c> sets it: <see cref="Query"/> is then the inner query
    /// that computes every window input, and <c>DerivedTableExecutor</c> runs the window operator over
    /// its rows, which adds one <see cref="WindowFunctionCall.ResultColumn"/> per call. The alias is
    /// <see cref="WindowDerivedAlias"/>, which no query can name.
    /// </summary>
    public IReadOnlyList<WindowFunctionCall>? Windows { get; init; }

    /// <summary>
    /// The alias of the derived table that holds a windowed SELECT. The asterisks keep it apart from
    /// any identifier a user can write without quotes, as for <see cref="SetOperationSource.DerivedAlias"/>.
    /// </summary>
    public const string WindowDerivedAlias = "*window*";
}
