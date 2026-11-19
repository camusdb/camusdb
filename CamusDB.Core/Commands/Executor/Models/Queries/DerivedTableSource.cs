
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
    /// The immutable id of the view this derived table was expanded from, or null for a derived table
    /// the user wrote. The binder checks the principal in scope — the caller, or the owner of the view
    /// whose body this one appears in — for SELECT on it, on every read.
    /// </summary>
    public string? ViewId { get; init; }

    /// <summary>The name of the view this derived table was expanded from, for error messages.</summary>
    public string? ViewName { get; init; }

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
    /// The work table this derived table stands for, or null for any other derived table. When it is
    /// set, <see cref="Query"/> is the inner shell <c>SELECT * FROM work table</c>: it is never bound or
    /// executed as a query, and the derived-table executor reads the rows of the previous step of the
    /// recursion (see <see cref="WorkTableSource"/>).
    /// </summary>
    public WorkTableSource? WorkTable => Query.Source as WorkTableSource;

    /// <summary>
    /// The window calls of the windowed SELECT this derived table stands for, or null for any other
    /// derived table. <c>WindowQueryRewriter</c> sets it: <see cref="Query"/> is then the inner query
    /// that computes every window input, and <c>DerivedTableExecutor</c> runs the window operator over
    /// its rows, which adds one <see cref="WindowFunctionCall.ResultColumn"/> per call. The alias is
    /// <see cref="WindowDerivedAlias"/>, which no query can name.
    /// </summary>
    public IReadOnlyList<WindowFunctionCall>? Windows { get; init; }

    /// <summary>
    /// The names of <c>AS alias(c1, c2)</c>, or of a CTE's column list, in order; null when the source
    /// names no columns. The binder gives the first columns of the query these names, as PostgreSQL
    /// does: a shorter list keeps the names of the later columns, and a longer list is refused.
    /// </summary>
    public IReadOnlyList<string>? ColumnAliases { get; init; }

    /// <summary>
    /// The <see cref="SQLParser.NodeType.CteMaterialization"/> node of a CTE that the statement computes
    /// once, or null. Every reference of that CTE holds the same node, so the executor keeps the CTE's
    /// rows under it for the statement, and each reference reads those rows.
    /// </summary>
    public SQLParser.NodeAst? ComputedOnce { get; init; }

    /// <summary>
    /// The SELECT of a <c>LATERAL</c> derived table, as parsed, or null for any other derived table.
    /// <see cref="Query"/> is the same SELECT as a query model. The binder keeps the parse tree because a
    /// <c>LATERAL</c> derived table that reads the <c>FROM</c> items to its left runs as a correlated body
    /// (<c>CorrelatedSubqueryBody</c>): each name of a left item becomes a hidden placeholder in a copy of
    /// this tree, and the copy runs once for each left row. A <c>LATERAL</c> derived table that reads no left
    /// item binds and runs as a plain derived table.
    /// </summary>
    public SQLParser.NodeAst? LateralBody { get; init; }

    /// <summary>
    /// The number of rows of the <c>VALUES</c> list this derived table stands for, or null for any other
    /// derived table. The parser writes a <c>VALUES</c> list as a <c>UNION ALL</c> of FROM-less SELECTs
    /// (<c>ValuesAst</c>), and the list binds and runs as that query; the count only names the source in
    /// <c>EXPLAIN</c>.
    /// </summary>
    public int? ValuesRowCount { get; init; }

    /// <summary>
    /// The alias of the derived table that holds a windowed SELECT. The asterisks keep it apart from
    /// any identifier a user can write without quotes, as for <see cref="SetOperationSource.DerivedAlias"/>.
    /// </summary>
    public const string WindowDerivedAlias = "*window*";
}
