/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// The window functions CamusDB runs. Ranking functions and <see cref="Lag"/>/<see cref="Lead"/>
/// ignore a frame, as in PostgreSQL; the value functions and the aggregates read it.
/// </summary>
public enum WindowFunctionKind
{
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// <summary>The kind of one frame bound. The order of the members is the order of the bounds in a partition.</summary>
public enum WindowBoundKind
{
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
}

/// <summary>
/// One bound of a frame. <see cref="Offset"/> is the integer literal or placeholder of
/// <see cref="WindowBoundKind.Preceding"/> and <see cref="WindowBoundKind.Following"/>, and null for
/// the other kinds. A placeholder is evaluated once per statement, when the window runs.
/// </summary>
public readonly record struct WindowBound(WindowBoundKind Kind, NodeAst? Offset);

/// <summary>
/// The frame of a window call. <see cref="IsRange"/> false is a ROWS frame, which counts rows from
/// the current row; true is a RANGE frame, whose CURRENT ROW bound means the first or the last peer of
/// the current row. RANGE takes no offset (the parser refuses it), so a RANGE frame only ever has
/// unbounded and current-row bounds.
/// </summary>
public sealed record WindowFrameSpec(bool IsRange, WindowBound Start, WindowBound End)
{
    /// <summary>
    /// The frame a window has when its specification names none: <c>RANGE BETWEEN UNBOUNDED PRECEDING
    /// AND CURRENT ROW</c>. With an ORDER BY it ends at the last peer of the current row. Without one
    /// every row of the partition is a peer, so it covers the whole partition.
    /// </summary>
    public static readonly WindowFrameSpec Default = new(
        true,
        new WindowBound(WindowBoundKind.UnboundedPreceding, null),
        new WindowBound(WindowBoundKind.CurrentRow, null));
}

/// <summary>
/// One window function call of a windowed SELECT, after <c>WindowQueryRewriter</c> moved its inputs
/// into the inner query of the <see cref="DerivedTableSource"/> that carries it.
///
/// <para><b>Inputs are inner columns.</b> Every argument that reads a column, and every PARTITION BY and
/// ORDER BY expression, is a column of the inner query's rows (a hidden <c>~win_in{k}</c> column). An
/// argument that reads no column (an offset, a default, the <c>n</c> of <c>ntile</c>) stays a literal
/// or a placeholder. So the window operator never evaluates an expression against the source tables:
/// it reads cells, and evaluates only those constants.</para>
///
/// <para><b>One result per distinct call.</b> Two calls with the same function, arguments and
/// specification are one call with one <see cref="ResultColumn"/>, which the outer query reads.</para>
/// </summary>
/// <param name="ResultColumn">The hidden column (<c>~win{n}</c>) that holds the result of each row.</param>
/// <param name="Kind">The function.</param>
/// <param name="Arguments">
/// The arguments in call order: an <see cref="NodeType.Identifier"/> that names an inner column, or a
/// constant node. Empty for <c>row_number()</c>; a single <see cref="NodeType.ExprAllFields"/> for
/// <c>count(*)</c>.
/// </param>
/// <param name="PartitionColumns">The inner columns of PARTITION BY, in order.</param>
/// <param name="OrderColumns">The inner columns and directions of the window ORDER BY, in order.</param>
/// <param name="Frame">The frame, or <see cref="WindowFrameSpec.Default"/> when the call names none.</param>
/// <param name="Call">The call as written, for EXPLAIN.</param>
public sealed record WindowFunctionCall(
    string ResultColumn,
    WindowFunctionKind Kind,
    IReadOnlyList<NodeAst> Arguments,
    IReadOnlyList<string> PartitionColumns,
    IReadOnlyList<QueryOrderBy> OrderColumns,
    WindowFrameSpec Frame,
    NodeAst Call)
{
    /// <summary>True for the functions that read the frame: the value functions and the aggregates.</summary>
    public bool UsesFrame => Kind is WindowFunctionKind.FirstValue
        or WindowFunctionKind.LastValue
        or WindowFunctionKind.NthValue
        or WindowFunctionKind.Count
        or WindowFunctionKind.Sum
        or WindowFunctionKind.Avg
        or WindowFunctionKind.Min
        or WindowFunctionKind.Max;
}
