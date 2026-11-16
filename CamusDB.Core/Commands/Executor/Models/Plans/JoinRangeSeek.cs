/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// The index range a <see cref="NestedLoopJoinNode"/> seeks on its right table for each left row, when
/// the <c>ON</c> predicate bounds the leading column of a right-side index with values of the left row
/// (<c>n.n BETWEEN s.lo AND s.hi</c>, or <c>n.n &gt;= s.lo AND n.n &lt;= s.hi</c>).
///
/// <para>The plan is a template: each <see cref="JoinRangeSeekBound.Expression"/> reads the left row
/// only, so the operator evaluates the bounds once per left row and reads just the right rows of that
/// range, instead of testing every right row against the predicate. The full <c>ON</c> predicate still
/// runs on every pair the seek returns, so the seek only narrows which right rows are considered and
/// never decides a match. A bound that cannot be applied to the key for one left row (a value of
/// another type that has no exact rewrite into the key's domain) is left out for that row, which
/// widens the range; a NULL bound makes the range empty, because a comparison with NULL is never
/// true.</para>
/// </summary>
public sealed class JoinRangeSeek
{
    /// <summary>The right-side index; its first key column is <see cref="KeyColumn"/>, ascending.</summary>
    public TableIndexSchema Index { get; }

    /// <summary>The bounded column of the right table, without its alias.</summary>
    public string KeyColumn { get; }

    /// <summary>The declared type of <see cref="KeyColumn"/>; each bound value is rewritten into it.</summary>
    public ColumnType KeyType { get; }

    /// <summary>The comparisons of the key with left-row expressions, at least one lower and one upper.</summary>
    public IReadOnlyList<JoinRangeSeekBound> Bounds { get; }

    public JoinRangeSeek(TableIndexSchema index, string keyColumn, ColumnType keyType, IReadOnlyList<JoinRangeSeekBound> bounds)
    {
        Index = index;
        KeyColumn = keyColumn;
        KeyType = keyType;
        Bounds = bounds;
    }
}

/// <summary>
/// One comparison <c>key Operator Expression</c> of a <see cref="JoinRangeSeek"/>, with the key on the
/// left: <c>&gt;</c> and <c>&gt;=</c> are lower bounds, <c>&lt;</c> and <c>&lt;=</c> upper bounds.
/// <see cref="Expression"/> reads only literals, parameters and columns of the left side.
/// </summary>
public sealed record JoinRangeSeekBound(string Operator, NodeAst Expression)
{
    public bool IsLower => Operator is ">" or ">=";
}
