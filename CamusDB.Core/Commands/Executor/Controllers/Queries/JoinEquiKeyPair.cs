/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A single equi-key pair extracted from a join ON predicate for use by hash join and merge join.
/// Unlike <see cref="JoinEquiJoinIndexMatch"/>, no index is required.
///
/// <para>Each side of a pair is a column, or an expression of the row of that side
/// (<see cref="LeftExpression"/>, <see cref="RightExpression"/>), as in <c>sd.id = CASE WHEN s.side = 0
/// THEN l.right_sidedef ELSE l.left_sidedef END</c>. Only a hash join takes an expression pair: it
/// computes the expression once for each row (<see cref="JoinKeyExpressionList"/>). An index, merge or
/// broadcast join reads the key by name, so the planner keeps those operators for column pairs.</para>
/// </summary>
internal sealed class JoinEquiKeyPair
{
    /// <summary>
    /// Qualified left-side (probe) key in the merged row, e.g. <c>o.id</c>. For a left expression it is
    /// a placeholder name that no row carries (<see cref="JoinKeyExpressionList.PlaceholderName"/>).
    /// </summary>
    public string LeftLookupColumn { get; }

    /// <summary>
    /// Unqualified column name on the right (build) side, e.g. <c>order_id</c>. For a right expression
    /// it is a placeholder name that no row carries.
    /// </summary>
    public string RightColumnName { get; }

    /// <summary>The expression of the qualified left row that gives the key, or null for a column.</summary>
    public NodeAst? LeftExpression { get; }

    /// <summary>The expression of the right row that gives the key, or null for a column.</summary>
    public NodeAst? RightExpression { get; }

    /// <summary>
    /// For <see cref="RightExpression"/>: the key of the right-row cell for the text of each identifier
    /// of the expression (<c>p.u</c> to <c>u</c>). A right row is unqualified.
    /// </summary>
    public IReadOnlyDictionary<string, string>? RightRowKeys { get; }

    /// <summary>
    /// The type both sides share, or null when it is unknown. A computed key value of another numeric
    /// type is rewritten into it. Set for an expression pair only.
    /// </summary>
    public ColumnType? KeyType { get; }

    public JoinEquiKeyPair(string leftLookupColumn, string rightColumnName)
    {
        LeftLookupColumn = leftLookupColumn;
        RightColumnName = rightColumnName;
    }

    public JoinEquiKeyPair(
        string leftLookupColumn,
        string rightColumnName,
        NodeAst? leftExpression,
        NodeAst? rightExpression,
        IReadOnlyDictionary<string, string>? rightRowKeys,
        ColumnType? keyType)
    {
        LeftLookupColumn = leftLookupColumn;
        RightColumnName = rightColumnName;
        LeftExpression = leftExpression;
        RightExpression = rightExpression;
        RightRowKeys = rightRowKeys;
        KeyType = keyType;
    }

    /// <summary>True when either side is an expression and not a column.</summary>
    public bool IsExpression => LeftExpression is not null || RightExpression is not null;
}
