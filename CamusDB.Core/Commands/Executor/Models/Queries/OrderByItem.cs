
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// One sort key in an <c>ORDER BY</c> clause.
///
/// <para><b>A position at or after a <c>*</c>.</b> A position counts output columns, as in PostgreSQL,
/// so <c>SELECT *, x FROM t ORDER BY 3</c> names the third column that the list puts out. How many
/// columns a <c>*</c> puts out is known only when the sources are bound, so the query creator keeps
/// such a position in <see cref="OutputPosition"/> (with the <see cref="NodeType.Integer"/> node as
/// <see cref="Expression"/>), and the binder resolves it: to the item's expression when the position
/// names an item after the <c>*</c>, or to <see cref="RowKey"/> when it names a column of the
/// <c>*</c>.</para>
///
/// <para><b>Why a row key and not a name.</b> Two columns of a <c>*</c> can have one name (a <c>*</c>
/// over a join, or over a derived table that has two columns <c>id</c>), and a name would then be
/// ambiguous. The row key is the cell that the <c>*</c> reads, so it is never ambiguous.</para>
/// </summary>
/// <param name="Expression">Sort expression (typically a column identifier).</param>
/// <param name="Direction">Ascending or descending order.</param>
public sealed record OrderByItem(NodeAst Expression, OrderType Direction)
{
    /// <summary>
    /// The 1-based output position of an item that the binder must still resolve, or null. Set only
    /// by the query creator, for a position at or after the first <c>*</c> of the select list.
    /// </summary>
    public int? OutputPosition { get; init; }

    /// <summary>
    /// The row key of the <c>*</c> column that <see cref="OutputPosition"/> names, set by the binder.
    /// The sort reads this cell directly; nothing resolves <see cref="Expression"/> for such an item.
    /// </summary>
    public string? RowKey { get; init; }
}
