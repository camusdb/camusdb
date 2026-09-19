
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Output column metadata for a bound derived table source, and for the result of a query.
///
/// <para><see cref="Name"/> is what a client sees and need not be unique: <c>SELECT a.id, b.id</c>
/// declares two columns named <c>id</c>. <see cref="RowKey"/> is where the cell lives in a result
/// row, and is unique within one schema. The two are equal unless an earlier column already took the
/// name (see <see cref="Controllers.Queries.QueryProjectionResolver.GetRowKeys"/>).</para>
///
/// <para><b>Read a cell by <see cref="RowKey"/>, never by <see cref="Name"/>.</b> A reader that uses
/// the name returns the first same-named column's value for every one of them, with the right row
/// count and no error.</para>
/// </summary>
public sealed record DerivedColumnSchema(string Name, ColumnType Type)
{
    /// <summary>The key of this column's cell in a result row. Defaults to <see cref="Name"/>.</summary>
    public string RowKey { get; init; } = Name;
}
