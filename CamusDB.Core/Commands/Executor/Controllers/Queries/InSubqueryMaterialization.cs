
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Materialized uncorrelated IN/NOT IN subquery value set, backed by a
/// <see cref="SpillableValueList"/> to avoid pinning <c>O(N)</c> values in a plain
/// <c>List&lt;ColumnValue&gt;</c> when the subquery returns a large result set.
///
/// <para>
/// Callers must dispose this record (via <c>await using</c>) once the value list is no
/// longer needed — typically immediately after the <c>ExprList</c> AST tree has been built
/// from the enumeration. Disposing releases any spill file created during collection.
/// </para>
/// </summary>
internal sealed record InSubqueryMaterialization(
    SpillableValueList Values,
    bool ContainsNull,
    bool IsEmpty) : IAsyncDisposable
{
    public ValueTask DisposeAsync() => Values.DisposeAsync();
}
