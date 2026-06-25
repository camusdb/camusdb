
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Merged row representation for bound multi-source queries.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="QueryResultRow"/> remains the execution payload for today's single-table pipeline.
/// Join, subquery, and grouped execution should emit <see cref="BoundRow"/> instead of
/// extending <see cref="QueryResultRow"/> in place.
/// </para>
/// <para>
/// Key naming after binding:
/// </para>
/// <list type="bullet">
/// <item><description>Qualified keys: <c>{alias}.{column}</c></description></item>
/// <item><description>
/// Unqualified keys: <c>{column}</c> only when the name is unique across all sources in the query
/// </description></item>
/// </list>
/// <para>
/// <see cref="RowId"/> is the underlying KV row id when the row maps to exactly one stored row
/// (single-table scan, or the driving row before a join combines inputs). Synthetic rows — inner
/// join results, aggregated groups, derived-table rows — use <see cref="ObjectIdValue"/> default
/// (<see langword="default"/>).
/// </para>
/// </remarks>
public sealed record BoundRow(ObjectIdValue RowId, IReadOnlyDictionary<string, ColumnValue> Values);
