
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Helpers for turning a parsed index column list (<see cref="ColumnIndexInfo"/>, which carries a
/// per-column ASC/DESC <see cref="OrderType"/>) into the direction vector persisted on
/// <see cref="TableIndexSchema.ColumnDirections"/>.
///
/// Descending index columns are not yet honored by the key encoder or the query planner, so any
/// column declared <see cref="OrderType.Descending"/> is rejected up front: an index physically
/// stored ascending but tagged descending would be mis-decoded once descending encoding lands.
/// Once the encoder and planner understand direction, drop <see cref="RejectUnsupportedDescending"/>
/// and the extracted vector starts driving real behavior with no persistence change.
/// </summary>
internal static class IndexColumnOrder
{
    /// <summary>
    /// Projects the per-column <see cref="OrderType"/> into a direction vector aligned with the
    /// index's column list. Returns null when every column is ascending so the persisted form
    /// stays in its compact, backward-compatible all-ascending shape.
    /// </summary>
    internal static OrderType[]? Extract(ReadOnlySpan<ColumnIndexInfo> columns)
    {
        bool anyDescending = false;
        OrderType[] directions = new OrderType[columns.Length];

        for (int i = 0; i < columns.Length; i++)
        {
            directions[i] = columns[i].Order;
            if (columns[i].Order == OrderType.Descending)
                anyDescending = true;
        }

        return anyDescending ? directions : null;
    }

    /// <summary>
    /// Throws <see cref="CamusDBException"/> if any column requests descending order, naming the
    /// offending column. Called at every DDL entry point that creates an index (standalone and
    /// cluster CREATE INDEX / ALTER ADD INDEX, and inline CREATE TABLE constraints) so a
    /// descending index can never be persisted before the encoder supports it.
    /// </summary>
    internal static void RejectUnsupportedDescending(ReadOnlySpan<ColumnIndexInfo> columns, string indexName)
    {
        foreach (ColumnIndexInfo column in columns)
        {
            if (column.Order == OrderType.Descending)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Descending index columns are not yet supported (column '{column.Name}' in index '{indexName}')"
                );
        }
    }
}
